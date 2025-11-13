//! In-memory implementation of the log metadata trait.
//!
//! This implementation stores all data in memory and is suitable for testing
//! and development.

use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    fmt::Display,
    ops::Bound,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use dashmap::DashMap;
use tokio::{
    sync::{Notify, futures::OwnedNotified},
    time::Instant,
};
use tracing::{debug, info, trace};

use crate::{
    cluster_metadata::{ClusterMetadata, cache::TopicCache},
    log_metadata::{
        AcceptedBatchInfo, RejectedBatchInfo,
        timestamp::{ValidateRequestResult, validate_timestamp_in_request},
    },
    resources::{NamespaceName, PartitionValue, TopicName},
};

const COMPACTION_THRESHOLD: u64 = 64 * 1024 * 1024; // 64 MiB for testing

use super::{
    CommitPageRequest, CommitPageResponse, CommittedBatch, CompactionTask, CompleteTaskRequest,
    CompleteTaskResponse, FolioLocation, GetLogLocationOptions, GetLogLocationRequest,
    ListPartitionsRequest, ListPartitionsResponse, LogLocation, LogMetadata, LogMetadataError,
    LogOffset, PartitionMetadata, RequestTaskRequest, RequestTaskResponse, Result, Task,
    TaskMetadata, TaskStatus, error::InternalSnafu, timestamp::compare_batch_request_timestamps,
};

#[derive(Clone)]
pub struct InMemoryLogMetadata {
    /// Maps topic names to their log state
    topics: Arc<DashMap<TopicName, TopicLogState>>,
    /// Shared task manager for tracking tasks
    ///
    /// Since the mutex is sometimes held in the commit path, this may cause
    /// increased latency when requesting tasks.
    /// Since this is an in-memory implementation for development, it's
    /// not important.
    task_manager: Arc<Mutex<TaskManager>>,
    topic_cache: TopicCache,
}

#[derive(Debug, Clone)]
struct TopicLogState {
    /// Maps partition keys to their offset tracking
    partitions: BTreeMap<PartitionKey, PartitionLogState>,
}

/// A partition key used to identify unique (topic, partition_value) combinations.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PartitionKey {
    topic_name: TopicName,
    partition_value: Option<PartitionValue>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TaskDefinitionMetadata {
    pub task_id: String,
    pub created_at: SystemTime,
    pub updated_at: SystemTime,
}

/// Task definition
#[derive(Debug, Clone, PartialEq, Eq)]
enum TaskDefinition {
    Compaction(TaskDefinitionMetadata, CompactionTask),
}

/// Task manager for tracking tasks in different states
#[derive(Debug, Default)]
struct TaskManager {
    /// Tasks that have been queued but not assigned yet
    queued: VecDeque<TaskDefinition>,
    /// Tasks that are currently in progress
    in_progress: HashMap<String, Task>,
    /// Notify when a task is added.
    notify: Arc<Notify>,
}

#[derive(Debug, Clone)]
struct PartitionLogState {
    key: PartitionKey,
    /// Next offset to be assigned
    next_offset: LogOffset,
    /// Current estimated size of the partition data, in bytes.
    current_estimated_size: u64,
    /// First dirty data offset (oldest data that hasn't been compacted)
    first_dirty_offset: LogOffset,
    /// Maps start offset to page information for lookup.
    pages: BTreeMap<u64, PageInfo>,
    /// Maps timestamp to the first offset containing that timestamp.
    /// Used for efficient timestamp-based queries.
    timestamp_index: BTreeMap<SystemTime, u64>,
    /// Notify when a new page is added.
    notify: Arc<Notify>,
}

#[derive(Debug, Clone)]
struct PageInfo {
    /// The file reference for the page.
    pub file_ref: String,
    /// Where the Parquet file starts in the folio.
    pub offset_bytes: u64,
    /// The size of the Parquet file.
    pub size_bytes: u64,
    /// The end offset of the messages in the page.
    pub end_offset: LogOffset,
    /// The batches in the page.
    pub batches: Vec<CommittedBatch>,
}

#[derive(Debug)]
enum GetLogLocationResult {
    Done,
    NeedMore(OwnedNotified),
}

impl InMemoryLogMetadata {
    pub fn new(cluster_meta: Arc<dyn ClusterMetadata>) -> Self {
        let topic_cache = TopicCache::new(cluster_meta);
        Self {
            topics: Default::default(),
            task_manager: Default::default(),
            topic_cache,
        }
    }
}

#[async_trait]
impl LogMetadata for InMemoryLogMetadata {
    async fn commit_folio(
        &self,
        namespace: NamespaceName,
        file_ref: String,
        pages: &[CommitPageRequest],
    ) -> Result<Vec<CommitPageResponse>> {
        validate_pages_to_commit(pages)?;

        // Assign the same timestamp to all batches across pages.
        let now_ts = SystemTime::now();

        let mut committed_pages = Vec::new();
        let mut tasks = Vec::new();

        for page in pages {
            // This should have been checked already by the caller.
            assert_eq!(page.topic_name.parent(), &namespace);

            let partition_key =
                PartitionKey::new(page.topic_name.clone(), page.partition_value.clone());

            let mut topic_state = self
                .topics
                .entry(page.topic_name.clone())
                .or_insert_with(|| TopicLogState {
                    partitions: BTreeMap::new(),
                });

            let (committed_page, maybe_task) =
                topic_state.commit_page(partition_key, page, file_ref.clone(), now_ts)?;

            committed_pages.push(committed_page);

            if let Some(task) = maybe_task {
                tasks.push(task);
            }
        }

        if !tasks.is_empty() {
            let mut task_manager = self.task_manager.lock().map_err(|_| {
                InternalSnafu {
                    message: "task manager lock is poisoned".to_string(),
                }
                .build()
            })?;

            for task in tasks {
                task_manager.queue_task(task);
            }
        }

        Ok(committed_pages)
    }

    async fn get_log_location(&self, request: GetLogLocationRequest) -> Result<Vec<LogLocation>> {
        trace!(?request, "InMemoryLogMetadata::get_log_location");
        let partition_key = PartitionKey::new(request.topic_name.clone(), request.partition_value);
        let mut locations = Vec::new();

        let deadline = Instant::now() + request.options.deadline;

        loop {
            let notified = {
                let Some(topic_state) = self.topics.get(&request.topic_name) else {
                    return Ok(Vec::default());
                };

                match topic_state.get_log_location(
                    &partition_key,
                    request.offset,
                    &request.options,
                    &mut locations,
                )? {
                    GetLogLocationResult::Done => {
                        return Ok(locations);
                    }
                    GetLogLocationResult::NeedMore(notified) => notified,
                }
            };

            let timeout = tokio::time::sleep_until(deadline);
            debug!("Waiting for more log locations");

            tokio::select! {
                _ = notified => {
                    debug!("Notified of state change");
                }
                _ = timeout => {
                    return Ok(locations);
                }
            }
        }
    }

    async fn list_partitions(
        &self,
        request: ListPartitionsRequest,
    ) -> Result<ListPartitionsResponse> {
        let Some(topic_state) = self.topics.get(&request.topic_name) else {
            return Ok(ListPartitionsResponse {
                partitions: vec![],
                next_page_token: None,
            });
        };

        let topic = self
            .topic_cache
            .get(request.topic_name.clone())
            .await
            .map_err(|_| LogMetadataError::InvalidArgument {
                message: "could not get topic".to_string(),
            })?;

        let page_size = request.page_size.unwrap_or(100);

        let partition_value = if let Some(data_type) = topic.partition_field_data_type() {
            if let Some(page_token) = request.page_token.as_ref() {
                let pv =
                    PartitionValue::parse_with_datatype(data_type, page_token).map_err(|err| {
                        LogMetadataError::InvalidArgument {
                            message: format!("invalid partition value: {err}"),
                        }
                    })?;
                Some(pv)
            } else {
                None
            }
        } else {
            None
        };

        let partition_key = PartitionKey::new(request.topic_name.clone(), partition_value);

        let start_key_range: Bound<&PartitionKey> =
            if topic.partition_field().is_none() || partition_key.partition_value.is_none() {
                Bound::Unbounded
            } else {
                Bound::Excluded(&partition_key)
            };

        debug!(
            start_key_range = ?start_key_range,
            "InMemoryLogMetadata::list_partitions start"
        );

        let mut partitions = Vec::new();
        let mut next_page_token = None;

        for (key, state) in topic_state
            .partitions
            .range((start_key_range, Bound::Unbounded))
            .take(page_size)
        {
            next_page_token = key.partition_value.as_ref().map(|pv| pv.to_string());
            partitions.push(PartitionMetadata {
                partition_value: key.partition_value.clone(),
                end_offset: state.next_offset,
            });
        }

        if partitions.len() < page_size {
            next_page_token = None;
        }

        debug!(
            next_page_token = ?next_page_token,
            "InMemoryLogMetadata::list_partitions done"
        );

        Ok(ListPartitionsResponse {
            partitions,
            next_page_token,
        })
    }

    async fn request_task(&self, _request: RequestTaskRequest) -> Result<RequestTaskResponse> {
        debug!("Received task request");

        let timeout = tokio::time::sleep(Duration::from_secs(1));
        tokio::pin!(timeout);

        loop {
            let waiter = {
                let mut task_manager = self.task_manager.lock().map_err(|_| {
                    InternalSnafu {
                        message: "failed to acquire poisoned lock".to_string(),
                    }
                    .build()
                })?;

                if let Some(task) = task_manager.next_task() {
                    info!(task_id = task.task_id(), "Assigned task to worker");
                    return Ok(RequestTaskResponse { task: task.into() });
                }

                task_manager.notify.clone().notified_owned()
            };

            debug!("No task available. Waiting.");

            tokio::select! {
                _ = waiter => {
                    continue;
                }
                _ = &mut timeout => {
                    break;
                }
            }
        }

        Ok(RequestTaskResponse { task: None })
    }

    async fn complete_task(&self, _request: CompleteTaskRequest) -> Result<CompleteTaskResponse> {
        todo!();
    }
}

fn validate_pages_to_commit(pages: &[CommitPageRequest]) -> Result<()> {
    let mut seen_partitions = HashSet::new();
    for page in pages {
        if !seen_partitions.insert((page.topic_name.clone(), page.partition_value.clone())) {
            return Err(LogMetadataError::DuplicatePartitionValue {
                topic: page.topic_name.clone(),
                partition: page.partition_value.clone(),
            });
        }

        // Validate that the batches are sorted by timestamp.
        // We do it here because it's a protocol requirement to provide batches sorted by timestamp.
        // The timestamp validation is done later.
        if !page
            .batches
            .iter()
            .is_sorted_by(|a, b| compare_batch_request_timestamps(a, b) != Ordering::Greater)
        {
            return Err(LogMetadataError::UnorderedPageBatches {
                topic: page.topic_name.clone(),
                partition: page.partition_value.clone(),
            });
        }
    }

    Ok(())
}

impl TopicLogState {
    fn commit_page(
        &mut self,
        partition_key: PartitionKey,
        page: &CommitPageRequest,
        file_ref: String,
        now_ts: SystemTime,
    ) -> Result<(CommitPageResponse, Option<TaskDefinition>)> {
        let partition_state = self
            .partitions
            .entry(partition_key.clone())
            .or_insert_with(|| PartitionLogState {
                key: partition_key,
                next_offset: LogOffset::default(),
                current_estimated_size: 0,
                first_dirty_offset: LogOffset::default(),
                pages: BTreeMap::new(),
                timestamp_index: BTreeMap::new(),
                notify: Arc::new(Notify::new()),
            });

        partition_state.commit_page(page, file_ref, now_ts)
    }

    fn get_log_location(
        &self,
        partition_key: &PartitionKey,
        offset: u64,
        options: &GetLogLocationOptions,
        locations: &mut Vec<LogLocation>,
    ) -> Result<GetLogLocationResult> {
        let Some(partition_state) = self.partitions.get(partition_key) else {
            return Ok(GetLogLocationResult::Done);
        };

        partition_state.get_log_location(offset, options, locations)
    }
}

impl PartitionLogState {
    pub fn commit_page(
        &mut self,
        page: &CommitPageRequest,
        file_ref: String,
        now_ts: SystemTime,
    ) -> Result<(CommitPageResponse, Option<TaskDefinition>)> {
        let start_offset = self.next_offset;

        let mut batches = Vec::new();

        let mut current_offset = start_offset;

        // TODO: check that the timestamp is assigned correctly
        // we want the state to have the timestamp of the most recently assigned batch
        for batch in page.batches.iter() {
            match validate_timestamp_in_request(&current_offset, batch) {
                ValidateRequestResult::Reject => {
                    let rejected = RejectedBatchInfo {
                        num_messages: batch.num_messages,
                    };
                    batches.push(CommittedBatch::Rejected(rejected));
                }
                ValidateRequestResult::Accept {
                    start_offset,
                    end_offset,
                    timestamp,
                    next_offset,
                } => {
                    let accepted = AcceptedBatchInfo {
                        start_offset,
                        end_offset,
                        timestamp: timestamp.unwrap_or(now_ts),
                    };
                    current_offset = next_offset;
                    batches.push(CommittedBatch::Accepted(accepted));
                }
            }
        }

        let mut task_request = None;

        // Update state only if we accepted any data.
        if current_offset != start_offset {
            current_offset = current_offset.with_timestamp(now_ts);

            let end_offset = current_offset.previous();

            let page_info = PageInfo {
                file_ref: file_ref.clone(),
                offset_bytes: page.offset_bytes,
                size_bytes: page.batch_size_bytes,
                end_offset,
                batches: batches.clone(),
            };

            debug!(
                offset = start_offset.offset,
                next_offset = ?current_offset,
                page_info = ?page_info,
                "Updating partition state"
            );

            self.pages.insert(start_offset.offset, page_info);
            // TODO: check this is correct. Maybe the timestamp index should have the
            // timestamp from the current offset?
            self.timestamp_index
                .insert(start_offset.timestamp, start_offset.offset);

            self.next_offset = current_offset;
            self.current_estimated_size += page.batch_size_bytes;

            // Check if we should signal to the caller to trigger compaction
            if self.current_estimated_size > COMPACTION_THRESHOLD {
                let start_offset = self.first_dirty_offset;

                info!(
                    topic = %self.key.topic_name,
                    pv = ?self.key.partition_value,
                    start_offset = start_offset.offset,
                    end_offset = end_offset.offset,
                    "compaction triggered"
                );

                task_request = self.new_compaction_task(&start_offset, &end_offset).into();

                self.current_estimated_size = 0;
                self.first_dirty_offset = self.next_offset;
            }

            self.notify.notify_waiters();
        }

        let response = CommitPageResponse {
            topic_name: page.topic_name.clone(),
            partition_value: page.partition_value.clone(),
            batches,
        };

        Ok((response, task_request))
    }

    fn get_log_location(
        &self,
        offset: u64,
        options: &GetLogLocationOptions,
        locations: &mut Vec<LogLocation>,
    ) -> Result<GetLogLocationResult> {
        let target_offset = offset + options.min_rows as u64;
        // Find the batch containing this offset
        let batch_start = self.pages.range(..=offset).next_back();

        debug!(offset, target_offset, "get log location by offset");

        let Some((&start_offset, _page_info)) = batch_start else {
            debug!("no page found for offset");
            return Ok(GetLogLocationResult::Done);
        };

        let mut current_offset = None;

        for (start_offset, page_info) in self.pages.range(start_offset..) {
            trace!(
                start_offset,
                end_offset = page_info.end_offset.offset,
                target_offset,
                "processing page"
            );

            if page_info.end_offset.offset < offset {
                continue;
            }

            if *start_offset > target_offset {
                break;
            }

            current_offset = Some(page_info.end_offset.offset);

            locations.push(LogLocation::Folio(FolioLocation {
                file_ref: page_info.file_ref.clone(),
                offset_bytes: page_info.offset_bytes,
                size_bytes: page_info.size_bytes,
                batches: page_info.batches.clone(),
            }));
        }

        trace!(target_offset, ?current_offset, "finished get_log_location");

        let Some(current_offset) = current_offset else {
            let notified = self.notify.clone().notified_owned();
            return Ok(GetLogLocationResult::NeedMore(notified));
        };

        if current_offset < target_offset {
            let notified = self.notify.clone().notified_owned();
            return Ok(GetLogLocationResult::NeedMore(notified));
        }

        Ok(GetLogLocationResult::Done)
    }

    pub fn new_compaction_task(
        &self,
        start_offset: &LogOffset,
        end_offset: &LogOffset,
    ) -> TaskDefinition {
        let now = SystemTime::now();
        let metadata = TaskDefinitionMetadata {
            task_id: format!("compaction@{}@{}", self.key, start_offset.offset),
            created_at: now,
            updated_at: now,
        };

        let task = CompactionTask {
            topic_name: self.key.topic_name.clone(),
            partition_value: self.key.partition_value.clone(),
            start_offset: start_offset.offset,
            end_offset: end_offset.offset,
        };

        TaskDefinition::Compaction(metadata, task)
    }
}

impl TaskManager {
    pub fn queue_task(&mut self, task: TaskDefinition) {
        debug!(task_id = task.task_id(), "added task to manager");
        self.queued.push_back(task);
        self.notify.notify_waiters();
    }

    pub fn next_task(&mut self) -> Option<Task> {
        let task = self.queued.pop_front()?.into_task(TaskStatus::InProgress);

        self.in_progress
            .insert(task.task_id().to_string(), task.clone());

        Some(task)
    }
}

impl PartitionKey {
    pub fn new(topic_name: TopicName, partition_value: Option<PartitionValue>) -> Self {
        PartitionKey {
            topic_name,
            partition_value,
        }
    }
}

impl PartialOrd for PartitionKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PartitionKey {
    fn cmp(&self, other: &Self) -> Ordering {
        match (
            self.partition_value.as_ref(),
            other.partition_value.as_ref(),
        ) {
            (Some(self_value), Some(other_value)) => self_value.cmp(other_value),
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Less,
            (Some(_), None) => Ordering::Greater,
        }
    }
}

impl Display for PartitionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.partition_value {
            None => write!(f, "{}", self.topic_name),
            Some(ref value) => write!(f, "{}-{}", self.topic_name, value),
        }
    }
}

impl TaskDefinition {
    pub fn task_id(&self) -> &str {
        match self {
            TaskDefinition::Compaction(meta, _) => &meta.task_id,
        }
    }

    pub fn into_task(self, status: TaskStatus) -> Task {
        match self {
            TaskDefinition::Compaction(metadata, task) => {
                let metadata = TaskMetadata {
                    task_id: metadata.task_id,
                    created_at: metadata.created_at,
                    updated_at: metadata.updated_at,
                    status,
                };
                Task::Compaction { metadata, task }
            }
        }
    }
}
