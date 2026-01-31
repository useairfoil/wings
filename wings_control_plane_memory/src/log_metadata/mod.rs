//! In-memory implementation of the log metadata trait.
//!
//! This implementation stores all data in memory and is suitable for testing
//! and development.

mod candidate;
mod page;
mod partition;
mod topic;

use std::{
    ops::Bound,
    time::{Duration, SystemTime},
};

use dashmap::DashMap;
use tokio::time::Instant;
use tracing::{debug, trace};
use wings_control_plane_core::log_metadata::{
    CommitPageRequest, CommitPageResponse, CompleteTaskRequest, CompleteTaskResponse,
    GetLogLocationRequest, ListPartitionsRequest, ListPartitionsResponse, LogLocation,
    LogMetadataError, PartitionMetadata, RequestTaskRequest, RequestTaskResponse, Result,
};
use wings_resources::{NamespaceName, PartitionValue, TopicName};

use self::{
    candidate::{CandidateTask, CandidateTaskQueue},
    partition::{GetLogLocationResult, PartitionKey},
    topic::TopicLogState,
};
use crate::cluster_metadata::ClusterMetadataStore;

#[derive(Debug)]
pub struct LogMetadataStore {
    /// Maps topic names to their log state
    topics: DashMap<TopicName, TopicLogState>,
    /// Candidate task queue for managing task polling
    candidate_queue: tokio::sync::Mutex<CandidateTaskQueue>,
    /// Maps task IDs to topic names for task completion
    task_to_topic: DashMap<String, TopicName>,
}

impl LogMetadataStore {
    pub fn new() -> Self {
        Self {
            topics: Default::default(),
            candidate_queue: tokio::sync::Mutex::new(CandidateTaskQueue::new()),
            task_to_topic: DashMap::new(),
        }
    }
}

impl LogMetadataStore {
    pub async fn commit_folio(
        &self,
        namespace: NamespaceName,
        file_ref: String,
        pages: &[CommitPageRequest],
        cluster_metadata: &ClusterMetadataStore,
    ) -> Result<Vec<CommitPageResponse>> {
        page::validate_pages_to_commit(pages)?;

        // Assign the same timestamp to all batches across pages.
        let now_ts = SystemTime::now();

        let mut committed_pages = Vec::new();
        let mut candidate_tasks = Vec::new();

        for page in pages {
            // This should have been checked already by the caller.
            assert_eq!(page.topic_name.parent(), &namespace);

            let partition_key =
                PartitionKey::new(page.topic_name.clone(), page.partition_value.clone());

            let topic_name = page.topic_name.clone();

            let topic_state = match self.topics.entry(topic_name.clone()) {
                dashmap::Entry::Occupied(entry) => entry.into_ref(),
                dashmap::Entry::Vacant(entry) => {
                    debug!(topic = %topic_name, "New topic detected, fetching compaction config");

                    // Fetch the topic to get compaction configuration - fail if we can't get it
                    let topic = cluster_metadata
                        .get_topic(topic_name.clone())
                        .map_err(|err| LogMetadataError::InvalidArgument {
                            message: format!(
                                "Failed to fetch topic {} for compaction config: {}",
                                topic_name, err
                            ),
                        })?;

                    let topic_state =
                        TopicLogState::new(topic_name.clone(), topic.compaction.clone());

                    // Queue table creation task since this is a new topic
                    let mut candidate_queue = self.candidate_queue.lock().await;
                    candidate_queue.queue_immediate(CandidateTask::Topic(topic_name));

                    entry.insert(topic_state)
                }
            };

            let mut topic_state = topic_state;

            let (committed_page, candidate_task) =
                topic_state.commit_page(partition_key, page, file_ref.clone(), now_ts)?;

            committed_pages.push(committed_page);

            // Collect candidate tasks for later addition to queue
            if let Some(task) = candidate_task {
                candidate_tasks.push(task);
            }
        }

        // Add all collected candidate tasks to the queue after committing all pages
        if !candidate_tasks.is_empty() {
            debug!(
                "Adding {} partition candidate tasks to queue",
                candidate_tasks.len()
            );

            let mut candidate_queue = self.candidate_queue.lock().await;
            for (task, duration) in candidate_tasks {
                debug!(task = ?task, ?duration, "adding candidate task");
                candidate_queue.queue(task, duration);
            }
        }

        Ok(committed_pages)
    }

    pub async fn get_log_location(
        &self,
        request: GetLogLocationRequest,
    ) -> Result<Vec<LogLocation>> {
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

    pub async fn list_partitions(
        &self,
        request: ListPartitionsRequest,
        cluster_metadata: &ClusterMetadataStore,
    ) -> Result<ListPartitionsResponse> {
        let Some(topic_state) = self.topics.get(&request.topic_name) else {
            return Ok(ListPartitionsResponse {
                partitions: vec![],
                next_page_token: None,
            });
        };

        let topic = cluster_metadata
            .get_topic(request.topic_name.clone())
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
            .partition_range(start_key_range, Bound::Unbounded)
            .take(page_size)
        {
            next_page_token = key.partition_value.as_ref().map(|pv| pv.to_string());
            partitions.push(PartitionMetadata {
                partition_value: key.partition_value.clone(),
                end_offset: state.next_offset(),
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

    pub async fn request_task(&self, _request: RequestTaskRequest) -> Result<RequestTaskResponse> {
        trace!("Received task request");

        let start_time = std::time::Instant::now();
        let max_duration = Duration::from_secs(1);
        let poll_interval = Duration::from_millis(100);

        loop {
            // Check if we've exceeded the maximum duration
            if start_time.elapsed() >= max_duration {
                trace!("Task request timeout after 1 second");
                return Ok(RequestTaskResponse { task: None });
            }

            // Poll the candidate queue for a new candidate
            let candidate = {
                let mut candidate_queue = self.candidate_queue.lock().await;
                candidate_queue.next_candidate()
            };

            if let Some(candidate) = candidate {
                let topic_name = candidate.topic_name().clone();
                let mut topic_state =
                    self.topics
                        .get_mut(&topic_name)
                        .ok_or_else(|| LogMetadataError::Internal {
                            message: format!("Candidate task for non existing topic {topic_name}"),
                        })?;

                if let Some(task) = topic_state.candidate_task(candidate) {
                    // Store the mapping from task ID to topic name, needed to dispatch result later
                    debug!(task_id = %task.task_id(), topic = %topic_name, "Assigned task to worker");
                    self.task_to_topic
                        .insert(task.task_id().to_string(), topic_name.clone());

                    return Ok(RequestTaskResponse { task: Some(task) });
                }

                trace!("Candidate could not be converted to task, trying another candidate");
            } else {
                trace!("No candidate available, waiting 100ms");
                tokio::time::sleep(poll_interval).await;
            }
        }
    }

    pub async fn complete_task(
        &self,
        request: CompleteTaskRequest,
    ) -> Result<CompleteTaskResponse> {
        let task_id = &request.task_id;

        // Look up the topic name for this task
        let topic_name = self
            .task_to_topic
            .remove(task_id)
            .map(|(_, topic_name)| topic_name)
            .ok_or_else(|| LogMetadataError::TaskNotFound {
                task_id: task_id.clone(),
            })?;

        // Get the topic state
        let mut topic_state =
            self.topics
                .get_mut(&topic_name)
                .ok_or_else(|| LogMetadataError::InvalidArgument {
                    message: format!("Topic {} not found", topic_name),
                })?;

        let (success, pending_candidates) =
            topic_state.complete_task(task_id, request.result.clone())?;

        // Queue any pending partition candidates for processing
        if !pending_candidates.is_empty() {
            let mut candidate_queue = self.candidate_queue.lock().await;
            for candidate in pending_candidates {
                candidate_queue.queue_immediate(candidate);
            }
        }

        Ok(CompleteTaskResponse { success })
    }
}
