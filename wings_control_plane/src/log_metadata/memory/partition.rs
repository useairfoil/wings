use std::{
    cmp::Ordering,
    collections::BTreeMap,
    fmt::Display,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use tokio::sync::{Notify, futures::OwnedNotified};
use tracing::{debug, trace};

use crate::{
    log_metadata::{
        AcceptedBatchInfo, CommitPageRequest, CommitPageResponse, CommittedBatch, FolioLocation,
        GetLogLocationOptions, LogLocation, LogOffset, RejectedBatchInfo,
        error::Result,
        memory::candidate::CandidateTask,
        timestamp::{ValidateRequestResult, validate_timestamp_in_request},
    },
    resources::{CompactionConfiguration, PartitionValue, TopicName},
};

use super::page::PageInfo;

/// A partition key used to identify unique (topic, partition_value) combinations.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PartitionKey {
    pub topic_name: TopicName,
    pub partition_value: Option<PartitionValue>,
}

#[derive(Debug, Clone)]
pub struct PartitionLogState {
    key: PartitionKey,
    /// Next offset to be assigned
    next_offset: LogOffset,
    /// Maps start offset to page information for lookup.
    pages: BTreeMap<u64, PageInfo>,
    /// Maps timestamp to the first offset containing that timestamp.
    /// Used for efficient timestamp-based queries.
    timestamp_index: BTreeMap<SystemTime, u64>,
    /// Notify when a new page is added.
    notify: Arc<Notify>,
    /// Last time a candidate task was generated for this partition
    last_candidate_task_time: Option<Instant>,
}

#[derive(Debug)]
pub enum GetLogLocationResult {
    Done,
    NeedMore(OwnedNotified),
}

impl PartitionLogState {
    pub fn new(key: PartitionKey) -> Self {
        Self {
            key,
            next_offset: LogOffset::default(),
            pages: BTreeMap::new(),
            timestamp_index: BTreeMap::new(),
            notify: Arc::new(Notify::new()),
            last_candidate_task_time: None,
        }
    }

    pub fn next_offset(&self) -> LogOffset {
        self.next_offset
    }

    /// Check if we should generate a candidate task for compaction based on the configuration
    fn should_generate_candidate_task(
        &mut self,
        config: &CompactionConfiguration,
    ) -> Option<(CandidateTask, Duration)> {
        // Check if enough time has passed since the last candidate task
        let now = Instant::now();
        if let Some(last_time) = self.last_candidate_task_time {
            if now.duration_since(last_time) < config.freshness {
                return None;
            }
        }

        self.last_candidate_task_time = Some(now);

        let task = CandidateTask::Partition(
            self.key.topic_name.clone(),
            self.key.partition_value.clone(),
        );

        Some((task, config.freshness))
    }

    pub fn commit_page(
        &mut self,
        page: &CommitPageRequest,
        file_ref: String,
        now_ts: SystemTime,
        compaction_config: Option<&CompactionConfiguration>,
    ) -> Result<(CommitPageResponse, Option<(CandidateTask, Duration)>)> {
        let start_offset = self.next_offset;

        let mut batches = Vec::new();

        let mut current_offset = start_offset;

        // TODO: check that the timestamp is assigned correctly
        // we want the state to have the timestamp of the most recently assigned batch
        for batch in page.batches.iter() {
            match validate_timestamp_in_request(&current_offset, batch) {
                ValidateRequestResult::Reject { reason } => {
                    let rejected = RejectedBatchInfo {
                        num_messages: batch.num_messages,
                        reason: reason.to_string(),
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

            self.notify.notify_waiters();
        }

        // Check if we should generate a candidate task for compaction
        let candidate_task = if let Some(config) = compaction_config {
            self.should_generate_candidate_task(config)
        } else {
            None
        };

        let response = CommitPageResponse {
            topic_name: page.topic_name.clone(),
            partition_value: page.partition_value.clone(),
            batches,
        };

        Ok((response, candidate_task))
    }

    pub fn get_log_location(
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
