//! In-memory implementation of the log metadata trait.
//!
//! This implementation stores all data in memory and is suitable for testing
//! and development.

use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashSet},
    ops::Bound,
    sync::Arc,
    time::SystemTime,
};

use async_trait::async_trait;
use dashmap::DashMap;
use tracing::debug;

use crate::{
    log_metadata::{
        AcceptedBatchInfo, RejectedBatchInfo,
        timestamp::{ValidateRequestResult, validate_timestamp_in_request},
    },
    resources::{NamespaceName, PartitionValue, TopicName},
};

use super::{
    CommitPageRequest, CommitPageResponse, CommittedBatch, FolioLocation, GetLogLocationRequest,
    ListPartitionsRequest, ListPartitionsResponse, LogLocation, LogLocationRequest, LogMetadata,
    LogMetadataError, LogOffset, PartitionMetadata, Result,
    timestamp::compare_batch_request_timestamps,
};

#[derive(Debug, Clone, Default)]
pub struct InMemoryLogMetadata {
    /// Maps topic names to their log state
    topics: Arc<DashMap<TopicName, TopicLogState>>,
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

#[derive(Debug, Clone)]
struct PartitionLogState {
    /// Next offset to be assigned
    next_offset: LogOffset,
    /// Maps start offset to page information for lookup.
    pages: BTreeMap<u64, PageInfo>,
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

        let committed_pages = pages
            .iter()
            .map(|page| {
                // This should have been checked already by the caller.
                assert_eq!(page.topic_name.parent(), &namespace);

                let partition_key =
                    PartitionKey::new(page.topic_name.clone(), page.partition_value.clone());

                let mut topic_state =
                    self.topics
                        .entry(page.topic_name.clone())
                        .or_insert_with(|| TopicLogState {
                            partitions: BTreeMap::new(),
                        });

                topic_state.commit_page(partition_key, page, file_ref.clone(), now_ts)
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(committed_pages)
    }

    async fn get_log_location(
        &self,
        request: GetLogLocationRequest,
    ) -> Result<Option<LogLocation>> {
        let Some(topic_state) = self.topics.get(&request.topic_name) else {
            return Ok(None);
        };

        let partition_key = PartitionKey::new(request.topic_name, request.partition_value);
        topic_state.get_log_location(partition_key, request.location, request.deadline)
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

        let page_size = request.page_size.unwrap_or(100);

        // TODO: fetch topic schema and use it to parse partition value from page token string.
        // For now just panic.
        assert!(
            request.page_token.is_none(),
            "pagination token is not supported yet"
        );
        let start_key_range: Bound<&PartitionKey> = Bound::Unbounded;

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

        Ok(ListPartitionsResponse {
            partitions,
            next_page_token,
        })
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
    ) -> Result<CommitPageResponse> {
        let partition_state = self
            .partitions
            .entry(partition_key.clone())
            .or_insert_with(|| PartitionLogState {
                next_offset: LogOffset::default(),
                pages: BTreeMap::new(),
            });

        partition_state.commit_page(page, file_ref, now_ts)
    }

    fn get_log_location(
        &self,
        partition_key: PartitionKey,
        location: LogLocationRequest,
        deadline: Option<SystemTime>,
    ) -> Result<Option<LogLocation>> {
        let Some(partition_state) = self.partitions.get(&partition_key) else {
            return Ok(None);
        };

        partition_state.get_log_location(location, deadline)
    }
}

impl PartitionLogState {
    pub fn commit_page(
        &mut self,
        page: &CommitPageRequest,
        file_ref: String,
        now_ts: SystemTime,
    ) -> Result<CommitPageResponse> {
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
            self.next_offset = current_offset;
        }

        Ok(CommitPageResponse {
            topic_name: page.topic_name.clone(),
            partition_value: page.partition_value.clone(),
            batches,
        })
    }

    fn get_log_location(
        &self,
        location: LogLocationRequest,
        deadline: Option<SystemTime>,
    ) -> Result<Option<LogLocation>> {
        match location {
            LogLocationRequest::Offset(offset) => self.get_log_location_by_offset(offset, deadline),
        }
    }

    fn get_log_location_by_offset(
        &self,
        offset: u64,
        _deadline: Option<SystemTime>,
    ) -> Result<Option<LogLocation>> {
        // Find the batch containing this offset
        let batch_start = self.pages.range(..=offset).next_back();

        let Some((&_start_offset, batch_info)) = batch_start else {
            return Ok(None);
        };

        if offset <= batch_info.end_offset.offset {
            return Ok(LogLocation::Folio(FolioLocation {
                file_ref: batch_info.file_ref.clone(),
                offset_bytes: batch_info.offset_bytes,
                size_bytes: batch_info.size_bytes,
                batches: batch_info.batches.clone(),
            })
            .into());
        }

        Ok(None)
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
            (Some(self_value), Some(other_value)) => self_value.cmp(&other_value),
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Less,
            (Some(_), None) => Ordering::Greater,
        }
    }
}
