//! In-memory implementation of the batch committer.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet};
use std::ops::Bound;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use dashmap::DashMap;
use tracing::debug;

use crate::admin::{NamespaceName, TopicName};
use crate::offset_registry::timestamp::{LogOffset, compare_batch_request_timestamps};
use crate::offset_registry::{
    AcceptedBatchInfo, CommitPageRequest, CommitPageResponse, CommittedBatch, FolioLocation,
    ListTopicPartitionStatesRequest, ListTopicPartitionStatesResponse, OffsetLocation,
    OffsetRegistry, OffsetRegistryError, OffsetRegistryResult,
};
use crate::partition::PartitionValue;

use super::PartitionValueState;
use super::timestamp::ValidateRequestResult;

/// A partition key used to identify unique (topic, partition_value) combinations.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PartitionKey {
    topic_id: TopicName,
    partition_value: Option<PartitionValue>,
}

/// In-memory implementation of the batch committer.
///
/// This implementation stores offset counters in memory for each (topic, partition value) tuple.
/// It's primarily intended for testing and development purposes.
/// State for a single namespace
#[derive(Debug, Clone)]
struct TopicOffsetState {
    /// Maps partition keys to their offset tracking
    partitions: BTreeMap<PartitionKey, PartitionOffsetState>,
}

/// State for tracking offsets in a partition
#[derive(Debug, Clone)]
struct PartitionOffsetState {
    /// Next offset to be assigned
    next_offset: LogOffset,
    /// Maps start offset to page information for lookup.
    pages: BTreeMap<u64, PageInfo>,
}

/// Information about a committed page of batches.
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

#[derive(Debug, Clone)]
pub struct InMemoryOffsetRegistry {
    /// Maps topic names to their offset state
    topics: Arc<DashMap<TopicName, TopicOffsetState>>,
}

impl InMemoryOffsetRegistry {
    /// Create a new in-memory batch committer.
    pub fn new() -> Self {
        Self {
            topics: Arc::new(DashMap::new()),
        }
    }
}

#[async_trait]
impl OffsetRegistry for InMemoryOffsetRegistry {
    async fn commit_folio(
        &self,
        namespace: NamespaceName,
        file_ref: String,
        pages: &[CommitPageRequest],
    ) -> OffsetRegistryResult<Vec<CommitPageResponse>> {
        let mut seen_partitions = HashSet::new();
        for page in pages {
            if !seen_partitions.insert((page.topic_name.clone(), page.partition_value.clone())) {
                return Err(OffsetRegistryError::DuplicatePartitionValue {
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
                return Err(OffsetRegistryError::UnorderedPageBatches {
                    topic: page.topic_name.clone(),
                    partition: page.partition_value.clone(),
                });
            }
        }

        let mut committed_pages = Vec::with_capacity(pages.len());

        // Assign the same timestamp to all batches across pages.
        let now_ts = SystemTime::now();

        for page in pages {
            // This should have been checked already by the caller.
            assert_eq!(page.topic_name.parent(), &namespace);

            let partition_key =
                PartitionKey::new(page.topic_name.clone(), page.partition_value.clone());

            let mut topic_state = self
                .topics
                .entry(page.topic_name.clone())
                .or_insert_with(|| TopicOffsetState {
                    partitions: BTreeMap::new(),
                });

            let partition_state = topic_state
                .partitions
                .entry(partition_key.clone())
                .or_insert_with(|| PartitionOffsetState {
                    next_offset: LogOffset::default(),
                    pages: BTreeMap::new(),
                });

            let start_offset = partition_state.next_offset;

            let mut batches = Vec::new();

            let mut current_offset = start_offset;

            for batch in page.batches.iter() {
                match current_offset.validate_request(batch) {
                    ValidateRequestResult::Reject => {
                        batches.push(CommittedBatch::Rejected {
                            num_messages: batch.num_messages,
                        });
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
                        batches.push(CommittedBatch::Accepted(accepted));
                        current_offset = next_offset;
                    }
                }
            }

            // Update state only if we accepted any data.
            if current_offset != start_offset {
                current_offset = current_offset.with_timestamp(now_ts);

                let end_offset = current_offset.previous();

                let batch_info = PageInfo {
                    file_ref: file_ref.clone(),
                    offset_bytes: page.offset_bytes,
                    size_bytes: page.batch_size_bytes,
                    end_offset,
                    batches: batches.clone(),
                };

                debug!(
                    offset = start_offset.offset,
                    next_offset = ?current_offset,
                    batch_info = ?batch_info,
                    "Updating partition state"
                );

                partition_state
                    .pages
                    .insert(start_offset.offset, batch_info);
                partition_state.next_offset = current_offset;
            }

            committed_pages.push(CommitPageResponse {
                topic_name: page.topic_name.clone(),
                partition_value: page.partition_value.clone(),
                batches,
            });
        }

        Ok(committed_pages)
    }

    async fn offset_location(
        &self,
        topic: TopicName,
        partition_value: Option<PartitionValue>,
        offset: u64,
        _deadline: SystemTime,
    ) -> OffsetRegistryResult<Option<OffsetLocation>> {
        let Some(topic_state) = self.topics.get(&topic) else {
            return Ok(None);
        };

        let partition_key = PartitionKey::new(topic.clone(), partition_value.clone());

        let Some(partition_state) = topic_state.partitions.get(&partition_key) else {
            return Ok(None);
        };

        // Find the batch containing this offset
        let batch_start = partition_state.pages.range(..=offset).next_back();

        let Some((&_start_offset, batch_info)) = batch_start else {
            return Ok(None);
        };

        if offset <= batch_info.end_offset.offset {
            return Ok(OffsetLocation::Folio(FolioLocation {
                file_ref: batch_info.file_ref.clone(),
                offset_bytes: batch_info.offset_bytes,
                size_bytes: batch_info.size_bytes,
                batches: batch_info.batches.clone(),
            })
            .into());
        }

        Ok(None)
    }

    async fn list_topic_partition_states(
        &self,
        request: ListTopicPartitionStatesRequest,
    ) -> OffsetRegistryResult<ListTopicPartitionStatesResponse> {
        let Some(topic_state) = self.topics.get(&request.topic_name) else {
            return Ok(ListTopicPartitionStatesResponse {
                states: vec![],
                next_page_token: None,
            });
        };

        let page_size = request.page_size.unwrap_or(100);

        if request.page_token.is_some() {
            // TODO: this is a hack to avoid infinite runs later

            return Ok(ListTopicPartitionStatesResponse {
                states: vec![],
                next_page_token: None,
            });
        }

        // TODO: fetch topic schema and use it to parse partition value from string.
        let start_key_range: Bound<&PartitionKey> = Bound::Unbounded;

        let mut states = Vec::new();
        let mut next_page_token = None;

        for (key, state) in topic_state
            .partitions
            .range((start_key_range, Bound::Unbounded))
            .take(page_size)
        {
            next_page_token = key.partition_value.as_ref().map(|pv| pv.to_string());
            states.push(PartitionValueState {
                partition_value: key.partition_value.clone(),
                next_offset: state.next_offset,
            });
        }

        Ok(ListTopicPartitionStatesResponse {
            states,
            next_page_token,
        })
    }
}

impl PartitionKey {
    fn new(topic_id: TopicName, partition_value: Option<PartitionValue>) -> Self {
        Self {
            topic_id,
            partition_value,
        }
    }
}

impl Default for InMemoryOffsetRegistry {
    fn default() -> Self {
        Self::new()
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
