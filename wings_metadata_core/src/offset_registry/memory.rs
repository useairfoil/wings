//! In-memory implementation of the batch committer.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet};
use std::ops::Bound;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use dashmap::DashMap;

use crate::admin::{NamespaceName, TopicName};
use crate::offset_registry::{
    BatchToCommit, CommittedBatch, FolioLocation, ListTopicPartitionStatesRequest,
    ListTopicPartitionStatesResponse, OffsetLocation, OffsetRegistry, OffsetRegistryError,
    OffsetRegistryResult,
};
use crate::partition::PartitionValue;

use super::PartitionValueState;

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
    next_offset: u64,
    /// Maps start offset to batch information for lookup
    batches: BTreeMap<u64, BatchInfo>,
}

/// Information about a committed batch
#[derive(Debug, Clone)]
struct BatchInfo {
    pub file_ref: String,
    pub offset_bytes: u64,
    pub size_bytes: u64,
    pub end_offset: u64,
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
        batches: &[BatchToCommit],
    ) -> OffsetRegistryResult<Vec<CommittedBatch>> {
        let mut seen_partitions = HashSet::new();
        for batch in batches {
            if !seen_partitions.insert((batch.topic_name.clone(), batch.partition_value.clone())) {
                return Err(OffsetRegistryError::DuplicatePartitionValue {
                    topic: batch.topic_name.clone(),
                    partition: batch.partition_value.clone(),
                });
            }
        }

        let mut committed_batches = Vec::with_capacity(batches.len());

        for batch in batches {
            // This should have been checked already by the caller.
            assert_eq!(batch.topic_name.parent(), &namespace);

            let partition_key =
                PartitionKey::new(batch.topic_name.clone(), batch.partition_value.clone());

            let mut topic_state =
                self.topics
                    .entry(batch.topic_name.clone())
                    .or_insert_with(|| TopicOffsetState {
                        partitions: BTreeMap::new(),
                    });

            let partition_state = topic_state
                .partitions
                .entry(partition_key.clone())
                .or_insert_with(|| PartitionOffsetState {
                    next_offset: 0,
                    batches: BTreeMap::new(),
                });

            let start_offset = partition_state.next_offset;
            let end_offset = start_offset + batch.num_messages as u64 - 1;

            // Store batch information
            let batch_info = BatchInfo {
                file_ref: file_ref.clone(),
                offset_bytes: batch.offset_bytes,
                size_bytes: batch.batch_size_bytes,
                end_offset,
            };

            partition_state.batches.insert(start_offset, batch_info);
            partition_state.next_offset = end_offset + 1;

            committed_batches.push(CommittedBatch {
                topic_name: batch.topic_name.clone(),
                partition_value: batch.partition_value.clone(),
                start_offset,
                end_offset,
            });
        }

        Ok(committed_batches)
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
        let batch_start = partition_state.batches.range(..=offset).next_back();

        let Some((&start_offset, batch_info)) = batch_start else {
            return Ok(None);
        };

        if offset <= batch_info.end_offset {
            return Ok(OffsetLocation::Folio(FolioLocation {
                file_ref: batch_info.file_ref.clone(),
                offset_bytes: batch_info.offset_bytes,
                size_bytes: batch_info.size_bytes,
                start_offset,
                end_offset: batch_info.end_offset,
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
