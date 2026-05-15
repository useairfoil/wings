use std::time::SystemTime;

use arrow::array::RecordBatch;
use wings_resources::{PartitionValue, TopicRef};

/// A request to the ingestor to write a batch of records to a topic.
#[derive(Debug, Clone)]
pub struct WriteBatchRequest {
    pub batch_id: u32,
    pub topic: TopicRef,
    pub partition: Option<PartitionValue>,
    pub records: RecordBatch,
    pub timestamp: Option<SystemTime>,
}

impl WriteBatchRequest {
    /// Creates a new [`WriteBatchRequest`] with the given topic and records.
    ///
    /// Defaults:
    /// - `batch_id`: 0
    /// - `partition`: `None`
    /// - `timestamp`: `None`
    pub fn new(topic: TopicRef, records: RecordBatch) -> Self {
        Self {
            batch_id: 0,
            topic,
            partition: None,
            records,
            timestamp: None,
        }
    }

    /// Sets the partition for this request.
    pub fn with_partition(mut self, partition: PartitionValue) -> Self {
        self.partition = Some(partition);
        self
    }

    /// Sets the batch id for this request.
    pub fn with_batch_id(mut self, batch_id: u32) -> Self {
        self.batch_id = batch_id;
        self
    }

    /// Sets the timestamp for this request.
    pub fn with_timestamp(mut self, timestamp: SystemTime) -> Self {
        self.timestamp = Some(timestamp);
        self
    }
}
