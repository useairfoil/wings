use std::time::SystemTime;

use arrow::array::RecordBatch;
use wings_resources::{PartitionValue, TopicRef};

/// A request to the ingestor to write a batch of records to a topic.
#[derive(Debug, Clone)]
pub struct WriteBatchRequest {
    pub topic: TopicRef,
    pub partition: Option<PartitionValue>,
    pub records: RecordBatch,
    pub timestamp: Option<SystemTime>,
}
