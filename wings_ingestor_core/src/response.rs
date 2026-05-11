use std::time::SystemTime;

use wings_resources::{PartitionValue, TopicName};

/// The response to a [`WriteBatchRequest`].
#[derive(Debug, Clone)]
pub struct WriteBatchResponse {
    pub topic_name: TopicName,
    pub partition_value: Option<PartitionValue>,
    pub folio: FolioPageMetadata,
    // The offset of the first row in the Parquet file.
    pub offset: u64,
    /// The number of rows in the batch.
    pub num_rows: u32,
    /// The timestamp assigned to the batch.
    pub timestamp: SystemTime,
}

/// Metadata about a Folio page.
#[derive(Debug, Clone)]
pub struct FolioPageMetadata {
    /// The filename of the Folio page.
    pub file_ref: String,
    /// The offset (in bytes) of the page in the folio.
    pub offset_bytes: u64,
    /// The size (in bytes) of the page.
    pub size_bytes: u64,
}
