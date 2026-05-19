use std::time::SystemTime;

use wings_resources::{PartitionValue, TableName};

/// The response to a [`WriteBatchRequest`].
#[derive(Debug, Clone)]
pub struct WriteBatchResponse {
    pub batch_id: u32,
    pub table_name: TableName,
    pub partition_value: Option<PartitionValue>,
    pub folio: FolioPageMetadata,
    // The seqnum of the first row in the Parquet file.
    pub seqnum: u64,
    /// The number of rows in the batch.
    pub num_rows: u32,
    /// The timestamp assigned to the batch.
    pub timestamp: Option<SystemTime>,
}

/// Metadata about a Folio page.
#[derive(Debug, Clone)]
pub struct FolioPageMetadata {
    /// The filename of the Folio page.
    pub file_ref: String,
    /// The seqnum (in bytes) of the page in the folio.
    pub offset_bytes: u64,
    /// The size (in bytes) of the page.
    pub size_bytes: u64,
}
