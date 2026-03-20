use wings_control_plane_core::log_metadata::{CommittedBatch, LogOffset};

#[derive(Debug, Clone)]
pub struct PageInfo {
    /// The file reference for the page.
    pub file_ref: String,
    /// Where the Parquet file starts in the folio.
    pub offset_bytes: u64,
    /// The size of the Parquet file.
    pub size_bytes: u64,
    /// The end offset of the rows in the page.
    pub end_offset: LogOffset,
    /// The number of rows in the page.
    pub num_rows: usize,
    /// The batches in the page.
    pub batches: Vec<CommittedBatch>,
}
