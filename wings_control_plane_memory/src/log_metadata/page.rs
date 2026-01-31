use std::{cmp::Ordering, collections::HashSet};

use wings_control_plane_core::log_metadata::{
    CommitPageRequest, CommittedBatch, LogMetadataError, LogOffset, Result,
    timestamp::compare_batch_request_timestamps,
};

#[derive(Debug, Clone)]
pub struct PageInfo {
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

/// Validates that the pages to commit meet the following:
///
/// - no duplicate partition values
/// - batches are sorted by timestamp
pub fn validate_pages_to_commit(pages: &[CommitPageRequest]) -> Result<()> {
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
