use std::{cmp::Ordering, collections::HashSet};

use wings_resources::NamespaceName;

use crate::log_metadata::{
    CommitPageRequest, LogMetadataError, Result, timestamp::compare_batch_request_timestamps,
};

/// Validates that the pages to commit meet the following:
///
/// - no duplicate partition values
/// - batches are sorted by timestamp
pub fn validate_pages_to_commit(
    namespace: &NamespaceName,
    pages: &[CommitPageRequest],
) -> Result<()> {
    let mut seen_partitions = HashSet::new();
    for page in pages {
        if page.topic_name.parent() != namespace {
            return Err(LogMetadataError::InvalidArgument {
                message: format!(
                    "Topic name {} does not belong to namespace {}",
                    page.topic_name, namespace
                ),
            });
        }

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
