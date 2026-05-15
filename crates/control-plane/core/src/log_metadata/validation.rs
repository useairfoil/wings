use std::{cmp::Ordering, collections::HashMap};

use wings_resources::NamespaceName;

use crate::log_metadata::{
    CommitBatchRequest, LogMetadataError, Result, timestamp::compare_batch_request_timestamps,
};

/// Validates that the batches to commit meet the following:
///
/// - topics belong to the namespace
/// - batches are sorted by timestamp within each topic and partition
pub fn validate_batches_to_commit(
    namespace: &NamespaceName,
    batches: &[CommitBatchRequest],
) -> Result<()> {
    let mut last_batch_by_partition = HashMap::new();

    for batch in batches {
        if batch.topic_name.parent() != namespace {
            return Err(LogMetadataError::InvalidArgument {
                message: format!(
                    "topic {} does not belong to namespace {}",
                    batch.topic_name, namespace
                ),
            });
        }

        let partition_key = (batch.topic_name.clone(), batch.partition_value.clone());
        if let Some(previous) = last_batch_by_partition.insert(partition_key, batch)
            && compare_batch_request_timestamps(previous, batch) == Ordering::Greater
        {
            return Err(LogMetadataError::InvalidArgument {
                message: format!(
                    "batches are not sorted by timestamp for topic {} partition {:?}",
                    batch.topic_name, batch.partition_value
                ),
            });
        }
    }

    Ok(())
}
