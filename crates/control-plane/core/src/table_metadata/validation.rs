use std::{cmp::Ordering, collections::HashMap};

use wings_resources::NamespaceName;

use crate::table_metadata::{
    CommitBatchRequest, TableMetadataError, Result, timestamp::compare_batch_request_timestamps,
};

/// Validates that the batches to commit meet the following:
///
/// - tables belong to the namespace
/// - batches are sorted by timestamp within each table and partition
pub fn validate_batches_to_commit(
    namespace: &NamespaceName,
    batches: &[CommitBatchRequest],
) -> Result<()> {
    let mut last_batch_by_partition = HashMap::new();

    for batch in batches {
        if batch.table_name.parent() != namespace {
            return Err(TableMetadataError::InvalidArgument {
                message: format!(
                    "table {} does not belong to namespace {}",
                    batch.table_name, namespace
                ),
            });
        }

        let partition_key = (batch.table_name.clone(), batch.partition_value.clone());
        if let Some(previous) = last_batch_by_partition.insert(partition_key, batch)
            && compare_batch_request_timestamps(previous, batch) == Ordering::Greater
        {
            return Err(TableMetadataError::InvalidArgument {
                message: format!(
                    "batches are not sorted by timestamp for table {} partition {:?}",
                    batch.table_name, batch.partition_value
                ),
            });
        }
    }

    Ok(())
}
