use arrow::array::RecordBatch;
use tokio::sync::oneshot;
use wings_metadata_core::{
    admin::{NamespaceRef, TopicRef},
    partition::PartitionValue,
};

use crate::error::{Result, ValidationSnafu};

#[derive(Debug)]
pub struct Batch {
    pub namespace: NamespaceRef,
    pub topic: TopicRef,
    pub partition: Option<PartitionValue>,
    pub records: RecordBatch,
}

#[derive(Debug, Clone)]
pub struct WriteInfo {
    pub start_offset: u64,
    pub end_offset: u64,
}

pub type WriteReplySender = oneshot::Sender<Result<WriteInfo>>;

impl Batch {
    pub fn validate(&self) -> Result<()> {
        match (self.topic.partition_column(), self.partition.as_ref()) {
            (None, None) => {}
            (None, Some(_)) => {
                return ValidationSnafu {
                    message: format!(
                        "topic {} does not specify a partition key but batch contains partition data",
                        self.topic.name
                    ),
                }
                .fail();
            }
            (Some(_), None) => {
                return ValidationSnafu {
                    message: format!(
                        "topic {} specifies a partition key but batch does not contain partition data",
                        self.topic.name
                    ),
                }
                .fail();
            }
            (Some(col), Some(partition)) => {
                if col.data_type() != &partition.data_type() {
                    return ValidationSnafu {
                            message: format!(
                                "topic {} partition column data type {} does not match batch partition data type {}",
                                self.topic.name,
                                col.data_type(),
                                partition.data_type()
                            ),
                        }
                        .fail();
                }
            }
        }

        if self.topic.name.parent() != &self.namespace.name {
            return ValidationSnafu {
                message: format!(
                    "topic namespace {} does not match provided namespace {}",
                    self.topic.name.parent(),
                    self.namespace.name
                ),
            }
            .fail();
        }

        Ok(())
    }
}
