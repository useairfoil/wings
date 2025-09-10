use std::{fmt::Debug, sync::Arc, time::SystemTime};

use datafusion::common::arrow::array::RecordBatch;
use parquet::errors::ParquetError;
use snafu::Snafu;
use tokio::sync::oneshot;
use wings_control_plane::{
    log_metadata::{AcceptedBatchInfo, LogMetadataError, RejectedBatchInfo},
    resources::{NamespaceRef, PartitionValue, TopicRef},
};

#[derive(Debug, Clone)]
pub struct WriteBatchRequest {
    pub namespace: NamespaceRef,
    pub topic: TopicRef,
    pub partition: Option<PartitionValue>,
    pub records: RecordBatch,
    pub timestamp: Option<SystemTime>,
}

#[derive(Debug, Clone, Snafu)]
pub enum WriteBatchError {
    #[snafu(display("Validation error: {}", message))]
    Validation { message: String },
    #[snafu(display("Parquet error: {}", message))]
    Parquet {
        message: String,
        source: Arc<ParquetError>,
    },
    #[snafu(display("Object store error: {}", message))]
    ObjectStore {
        message: String,
        source: Arc<object_store::Error>,
    },
    #[snafu(display("Log metadata error: {}", message))]
    LogMetadata {
        message: String,
        source: LogMetadataError,
    },
    #[snafu(display("Batch rejected"))]
    BatchRejected { info: RejectedBatchInfo },
    #[snafu(display("Internal error: {}", message))]
    Internal { message: String },
}

pub type Result<T, E = WriteBatchError> = std::result::Result<T, E>;

pub type WriteBatchResultSender = oneshot::Sender<Result<AcceptedBatchInfo>>;

pub struct WithReplyChannel<T> {
    pub reply: WriteBatchResultSender,
    pub data: T,
}

#[must_use]
pub struct ReplyWithWriteBatchError {
    replies: Vec<(WriteBatchResultSender, WriteBatchError)>,
}

impl WriteBatchRequest {
    pub fn validate(&self) -> Result<()> {
        match (self.topic.partition_field(), self.partition.as_ref()) {
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

impl ReplyWithWriteBatchError {
    pub fn new_empty() -> Self {
        Self {
            replies: Vec::new(),
        }
    }

    pub fn new_fanout(error: WriteBatchError, replies: Vec<WriteBatchResultSender>) -> Self {
        Self {
            replies: replies
                .into_iter()
                .map(|reply| (reply, error.clone()))
                .collect(),
        }
    }

    pub fn new_single(error: WriteBatchError, reply: WriteBatchResultSender) -> Self {
        Self {
            replies: vec![(reply, error)],
        }
    }

    pub fn add_error(&mut self, error: WriteBatchError, reply: WriteBatchResultSender) {
        self.replies.push((reply, error));
    }

    pub fn merge(&mut self, other: Self) {
        self.replies.extend(other.replies);
    }

    pub fn send_to_all(self) {
        for (reply, error) in self.replies {
            let _ = reply.send(Err(error));
        }
    }
}

impl<T> From<ReplyWithWriteBatchError> for std::result::Result<T, ReplyWithWriteBatchError> {
    fn from(value: ReplyWithWriteBatchError) -> Self {
        Err(value)
    }
}

impl<T: Debug> Debug for WithReplyChannel<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WithReplyChannel")
            .field("data", &self.data)
            .finish()
    }
}
