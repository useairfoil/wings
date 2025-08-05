use thiserror::Error;

/// Error type for the wings_server_core crate.
#[derive(Error, Debug)]
pub enum ServerError {
    #[error("invalid topic name: {0}")]
    InvalidTopicName(String),

    #[error("invalid partition value: {0}")]
    InvalidPartitionValue(String),

    #[error("topic not found: {0}")]
    TopicNotFound(String),

    #[error("partition not found: {0}")]
    PartitionNotFound(String),

    #[error("invalid offset: {0}")]
    InvalidOffset(u64),

    #[error("object store error")]
    ObjectStoreError,

    #[error("metadata error")]
    MetadataError,

    #[error("data fusion error")]
    DataFusionError,

    #[error("timeout exceeded")]
    TimeoutExceeded,

    #[error("invalid request parameters: {0}")]
    InvalidRequest(String),
}

pub type ServerResult<T> = error_stack::Result<T, ServerError>;
