use std::{sync::Arc, time::SystemTimeError};

use datafusion::error::DataFusionError;
use snafu::Snafu;

use crate::{
    admin::{NamespaceName, TopicName},
    partition::PartitionValue,
    resource::ResourceError,
};

/// Errors that can occur during batch committer operations.
#[derive(Clone, Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum OffsetRegistryError {
    #[snafu(display("duplicate partition value: {topic} {partition:?}"))]
    DuplicatePartitionValue {
        topic: TopicName,
        partition: Option<PartitionValue>,
    },
    #[snafu(display("namespace not found: {namespace}"))]
    NamespaceNotFound { namespace: NamespaceName },
    #[snafu(display(
        "offset not found for topic: {topic}, partition: {partition:?}, offset: {offset}"
    ))]
    OffsetNotFound {
        topic: TopicName,
        partition: Option<PartitionValue>,
        offset: u64,
    },
    #[snafu(display("invalid offset range"))]
    InvalidOffsetRange,
    #[snafu(display("internal error: {message}"))]
    Internal { message: String },
    #[snafu(display("invalid argument: {message}"))]
    InvalidArgument { message: String },
    #[snafu(display("invalid {resource} name"))]
    InvalidResourceName {
        resource: &'static str,
        source: ResourceError,
    },
    #[snafu(display("invalid deadline"))]
    InvalidDeadline { source: SystemTimeError },
    #[snafu(display("invalid timestamp"))]
    InvalidTimestamp {
        source: Arc<prost_types::TimestampError>,
    },
}

pub type OffsetRegistryResult<T, E = OffsetRegistryError> = ::std::result::Result<T, E>;

impl From<OffsetRegistryError> for DataFusionError {
    fn from(err: OffsetRegistryError) -> Self {
        DataFusionError::External(Box::new(err))
    }
}
