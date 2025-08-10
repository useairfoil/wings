//! Remote offset registry service implementation that communicates with a remote offset registry service via gRPC.

use std::{
    marker::Send,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    admin::{NamespaceName, TopicName},
    partition::PartitionValue,
    protocol::wings::v1 as pb,
    protocol::wings::v1::offset_registry_service_client::OffsetRegistryServiceClient,
};
use async_trait::async_trait;
use bytes::Bytes;
use http_body::Body;
use snafu::ResultExt;

use super::{
    BatchToCommit, CommittedBatch, OffsetLocation, OffsetRegistry, OffsetRegistryError,
    OffsetRegistryResult, error::InvalidDeadlineSnafu,
};

pub type StdError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Remote offset registry service that communicates with a remote offset registry service via gRPC.
pub struct RemoteOffsetRegistryService<T> {
    client: OffsetRegistryServiceClient<T>,
}

impl<T> RemoteOffsetRegistryService<T>
where
    T: tonic::client::GrpcService<tonic::body::Body> + Clone,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
{
    /// Create a new remote admin service with the given transport.
    pub fn new(inner: T) -> Self {
        Self::new_with_client(OffsetRegistryServiceClient::new(inner))
    }

    /// Create a new remote admin service with the given client.
    pub fn new_with_client(client: OffsetRegistryServiceClient<T>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl<T> OffsetRegistry for RemoteOffsetRegistryService<T>
where
    T: tonic::client::GrpcService<tonic::body::Body> + Send + Sync + Clone,
    <T as tonic::client::GrpcService<tonic::body::Body>>::Future: Send,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
{
    async fn commit_folio(
        &self,
        namespace: NamespaceName,
        file_ref: String,
        batches: &[BatchToCommit],
    ) -> OffsetRegistryResult<Vec<CommittedBatch>> {
        let request = pb::CommitFolioRequest {
            namespace: namespace.to_string(),
            file_ref,
            batches: batches.iter().map(Into::into).collect(),
        };

        self.client
            .clone()
            .commit_folio(request)
            .await
            .map_err(status_to_offset_registry_error)?
            .into_inner()
            .try_into()
    }

    async fn offset_location(
        &self,
        topic: TopicName,
        partition_value: Option<PartitionValue>,
        offset: u64,
        deadline: SystemTime,
    ) -> OffsetRegistryResult<Option<OffsetLocation>> {
        let epoch = deadline
            .duration_since(UNIX_EPOCH)
            .context(InvalidDeadlineSnafu)?;

        let deadline = prost_types::Timestamp {
            seconds: epoch.as_secs() as i64,
            nanos: epoch.subsec_nanos() as i32,
        };

        let request = pb::OffsetLocationRequest {
            topic: topic.to_string(),
            partition: partition_value.as_ref().map(Into::into),
            offset,
            deadline: deadline.into(),
        };

        let response = self
            .client
            .clone()
            .offset_location(request)
            .await
            .map_err(status_to_offset_registry_error)?
            .into_inner();

        Ok(response.into())
    }
}

fn status_to_offset_registry_error(status: tonic::Status) -> OffsetRegistryError {
    OffsetRegistryError::Internal {
        message: format!("error from remote service: {}", status.message()),
    }
}
