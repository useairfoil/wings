use std::marker::Send;

use async_trait::async_trait;
use bytes::Bytes;
use http_body::Body;

use crate::{
    log_metadata::{
        CommitPageRequest, CommitPageResponse, GetLogLocationRequest, ListPartitionsRequest,
        ListPartitionsResponse, LogLocation, LogMetadata, LogMetadataError, Result,
    },
    resources::NamespaceName,
};

use super::pb::{self, log_metadata_service_client::LogMetadataServiceClient as TonicClient};

type StdError = Box<dyn std::error::Error + Send + Sync + 'static>;

pub struct LogMetadataClient<T> {
    client: TonicClient<T>,
}

impl<T> LogMetadataClient<T>
where
    T: tonic::client::GrpcService<tonic::body::Body> + Clone,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
{
    /// Create a new remote log metadata service with the given transport.
    pub fn new(inner: T) -> Self {
        Self::new_with_client(TonicClient::new(inner))
    }

    /// Create a new remote log metadata service with the given client.
    pub fn new_with_client(client: TonicClient<T>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl<T> LogMetadata for LogMetadataClient<T>
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
        pages: &[CommitPageRequest],
    ) -> Result<Vec<CommitPageResponse>> {
        let pages = pages.iter().map(Into::into).collect::<Vec<_>>();
        let request = pb::CommitFolioRequest {
            namespace: namespace.to_string(),
            file_ref,
            pages,
        };

        self.client
            .clone()
            .commit_folio(request)
            .await
            .map_err(status_to_log_metadata_error)?
            .into_inner()
            .try_into()
    }

    async fn get_log_location(&self, request: GetLogLocationRequest) -> Result<Vec<LogLocation>> {
        let request: pb::GetLogLocationRequest = request.try_into()?;

        let response = self
            .client
            .clone()
            .get_log_location(request)
            .await
            .map_err(status_to_log_metadata_error)?
            .into_inner();

        response.try_into()
    }

    async fn list_partitions(
        &self,
        request: ListPartitionsRequest,
    ) -> Result<ListPartitionsResponse> {
        let request: pb::ListPartitionsRequest = request.into();

        let response = self
            .client
            .clone()
            .list_partitions(request)
            .await
            .map_err(status_to_log_metadata_error)?
            .into_inner()
            .try_into()?;

        Ok(response)
    }
}

fn status_to_log_metadata_error(status: tonic::Status) -> LogMetadataError {
    LogMetadataError::Internal {
        message: format!("error from remote service: {}", status.message()),
    }
}
