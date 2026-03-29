use std::marker::Send;

use async_trait::async_trait;
use bytes::Bytes;
use http_body::Body;
use wings_resources::NamespaceName;

use crate::{
    log_metadata::{
        CommitPageRequest, CommitPageResponse, CompleteTaskRequest, CompleteTaskResponse,
        GetLogLocationRequest, ListPartitionsRequest, ListPartitionsResponse, LogLocation,
        LogMetadata, RequestTaskRequest, RequestTaskResponse, Result,
    },
    pb::{self, log_metadata_service_client::LogMetadataServiceClient as TonicClient},
};

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
            .await?
            .into_inner()
            .try_into()
            .map_err(Into::into)
    }

    async fn get_log_location(&self, request: GetLogLocationRequest) -> Result<Vec<LogLocation>> {
        let request: pb::GetLogLocationRequest = request.try_into()?;

        self.client
            .clone()
            .get_log_location(request)
            .await?
            .into_inner()
            .try_into()
            .map_err(Into::into)
    }

    async fn list_partitions(
        &self,
        request: ListPartitionsRequest,
    ) -> Result<ListPartitionsResponse> {
        let request: pb::ListPartitionsRequest = request.into();

        self.client
            .clone()
            .list_partitions(request)
            .await?
            .into_inner()
            .try_into()
            .map_err(Into::into)
    }

    async fn request_task(&self, request: RequestTaskRequest) -> Result<RequestTaskResponse> {
        let request: pb::RequestTaskRequest = request.into();

        self.client
            .clone()
            .request_task(request)
            .await?
            .into_inner()
            .try_into()
            .map_err(Into::into)
    }

    async fn complete_task(&self, request: CompleteTaskRequest) -> Result<CompleteTaskResponse> {
        let request: pb::CompleteTaskRequest = request.into();

        self.client
            .clone()
            .complete_task(request)
            .await?
            .into_inner()
            .try_into()
            .map_err(Into::into)
    }
}
