use std::sync::Arc;

use tonic::{Request, Response, Status, async_trait};
use wings_resources::NamespaceName;

use crate::{
    error::ResourceErrorExt,
    log_metadata::{
        CompleteTaskRequest, GetLogLocationRequest, ListPartitionsRequest, LogMetadata,
        LogMetadataError, RequestTaskRequest,
    },
    pb::{
        self,
        log_metadata_service_server::{
            LogMetadataService as TonicService, LogMetadataServiceServer as TonicServer,
        },
    },
};

/// A tonic service for managing log metadata.
pub struct LogMetadataServer {
    inner: Arc<dyn LogMetadata>,
}

impl LogMetadataServer {
    /// Create a new tonic log metadata server.
    pub fn new(inner: Arc<dyn LogMetadata>) -> Self {
        Self { inner }
    }

    pub fn into_tonic_server(self) -> TonicServer<Self> {
        TonicServer::new(self)
    }
}

#[async_trait]
impl TonicService for LogMetadataServer {
    async fn commit_folio(
        &self,
        request: Request<pb::CommitFolioRequest>,
    ) -> Result<Response<pb::CommitFolioResponse>, Status> {
        let request = request.into_inner();

        let namespace = NamespaceName::parse(&request.namespace)
            .map_err(|err| err.to_log_metadata_error("namespace"))?;

        let pages = request
            .pages
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, _>>()
            .map_err(LogMetadataError::from)?;

        let response = self
            .inner
            .commit_folio(namespace, request.file_ref, &pages)
            .await?;

        let pages = response.into_iter().map(Into::into).collect();

        Ok(Response::new(pb::CommitFolioResponse { pages }))
    }

    async fn get_log_location(
        &self,
        request: Request<pb::GetLogLocationRequest>,
    ) -> Result<Response<pb::GetLogLocationResponse>, Status> {
        let request: GetLogLocationRequest = request
            .into_inner()
            .try_into()
            .map_err(LogMetadataError::from)?;

        let response = self.inner.get_log_location(request).await?.into();

        Ok(Response::new(response))
    }

    async fn list_partitions(
        &self,
        request: Request<pb::ListPartitionsRequest>,
    ) -> Result<Response<pb::ListPartitionsResponse>, Status> {
        let request: ListPartitionsRequest = request
            .into_inner()
            .try_into()
            .map_err(LogMetadataError::from)?;

        let response = self.inner.list_partitions(request).await?.into();

        Ok(Response::new(response))
    }

    async fn request_task(
        &self,
        request: Request<pb::RequestTaskRequest>,
    ) -> Result<Response<pb::RequestTaskResponse>, Status> {
        let request: RequestTaskRequest = request
            .into_inner()
            .try_into()
            .map_err(LogMetadataError::from)?;

        let response = self.inner.request_task(request).await?.into();

        Ok(Response::new(response))
    }

    async fn complete_task(
        &self,
        request: Request<pb::CompleteTaskRequest>,
    ) -> Result<Response<pb::CompleteTaskResponse>, Status> {
        let request: CompleteTaskRequest = request
            .into_inner()
            .try_into()
            .map_err(LogMetadataError::from)?;

        let response = self.inner.complete_task(request).await?.into();

        Ok(Response::new(response))
    }
}
