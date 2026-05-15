use std::sync::Arc;

use tonic::{Request, Response, Status, async_trait};
use wings_resources::NamespaceName;

use crate::{
    error::ResourceErrorExt,
    table_metadata::{
        CompleteTaskRequest, GetTableLocationRequest, ListPartitionsRequest, TableMetadata,
        TableMetadataError, RequestTaskRequest,
    },
    pb::{
        self,
        table_metadata_service_server::{
            TableMetadataService as TonicService, TableMetadataServiceServer as TonicServer,
        },
    },
};

/// A tonic service for managing log metadata.
pub struct TableMetadataServer {
    inner: Arc<dyn TableMetadata>,
}

impl TableMetadataServer {
    /// Create a new tonic log metadata server.
    pub fn new(inner: Arc<dyn TableMetadata>) -> Self {
        Self { inner }
    }

    pub fn into_tonic_server(self) -> TonicServer<Self> {
        TonicServer::new(self)
    }
}

#[async_trait]
impl TonicService for TableMetadataServer {
    async fn commit(
        &self,
        request: Request<pb::CommitRequest>,
    ) -> Result<Response<pb::CommitResponse>, Status> {
        let request = request.into_inner();

        let namespace = NamespaceName::parse(&request.namespace)
            .map_err(|err| err.to_table_metadata_error("namespace"))?;

        let batches = request
            .batches
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, _>>()
            .map_err(TableMetadataError::from)?;

        let response = self.inner.commit(namespace, batches).await?;

        let batches = response.into_iter().map(Into::into).collect();

        Ok(Response::new(pb::CommitResponse { batches }))
    }

    async fn get_table_location(
        &self,
        request: Request<pb::GetTableLocationRequest>,
    ) -> Result<Response<pb::GetTableLocationResponse>, Status> {
        let request: GetTableLocationRequest = request
            .into_inner()
            .try_into()
            .map_err(TableMetadataError::from)?;

        let response = self.inner.get_table_location(request).await?.into();

        Ok(Response::new(response))
    }

    async fn list_partitions(
        &self,
        request: Request<pb::ListPartitionsRequest>,
    ) -> Result<Response<pb::ListPartitionsResponse>, Status> {
        let request: ListPartitionsRequest = request
            .into_inner()
            .try_into()
            .map_err(TableMetadataError::from)?;

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
            .map_err(TableMetadataError::from)?;

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
            .map_err(TableMetadataError::from)?;

        let response = self.inner.complete_task(request).await?.into();

        Ok(Response::new(response))
    }
}
