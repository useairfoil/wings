use std::sync::Arc;

use snafu::ResultExt;
use tonic::{Request, Response, Status, async_trait};

use crate::{
    log_metadata::{
        CompleteTaskRequest, GetLogLocationRequest, ListPartitionsRequest, LogMetadata,
        LogMetadataError, RequestTaskRequest, error::InvalidResourceNameSnafu,
    },
    resources::{NamespaceName, name::resource_error_to_status},
};

use super::pb::{
    self,
    log_metadata_service_server::{
        LogMetadataService as TonicService, LogMetadataServiceServer as TonicServer,
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
            .context(InvalidResourceNameSnafu {
                resource: "namespace",
            })
            .map_err(log_metadata_error_to_status)?;

        let pages = request
            .pages
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, _>>()
            .map_err(log_metadata_error_to_status)?;

        let response = self
            .inner
            .commit_folio(namespace, request.file_ref, &pages)
            .await
            .map_err(log_metadata_error_to_status)?;

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
            .map_err(log_metadata_error_to_status)?;

        let response = self
            .inner
            .get_log_location(request)
            .await
            .map_err(log_metadata_error_to_status)?
            .into();

        Ok(Response::new(response))
    }

    async fn list_partitions(
        &self,
        request: Request<pb::ListPartitionsRequest>,
    ) -> Result<Response<pb::ListPartitionsResponse>, Status> {
        let request: ListPartitionsRequest = request
            .into_inner()
            .try_into()
            .map_err(log_metadata_error_to_status)?;

        let response = self
            .inner
            .list_partitions(request)
            .await
            .map_err(log_metadata_error_to_status)?
            .into();

        Ok(Response::new(response))
    }

    async fn request_task(
        &self,
        request: Request<pb::RequestTaskRequest>,
    ) -> Result<Response<pb::RequestTaskResponse>, Status> {
        let request: RequestTaskRequest = request
            .into_inner()
            .try_into()
            .map_err(log_metadata_error_to_status)?;

        let response = self
            .inner
            .request_task(request)
            .await
            .map_err(log_metadata_error_to_status)?
            .into();

        Ok(Response::new(response))
    }

    async fn complete_task(
        &self,
        request: Request<pb::CompleteTaskRequest>,
    ) -> Result<Response<pb::CompleteTaskResponse>, Status> {
        let request: CompleteTaskRequest = request
            .into_inner()
            .try_into()
            .map_err(log_metadata_error_to_status)?;

        let response = self
            .inner
            .complete_task(request)
            .await
            .map_err(log_metadata_error_to_status)?
            .into();

        Ok(Response::new(response))
    }
}

fn log_metadata_error_to_status(error: LogMetadataError) -> Status {
    match error {
        LogMetadataError::DuplicatePartitionValue { topic, partition } => Status::already_exists(
            format!("duplicate partition value: topic={topic}, partition={partition:?}",),
        ),
        LogMetadataError::UnorderedPageBatches { topic, partition } => Status::invalid_argument(
            format!("unordered page batches: topic={topic}, partition={partition:?}",),
        ),
        LogMetadataError::NamespaceNotFound { namespace } => {
            Status::not_found(format!("namespace not found: {namespace}"))
        }
        LogMetadataError::OffsetNotFound {
            topic,
            partition,
            offset,
        } => Status::not_found(format!(
            "offset not found: topic={topic}, partition={partition:?}, offset={offset}",
        )),
        LogMetadataError::InvalidOffsetRange => Status::invalid_argument("invalid offset range"),
        LogMetadataError::InvalidArgument { message } => Status::invalid_argument(message.clone()),
        LogMetadataError::InvalidResourceName { resource, source } => {
            resource_error_to_status(resource, source)
        }
        LogMetadataError::Internal { message } => {
            Status::internal(format!("internal error: {message}"))
        }
        LogMetadataError::InvalidDeadline { source } => {
            Status::invalid_argument(format!("invalid deadline: {source}"))
        }
        LogMetadataError::InvalidTimestamp { source } => {
            Status::invalid_argument(format!("invalid timestamp: {source}"))
        }
        LogMetadataError::InvalidDuration { source } => {
            Status::invalid_argument(format!("invalid duration: {source}"))
        }
    }
}
