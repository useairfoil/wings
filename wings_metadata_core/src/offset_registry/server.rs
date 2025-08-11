//! gRPC server implementation for the offset registry.

use std::sync::Arc;

use async_trait::async_trait;
use snafu::ResultExt;
use tonic::{Request, Response, Status};

use crate::admin::{NamespaceName, TopicName};
use crate::offset_registry::error::OffsetRegistryError;
use crate::offset_registry::{BatchToCommit, ListTopicPartitionValuesRequest, OffsetRegistry};
use crate::protocol::wings::v1::{
    self as pb,
    offset_registry_service_server::{
        OffsetRegistryService as OffsetRegistryServiceTrait, OffsetRegistryServiceServer,
    },
};
use crate::resource::ResourceError;

use super::error::InvalidResourceNameSnafu;

/// gRPC server implementation for the OffsetRegistryService.
pub struct OffsetRegistryService {
    offset_registry: Arc<dyn OffsetRegistry>,
}

impl OffsetRegistryService {
    /// Create a new OffsetRegistryService server with the given offset registry implementation.
    pub fn new(offset_registry: Arc<dyn OffsetRegistry>) -> Self {
        Self { offset_registry }
    }

    pub fn into_service(self) -> OffsetRegistryServiceServer<Self> {
        OffsetRegistryServiceServer::new(self)
    }
}

#[async_trait]
impl OffsetRegistryServiceTrait for OffsetRegistryService {
    async fn commit_folio(
        &self,
        request: Request<pb::CommitFolioRequest>,
    ) -> Result<Response<pb::CommitFolioResponse>, Status> {
        let request = request.into_inner();

        let namespace_name = NamespaceName::parse(&request.namespace)
            .context(InvalidResourceNameSnafu {
                resource: "namespace",
            })
            .map_err(Into::into)
            .map_err(offset_registry_error_to_status)?;

        let batches: Vec<BatchToCommit> = request
            .batches
            .into_iter()
            .map(|batch| batch.try_into())
            .collect::<Result<Vec<_>, OffsetRegistryError>>()
            .map_err(offset_registry_error_to_status)?;

        let committed_batches = self
            .offset_registry
            .commit_folio(namespace_name, request.file_ref, &batches)
            .await
            .map_err(offset_registry_error_to_status)?;

        let response = pb::CommitFolioResponse {
            batches: committed_batches.into_iter().map(Into::into).collect(),
        };

        Ok(Response::new(response))
    }

    async fn offset_location(
        &self,
        request: Request<pb::OffsetLocationRequest>,
    ) -> Result<Response<pb::OffsetLocationResponse>, Status> {
        let request = request.into_inner();

        let topic_name = TopicName::parse(&request.topic)
            .context(InvalidResourceNameSnafu { resource: "topic" })
            .map_err(Into::into)
            .map_err(offset_registry_error_to_status)?;

        let partition_value = request
            .partition
            .map(TryFrom::try_from)
            .transpose()
            .map_err(offset_registry_error_to_status)?;

        let deadline = request
            .deadline
            .ok_or_else(|| Status::invalid_argument("deadline is required"))?
            .try_into()
            .map_err(|_| Status::invalid_argument("deadline is invalid"))?;

        let offset_location = self
            .offset_registry
            .offset_location(topic_name, partition_value, request.offset, deadline)
            .await
            .map_err(offset_registry_error_to_status)?;

        Ok(Response::new(offset_location.into()))
    }

    async fn list_topic_partition_values(
        &self,
        request: Request<pb::ListTopicPartitionValuesRequest>,
    ) -> Result<Response<pb::ListTopicPartitionValuesResponse>, Status> {
        let request = request.into_inner();

        let topic_name = TopicName::parse(&request.topic)
            .context(InvalidResourceNameSnafu { resource: "topic" })
            .map_err(Into::into)
            .map_err(offset_registry_error_to_status)?;

        let response = self
            .offset_registry
            .list_topic_partition_values(ListTopicPartitionValuesRequest {
                topic_name,
                page_size: request.page_size.map(|v| v as usize),
                page_token: request.page_token,
            })
            .await
            .map_err(offset_registry_error_to_status)?;

        Ok(Response::new(response.into()))
    }
}

fn offset_registry_error_to_status(error: OffsetRegistryError) -> Status {
    match error {
        OffsetRegistryError::DuplicatePartitionValue { topic, partition } => {
            Status::already_exists(format!(
                "duplicate partition value: topic={topic}, partition={partition:?}",
            ))
        }
        OffsetRegistryError::NamespaceNotFound { namespace } => {
            Status::not_found(format!("namespace not found: {namespace}"))
        }
        OffsetRegistryError::OffsetNotFound {
            topic,
            partition,
            offset,
        } => Status::not_found(format!(
            "offset not found: topic={topic}, partition={partition:?}, offset={offset}",
        )),
        OffsetRegistryError::InvalidOffsetRange => Status::invalid_argument("invalid offset range"),
        OffsetRegistryError::InvalidArgument { message } => {
            Status::invalid_argument(message.clone())
        }
        OffsetRegistryError::InvalidResourceName { resource, source } => match source {
            ResourceError::InvalidFormat { expected, actual } => Status::invalid_argument(format!(
                "invalid {resource} name format: expected '{expected}' but got '{actual}'",
            )),
            ResourceError::InvalidName { name } => {
                Status::invalid_argument(format!("invalid {resource} name: {name}"))
            }
            ResourceError::MissingParent { name } => {
                Status::invalid_argument(format!("missing parent {resource} in name: {name}"))
            }
            ResourceError::InvalidResourceId { id } => {
                Status::invalid_argument(format!("invalid {resource} id: {id}"))
            }
        },
        OffsetRegistryError::Internal { message } => {
            Status::internal(format!("internal error: {message}"))
        }
        OffsetRegistryError::InvalidDeadline { source } => {
            Status::invalid_argument(format!("invalid deadline: {source}"))
        }
    }
}
