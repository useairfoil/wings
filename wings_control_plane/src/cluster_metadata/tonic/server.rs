use std::sync::Arc;

use snafu::ResultExt;
use tonic::{Request, Response, Status, async_trait};

use crate::{
    cluster_metadata::{
        ClusterMetadata, ClusterMetadataError, ListNamespacesRequest, ListTenantsRequest,
        ListTopicsRequest, error::InvalidResourceNameSnafu,
    },
    resources::{
        NamespaceName, NamespaceOptions, TenantName, TopicName, TopicOptions,
        name::resource_error_to_status,
    },
};

use super::pb::{
    self,
    cluster_metadata_service_server::{
        ClusterMetadataService as TonicService, ClusterMetadataServiceServer as TonicServer,
    },
};

/// A tonic service for managing cluster metadata.
pub struct ClusterMetadataServer {
    inner: Arc<dyn ClusterMetadata>,
}

impl ClusterMetadataServer {
    /// Create a new tonic cluster metadata server.
    pub fn new(inner: Arc<dyn ClusterMetadata>) -> Self {
        Self { inner }
    }

    pub fn into_tonic_server(self) -> TonicServer<Self> {
        TonicServer::new(self)
    }
}

#[async_trait]
impl TonicService for ClusterMetadataServer {
    async fn create_tenant(
        &self,
        request: Request<pb::CreateTenantRequest>,
    ) -> Result<Response<pb::Tenant>, Status> {
        let request = request.into_inner();

        let tenant_name = TenantName::new(request.tenant_id)
            .context(InvalidResourceNameSnafu { resource: "tenant" })
            .map_err(cluster_metadata_error_to_status)?;

        let tenant = self
            .inner
            .create_tenant(tenant_name)
            .await
            .map_err(cluster_metadata_error_to_status)?;

        Ok(Response::new(tenant.into()))
    }

    async fn get_tenant(
        &self,
        request: Request<pb::GetTenantRequest>,
    ) -> Result<Response<pb::Tenant>, Status> {
        let request = request.into_inner();

        let tenant_name = TenantName::parse(&request.name)
            .context(InvalidResourceNameSnafu { resource: "tenant" })
            .map_err(cluster_metadata_error_to_status)?;

        let tenant = self
            .inner
            .get_tenant(tenant_name)
            .await
            .map_err(cluster_metadata_error_to_status)?;

        Ok(Response::new(tenant.into()))
    }

    async fn list_tenants(
        &self,
        request: Request<pb::ListTenantsRequest>,
    ) -> Result<Response<pb::ListTenantsResponse>, Status> {
        let request = ListTenantsRequest::from(request.into_inner());

        let response = self
            .inner
            .list_tenants(request)
            .await
            .map_err(cluster_metadata_error_to_status)?;

        Ok(Response::new(response.into()))
    }

    async fn delete_tenant(
        &self,
        request: Request<pb::DeleteTenantRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();

        let tenant_name = TenantName::parse(&request.name)
            .context(InvalidResourceNameSnafu { resource: "tenant" })
            .map_err(cluster_metadata_error_to_status)?;

        self.inner
            .delete_tenant(tenant_name)
            .await
            .map_err(cluster_metadata_error_to_status)?;

        Ok(Response::new(()))
    }

    async fn create_namespace(
        &self,
        request: Request<pb::CreateNamespaceRequest>,
    ) -> Result<Response<pb::Namespace>, Status> {
        let request = request.into_inner();

        let tenant_name = TenantName::parse(&request.parent)
            .context(InvalidResourceNameSnafu { resource: "tenant" })
            .map_err(cluster_metadata_error_to_status)?;

        let namespace_name = NamespaceName::new(request.namespace_id, tenant_name)
            .context(InvalidResourceNameSnafu {
                resource: "namespace",
            })
            .map_err(cluster_metadata_error_to_status)?;

        let options = NamespaceOptions::try_from(request.namespace.unwrap_or_default())
            .map_err(cluster_metadata_error_to_status)?;

        let namespace = self
            .inner
            .create_namespace(namespace_name, options)
            .await
            .map_err(cluster_metadata_error_to_status)?;

        Ok(Response::new(namespace.into()))
    }

    async fn get_namespace(
        &self,
        request: Request<pb::GetNamespaceRequest>,
    ) -> Result<Response<pb::Namespace>, Status> {
        let request = request.into_inner();

        let namespace_name = NamespaceName::parse(&request.name)
            .context(InvalidResourceNameSnafu {
                resource: "namespace",
            })
            .map_err(cluster_metadata_error_to_status)?;

        let namespace = self
            .inner
            .get_namespace(namespace_name)
            .await
            .map_err(cluster_metadata_error_to_status)?;

        Ok(Response::new(namespace.into()))
    }

    async fn list_namespaces(
        &self,
        request: Request<pb::ListNamespacesRequest>,
    ) -> Result<Response<pb::ListNamespacesResponse>, Status> {
        let request = request.into_inner();
        let request =
            ListNamespacesRequest::try_from(request).map_err(cluster_metadata_error_to_status)?;

        let response = self
            .inner
            .list_namespaces(request)
            .await
            .map_err(cluster_metadata_error_to_status)?;

        Ok(Response::new(response.into()))
    }

    async fn delete_namespace(
        &self,
        request: Request<pb::DeleteNamespaceRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();

        let namespace_name = NamespaceName::parse(&request.name)
            .context(InvalidResourceNameSnafu {
                resource: "namespace",
            })
            .map_err(cluster_metadata_error_to_status)?;

        self.inner
            .delete_namespace(namespace_name)
            .await
            .map_err(cluster_metadata_error_to_status)?;

        Ok(Response::new(()))
    }

    async fn create_topic(
        &self,
        request: Request<pb::CreateTopicRequest>,
    ) -> Result<Response<pb::Topic>, Status> {
        let request = request.into_inner();

        let namespace_name = NamespaceName::parse(&request.parent)
            .context(InvalidResourceNameSnafu {
                resource: "namespace",
            })
            .map_err(cluster_metadata_error_to_status)?;

        let topic_name = TopicName::new(request.topic_id, namespace_name)
            .context(InvalidResourceNameSnafu { resource: "topic" })
            .map_err(cluster_metadata_error_to_status)?;

        let options = TopicOptions::try_from(request.topic.unwrap_or_default())
            .map_err(cluster_metadata_error_to_status)?;

        let topic = self
            .inner
            .create_topic(topic_name, options)
            .await
            .map_err(cluster_metadata_error_to_status)?;

        Ok(Response::new(topic.into()))
    }

    async fn get_topic(
        &self,
        request: Request<pb::GetTopicRequest>,
    ) -> Result<Response<pb::Topic>, Status> {
        let request = request.into_inner();

        let topic_name = TopicName::parse(&request.name)
            .context(InvalidResourceNameSnafu { resource: "topic" })
            .map_err(cluster_metadata_error_to_status)?;

        let topic = self
            .inner
            .get_topic(topic_name)
            .await
            .map_err(cluster_metadata_error_to_status)?;

        Ok(Response::new(topic.into()))
    }

    async fn list_topics(
        &self,
        request: Request<pb::ListTopicsRequest>,
    ) -> Result<Response<pb::ListTopicsResponse>, Status> {
        let request = request.into_inner();

        let request =
            ListTopicsRequest::try_from(request).map_err(cluster_metadata_error_to_status)?;

        let response = self
            .inner
            .list_topics(request)
            .await
            .map_err(cluster_metadata_error_to_status)?;

        Ok(Response::new(response.into()))
    }

    async fn delete_topic(
        &self,
        request: Request<pb::DeleteTopicRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();

        let topic_name = TopicName::parse(&request.name)
            .context(InvalidResourceNameSnafu { resource: "topic" })
            .map_err(cluster_metadata_error_to_status)?;

        self.inner
            .delete_topic(topic_name, request.force)
            .await
            .map_err(cluster_metadata_error_to_status)?;

        Ok(Response::new(()))
    }
}

fn cluster_metadata_error_to_status(error: ClusterMetadataError) -> Status {
    match error {
        ClusterMetadataError::NotFound { resource, message } => {
            Status::not_found(format!("{resource} not found: {message}"))
        }
        ClusterMetadataError::AlreadyExists { resource, message } => {
            Status::already_exists(format!("{resource} already exists: {message}"))
        }
        ClusterMetadataError::InvalidArgument { resource, message } => {
            Status::invalid_argument(format!("invalid {resource}: {message}"))
        }
        ClusterMetadataError::InvalidResourceName { resource, source } => {
            resource_error_to_status(resource, source)
        }
        ClusterMetadataError::Internal { message } => {
            Status::internal(format!("internal error: {message}"))
        }
    }
}
