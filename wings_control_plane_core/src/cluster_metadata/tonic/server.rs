use std::sync::Arc;

use snafu::ResultExt;
use tonic::{Request, Response, Status, async_trait};
use wings_resources::{
    DataLakeName, NamespaceName, NamespaceOptions, ObjectStoreConfiguration, ObjectStoreName,
    TenantName, TopicName, TopicOptions,
};

use crate::{
    cluster_metadata::{
        ClusterMetadata, ClusterMetadataError, ListDataLakesRequest, ListNamespacesRequest,
        ListObjectStoresRequest, ListTenantsRequest, ListTopicsRequest, Result,
        error::InvalidResourceNameSnafu,
    },
    pb::{
        self,
        cluster_metadata_service_server::{
            ClusterMetadataService as TonicService, ClusterMetadataServiceServer as TonicServer,
        },
    },
    status::resource_error_to_status,
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
            .map_err(cluster_metadata_error_to_status)?
            .try_into()
            .map_err(cluster_metadata_error_to_status)?;

        Ok(Response::new(topic))
    }

    async fn get_topic(
        &self,
        request: Request<pb::GetTopicRequest>,
    ) -> Result<Response<pb::Topic>, Status> {
        let request = request.into_inner();

        let topic_name = TopicName::parse(&request.name)
            .context(InvalidResourceNameSnafu { resource: "topic" })
            .map_err(cluster_metadata_error_to_status)?;

        let view = request.view().into();

        let topic = self
            .inner
            .get_topic(topic_name, view)
            .await
            .map_err(cluster_metadata_error_to_status)?
            .try_into()
            .map_err(cluster_metadata_error_to_status)?;

        Ok(Response::new(topic))
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
            .map_err(cluster_metadata_error_to_status)?
            .try_into()
            .map_err(cluster_metadata_error_to_status)?;

        Ok(Response::new(response))
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

    async fn create_object_store(
        &self,
        request: Request<pb::CreateObjectStoreRequest>,
    ) -> Result<Response<pb::ObjectStore>, Status> {
        let request = request.into_inner();

        let tenant_name = TenantName::parse(&request.parent)
            .context(InvalidResourceNameSnafu { resource: "tenant" })
            .map_err(cluster_metadata_error_to_status)?;

        let object_store_name = ObjectStoreName::new(request.object_store_id, tenant_name)
            .context(InvalidResourceNameSnafu {
                resource: "object store",
            })
            .map_err(cluster_metadata_error_to_status)?;

        let object_store_config =
            ObjectStoreConfiguration::try_from(request.object_store.unwrap_or_default())
                .map_err(cluster_metadata_error_to_status)?;

        let object_store = self
            .inner
            .create_object_store(object_store_name, object_store_config)
            .await
            .map_err(cluster_metadata_error_to_status)?;

        Ok(Response::new(object_store.into()))
    }

    async fn get_object_store(
        &self,
        request: Request<pb::GetObjectStoreRequest>,
    ) -> Result<Response<pb::ObjectStore>, Status> {
        let request = request.into_inner();

        let object_store_name = ObjectStoreName::parse(&request.name)
            .context(InvalidResourceNameSnafu {
                resource: "object store",
            })
            .map_err(cluster_metadata_error_to_status)?;

        let object_store = self
            .inner
            .get_object_store(object_store_name)
            .await
            .map_err(cluster_metadata_error_to_status)?;

        Ok(Response::new(object_store.into()))
    }

    async fn list_object_stores(
        &self,
        request: Request<pb::ListObjectStoresRequest>,
    ) -> Result<Response<pb::ListObjectStoresResponse>, Status> {
        let request = request.into_inner();
        let request =
            ListObjectStoresRequest::try_from(request).map_err(cluster_metadata_error_to_status)?;

        let response = self
            .inner
            .list_object_stores(request)
            .await
            .map_err(cluster_metadata_error_to_status)?;

        Ok(Response::new(response.into()))
    }

    async fn delete_object_store(
        &self,
        request: Request<pb::DeleteObjectStoreRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();

        let object_store_name = ObjectStoreName::parse(&request.name)
            .context(InvalidResourceNameSnafu {
                resource: "object store",
            })
            .map_err(cluster_metadata_error_to_status)?;

        self.inner
            .delete_object_store(object_store_name)
            .await
            .map_err(cluster_metadata_error_to_status)?;

        Ok(Response::new(()))
    }

    async fn create_data_lake(
        &self,
        request: Request<pb::CreateDataLakeRequest>,
    ) -> Result<Response<pb::DataLake>, Status> {
        let request = request.into_inner();

        let (data_lake_name, data_lake_config) = request
            .try_into()
            .map_err(cluster_metadata_error_to_status)?;

        let data_lake = self
            .inner
            .create_data_lake(data_lake_name, data_lake_config)
            .await
            .map_err(cluster_metadata_error_to_status)?;

        Ok(Response::new(data_lake.into()))
    }

    async fn get_data_lake(
        &self,
        request: Request<pb::GetDataLakeRequest>,
    ) -> Result<Response<pb::DataLake>, Status> {
        let request = request.into_inner();

        let data_lake_name =
            DataLakeName::try_from(request).map_err(cluster_metadata_error_to_status)?;

        let data_lake = self
            .inner
            .get_data_lake(data_lake_name)
            .await
            .map_err(cluster_metadata_error_to_status)?;

        Ok(Response::new(data_lake.into()))
    }

    async fn list_data_lakes(
        &self,
        request: Request<pb::ListDataLakesRequest>,
    ) -> Result<Response<pb::ListDataLakesResponse>, Status> {
        let request = request.into_inner();
        let request =
            ListDataLakesRequest::try_from(request).map_err(cluster_metadata_error_to_status)?;

        let response = self
            .inner
            .list_data_lakes(request)
            .await
            .map_err(cluster_metadata_error_to_status)?;

        Ok(Response::new(response.into()))
    }

    async fn delete_data_lake(
        &self,
        request: Request<pb::DeleteDataLakeRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();

        let data_lake_name =
            DataLakeName::try_from(request).map_err(cluster_metadata_error_to_status)?;

        self.inner
            .delete_data_lake(data_lake_name)
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
        ClusterMetadataError::Schema { source } => {
            Status::invalid_argument(format!("invalid schema: {source}"))
        }
        ClusterMetadataError::Internal { message } => {
            Status::internal(format!("internal error: {message}"))
        }
    }
}
