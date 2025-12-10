use std::marker::Send;

use async_trait::async_trait;
use bytes::Bytes;
use http_body::Body;

use crate::{
    cluster_metadata::{
        ClusterMetadata, ClusterMetadataError, ListDataLakesRequest, ListDataLakesResponse,
        ListNamespacesRequest, ListNamespacesResponse, ListObjectStoresRequest,
        ListObjectStoresResponse, ListTenantsRequest, ListTenantsResponse, ListTopicsRequest,
        ListTopicsResponse, Result,
    },
    resources::{
        DataLake, DataLakeConfiguration, DataLakeName, Namespace, NamespaceName, NamespaceOptions,
        ObjectStore, ObjectStoreConfiguration, ObjectStoreName, Tenant, TenantName, Topic,
        TopicName, TopicOptions,
    },
};

use super::pb::{
    self, cluster_metadata_service_client::ClusterMetadataServiceClient as TonicClient,
};

type StdError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug, Clone)]
pub struct ClusterMetadataClient<T> {
    client: TonicClient<T>,
}

impl<T> ClusterMetadataClient<T>
where
    T: tonic::client::GrpcService<tonic::body::Body> + Clone,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
{
    /// Create a new remote cluster metadata service with the given transport.
    pub fn new(inner: T) -> Self {
        Self::new_with_client(TonicClient::new(inner))
    }

    /// Create a new remote cluster metadata service with the given client.
    pub fn new_with_client(client: TonicClient<T>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl<T> ClusterMetadata for ClusterMetadataClient<T>
where
    T: tonic::client::GrpcService<tonic::body::Body> + Send + Sync + Clone,
    <T as tonic::client::GrpcService<tonic::body::Body>>::Future: Send,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
{
    async fn create_tenant(&self, name: TenantName) -> Result<Tenant> {
        let request = pb::CreateTenantRequest {
            tenant_id: name.id().to_string(),
            tenant: None,
        };

        self.client
            .clone()
            .create_tenant(request)
            .await
            .map_err(|status| status_to_cluster_metadata_error("tenant", status))?
            .into_inner()
            .try_into()
    }

    async fn get_tenant(&self, name: TenantName) -> Result<Tenant> {
        let request = pb::GetTenantRequest {
            name: name.to_string(),
        };

        self.client
            .clone()
            .get_tenant(request)
            .await
            .map_err(|status| status_to_cluster_metadata_error("tenant", status))?
            .into_inner()
            .try_into()
    }

    async fn list_tenants(&self, request: ListTenantsRequest) -> Result<ListTenantsResponse> {
        let request = pb::ListTenantsRequest::from(request);

        self.client
            .clone()
            .list_tenants(request)
            .await
            .map_err(|status| status_to_cluster_metadata_error("tenant", status))?
            .into_inner()
            .try_into()
    }

    async fn delete_tenant(&self, name: TenantName) -> Result<()> {
        let request = pb::DeleteTenantRequest {
            name: name.to_string(),
        };

        self.client
            .clone()
            .delete_tenant(request)
            .await
            .map_err(|status| status_to_cluster_metadata_error("tenant", status))?;

        Ok(())
    }

    async fn create_namespace(
        &self,
        name: NamespaceName,
        options: NamespaceOptions,
    ) -> Result<Namespace> {
        let request = pb::CreateNamespaceRequest {
            parent: name.parent().to_string(),
            namespace_id: name.id().to_string(),
            namespace: pb::Namespace::from(options).into(),
        };

        self.client
            .clone()
            .create_namespace(request)
            .await
            .map_err(|status| status_to_cluster_metadata_error("namespace", status))?
            .into_inner()
            .try_into()
    }

    async fn get_namespace(&self, name: NamespaceName) -> Result<Namespace> {
        let request = pb::GetNamespaceRequest {
            name: name.to_string(),
        };

        self.client
            .clone()
            .get_namespace(request)
            .await
            .map_err(|status| status_to_cluster_metadata_error("namespace", status))?
            .into_inner()
            .try_into()
    }

    async fn list_namespaces(
        &self,
        request: ListNamespacesRequest,
    ) -> Result<ListNamespacesResponse> {
        let request = pb::ListNamespacesRequest::from(request);

        self.client
            .clone()
            .list_namespaces(request)
            .await
            .map_err(|status| status_to_cluster_metadata_error("namespace", status))?
            .into_inner()
            .try_into()
    }

    async fn delete_namespace(&self, name: NamespaceName) -> Result<()> {
        let request = pb::DeleteNamespaceRequest {
            name: name.to_string(),
        };

        self.client
            .clone()
            .delete_namespace(request)
            .await
            .map_err(|status| status_to_cluster_metadata_error("tenant", status))?;

        Ok(())
    }

    async fn create_topic(&self, name: TopicName, options: TopicOptions) -> Result<Topic> {
        let request = pb::CreateTopicRequest {
            parent: name.parent().to_string(),
            topic_id: name.id().to_string(),
            topic: pb::Topic::from(options).into(),
        };

        self.client
            .clone()
            .create_topic(request)
            .await
            .map_err(|status| status_to_cluster_metadata_error("topic", status))?
            .into_inner()
            .try_into()
    }

    async fn get_topic(&self, name: TopicName) -> Result<Topic> {
        let request = pb::GetTopicRequest {
            name: name.to_string(),
        };

        self.client
            .clone()
            .get_topic(request)
            .await
            .map_err(|status| status_to_cluster_metadata_error("topic", status))?
            .into_inner()
            .try_into()
    }

    async fn list_topics(&self, request: ListTopicsRequest) -> Result<ListTopicsResponse> {
        let request = pb::ListTopicsRequest::from(request);

        self.client
            .clone()
            .list_topics(request)
            .await
            .map_err(|status| status_to_cluster_metadata_error("topic", status))?
            .into_inner()
            .try_into()
    }

    async fn delete_topic(&self, name: TopicName, force: bool) -> Result<()> {
        let request = pb::DeleteTopicRequest {
            name: name.to_string(),
            force,
        };

        self.client
            .clone()
            .delete_topic(request)
            .await
            .map_err(|status| status_to_cluster_metadata_error("topic", status))?;

        Ok(())
    }

    async fn create_object_store(
        &self,
        name: ObjectStoreName,
        object_store: ObjectStoreConfiguration,
    ) -> Result<ObjectStore> {
        let request = pb::CreateObjectStoreRequest {
            parent: name.parent().to_string(),
            object_store_id: name.id().to_string(),
            object_store: Some(object_store.into()),
        };

        self.client
            .clone()
            .create_object_store(request)
            .await
            .map_err(|status| status_to_cluster_metadata_error("object store", status))?
            .into_inner()
            .try_into()
    }

    async fn get_object_store(&self, name: ObjectStoreName) -> Result<ObjectStore> {
        let request = pb::GetObjectStoreRequest {
            name: name.to_string(),
        };

        self.client
            .clone()
            .get_object_store(request)
            .await
            .map_err(|status| status_to_cluster_metadata_error("object store", status))?
            .into_inner()
            .try_into()
    }

    async fn list_object_stores(
        &self,
        request: ListObjectStoresRequest,
    ) -> Result<ListObjectStoresResponse> {
        let request = pb::ListObjectStoresRequest::from(request);

        self.client
            .clone()
            .list_object_stores(request)
            .await
            .map_err(|status| status_to_cluster_metadata_error("object store", status))?
            .into_inner()
            .try_into()
    }

    async fn delete_object_store(&self, name: ObjectStoreName) -> Result<()> {
        let request = pb::DeleteObjectStoreRequest {
            name: name.to_string(),
        };

        self.client
            .clone()
            .delete_object_store(request)
            .await
            .map_err(|status| status_to_cluster_metadata_error("object store", status))?;

        Ok(())
    }

    async fn create_data_lake(
        &self,
        name: DataLakeName,
        configuration: DataLakeConfiguration,
    ) -> Result<DataLake> {
        let request = pb::CreateDataLakeRequest {
            parent: name.parent().to_string(),
            data_lake_id: name.id().to_string(),
            data_lake: Some(configuration.into()),
        };

        self.client
            .clone()
            .create_data_lake(request)
            .await
            .map_err(|status| status_to_cluster_metadata_error("data lake", status))?
            .into_inner()
            .try_into()
    }

    async fn get_data_lake(&self, name: DataLakeName) -> Result<DataLake> {
        let request = pb::GetDataLakeRequest {
            name: name.to_string(),
        };

        self.client
            .clone()
            .get_data_lake(request)
            .await
            .map_err(|status| status_to_cluster_metadata_error("data lake", status))?
            .into_inner()
            .try_into()
    }

    async fn list_data_lakes(
        &self,
        request: ListDataLakesRequest,
    ) -> Result<ListDataLakesResponse> {
        let request = pb::ListDataLakesRequest::from(request);

        self.client
            .clone()
            .list_data_lakes(request)
            .await
            .map_err(|status| status_to_cluster_metadata_error("data lake", status))?
            .into_inner()
            .try_into()
    }

    async fn delete_data_lake(&self, name: DataLakeName) -> Result<()> {
        let request = pb::DeleteDataLakeRequest {
            name: name.to_string(),
        };

        self.client
            .clone()
            .delete_data_lake(request)
            .await
            .map_err(|status| status_to_cluster_metadata_error("data lake", status))?;

        Ok(())
    }
}

fn status_to_cluster_metadata_error(
    resource: &'static str,
    status: tonic::Status,
) -> ClusterMetadataError {
    use tonic::Code;

    match status.code() {
        Code::NotFound => ClusterMetadataError::NotFound {
            resource,
            message: status.message().to_string(),
        },
        Code::AlreadyExists => ClusterMetadataError::AlreadyExists {
            resource,
            message: status.message().to_string(),
        },
        Code::InvalidArgument => ClusterMetadataError::InvalidArgument {
            resource,
            message: status.message().to_string(),
        },
        _ => ClusterMetadataError::Internal {
            message: format!("unknown error from remote service: {}", status.message()),
        },
    }
}
