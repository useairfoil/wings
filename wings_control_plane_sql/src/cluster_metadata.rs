use async_trait::async_trait;
use wings_control_plane_core::cluster_metadata::{
    ClusterMetadata, ListDataLakesRequest, ListDataLakesResponse, ListNamespacesRequest,
    ListNamespacesResponse, ListObjectStoresRequest, ListObjectStoresResponse, ListTenantsRequest,
    ListTenantsResponse, ListTopicsRequest, ListTopicsResponse, Result, TopicView,
};
use wings_resources::{
    DataLake, DataLakeConfiguration, DataLakeName, Namespace, NamespaceName, NamespaceOptions,
    ObjectStore, ObjectStoreConfiguration, ObjectStoreName, Tenant, TenantName, Topic, TopicName,
    TopicOptions,
};

use crate::SqlControlPlane;

#[async_trait]
impl ClusterMetadata for SqlControlPlane {
    async fn create_tenant(&self, name: TenantName) -> Result<Tenant> {
        self.db.create_tenant(name).await.map_err(Into::into)
    }

    async fn get_tenant(&self, name: TenantName) -> Result<Tenant> {
        self.db.get_tenant(name).await.map_err(Into::into)
    }

    async fn list_tenants(&self, request: ListTenantsRequest) -> Result<ListTenantsResponse> {
        self.db.list_tenants(request).await.map_err(Into::into)
    }

    async fn delete_tenant(&self, name: TenantName) -> Result<()> {
        self.db.delete_tenant(name).await.map_err(Into::into)
    }

    async fn create_namespace(
        &self,
        name: NamespaceName,
        options: NamespaceOptions,
    ) -> Result<Namespace> {
        self.db
            .create_namespace(name, options)
            .await
            .map_err(Into::into)
    }

    async fn get_namespace(&self, name: NamespaceName) -> Result<Namespace> {
        self.db.get_namespace(name).await.map_err(Into::into)
    }

    async fn list_namespaces(
        &self,
        request: ListNamespacesRequest,
    ) -> Result<ListNamespacesResponse> {
        self.db.list_namespaces(request).await.map_err(Into::into)
    }

    async fn delete_namespace(&self, name: NamespaceName) -> Result<()> {
        self.db.delete_namespace(name).await.map_err(Into::into)
    }

    async fn create_topic(&self, name: TopicName, options: TopicOptions) -> Result<Topic> {
        self.db
            .create_topic(name, options)
            .await
            .map_err(Into::into)
    }

    async fn get_topic(&self, name: TopicName, view: TopicView) -> Result<Topic> {
        self.db.get_topic(name, view).await.map_err(Into::into)
    }

    async fn list_topics(&self, request: ListTopicsRequest) -> Result<ListTopicsResponse> {
        self.db.list_topics(request).await.map_err(Into::into)
    }

    async fn delete_topic(&self, name: TopicName, force: bool) -> Result<()> {
        self.db.delete_topic(name, force).await.map_err(Into::into)
    }

    async fn create_object_store(
        &self,
        name: ObjectStoreName,
        configuration: ObjectStoreConfiguration,
    ) -> Result<ObjectStore> {
        self.db
            .create_object_store(name, configuration)
            .await
            .map_err(Into::into)
    }

    async fn get_object_store(&self, name: ObjectStoreName) -> Result<ObjectStore> {
        self.db.get_object_store(name).await.map_err(Into::into)
    }

    async fn list_object_stores(
        &self,
        request: ListObjectStoresRequest,
    ) -> Result<ListObjectStoresResponse> {
        self.db
            .list_object_stores(request)
            .await
            .map_err(Into::into)
    }

    async fn delete_object_store(&self, name: ObjectStoreName) -> Result<()> {
        self.db.delete_object_store(name).await.map_err(Into::into)
    }

    async fn create_data_lake(
        &self,
        name: DataLakeName,
        configuration: DataLakeConfiguration,
    ) -> Result<DataLake> {
        self.db
            .create_data_lake(name, configuration)
            .await
            .map_err(Into::into)
    }

    async fn get_data_lake(&self, name: DataLakeName) -> Result<DataLake> {
        self.db.get_data_lake(name).await.map_err(Into::into)
    }

    async fn list_data_lakes(
        &self,
        request: ListDataLakesRequest,
    ) -> Result<ListDataLakesResponse> {
        self.db.list_data_lakes(request).await.map_err(Into::into)
    }

    async fn delete_data_lake(&self, name: DataLakeName) -> Result<()> {
        self.db.delete_data_lake(name).await.map_err(Into::into)
    }
}
