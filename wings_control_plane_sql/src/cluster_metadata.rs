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

    async fn get_tenant(&self, _name: TenantName) -> Result<Tenant> {
        todo!()
    }

    async fn list_tenants(&self, _request: ListTenantsRequest) -> Result<ListTenantsResponse> {
        todo!()
    }

    async fn delete_tenant(&self, _name: TenantName) -> Result<()> {
        todo!()
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

    async fn get_namespace(&self, _name: NamespaceName) -> Result<Namespace> {
        todo!()
    }

    async fn list_namespaces(
        &self,
        _request: ListNamespacesRequest,
    ) -> Result<ListNamespacesResponse> {
        todo!()
    }

    async fn delete_namespace(&self, _name: NamespaceName) -> Result<()> {
        todo!()
    }

    async fn create_topic(&self, _name: TopicName, _options: TopicOptions) -> Result<Topic> {
        todo!()
    }

    async fn get_topic(&self, _name: TopicName, _view: TopicView) -> Result<Topic> {
        todo!()
    }

    async fn list_topics(&self, _request: ListTopicsRequest) -> Result<ListTopicsResponse> {
        todo!()
    }

    async fn delete_topic(&self, _name: TopicName, _force: bool) -> Result<()> {
        todo!()
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

    async fn get_object_store(&self, _name: ObjectStoreName) -> Result<ObjectStore> {
        todo!()
    }

    async fn list_object_stores(
        &self,
        _request: ListObjectStoresRequest,
    ) -> Result<ListObjectStoresResponse> {
        todo!()
    }

    async fn delete_object_store(&self, _name: ObjectStoreName) -> Result<()> {
        todo!()
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

    async fn get_data_lake(&self, _name: DataLakeName) -> Result<DataLake> {
        todo!()
    }

    async fn list_data_lakes(
        &self,
        _request: ListDataLakesRequest,
    ) -> Result<ListDataLakesResponse> {
        todo!()
    }

    async fn delete_data_lake(&self, _name: DataLakeName) -> Result<()> {
        todo!()
    }
}
