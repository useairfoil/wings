use async_trait::async_trait;
use wings_control_plane_core::cluster_metadata::{
    ClusterMetadata, ListDataLakesRequest, ListDataLakesResponse, ListNamespacesRequest,
    ListNamespacesResponse, ListObjectStoresRequest, ListObjectStoresResponse, ListTenantsRequest,
    ListTenantsResponse, ListTablesRequest, ListTablesResponse, Result, TableView,
};
use wings_resources::{
    DataLake, DataLakeConfiguration, DataLakeName, Namespace, NamespaceName, NamespaceOptions,
    ObjectStore, ObjectStoreConfiguration, ObjectStoreName, Tenant, TenantName, Table, TableName,
    TableOptions,
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

    async fn create_table(&self, name: TableName, options: TableOptions) -> Result<Table> {
        self.db
            .create_table(name, options)
            .await
            .map_err(Into::into)
    }

    async fn get_table(&self, name: TableName, view: TableView) -> Result<Table> {
        self.db.get_table(name, view).await.map_err(Into::into)
    }

    async fn list_tables(&self, request: ListTablesRequest) -> Result<ListTablesResponse> {
        self.db.list_tables(request).await.map_err(Into::into)
    }

    async fn delete_table(&self, name: TableName, force: bool) -> Result<()> {
        self.db.delete_table(name, force).await.map_err(Into::into)
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
