//! Cluster metadata.

pub mod cache;
pub(crate) mod error;
mod helpers;
mod metrics;
pub mod stream;
pub mod tonic;

use async_trait::async_trait;
use wings_resources::{
    DataLake, DataLakeConfiguration, DataLakeName, Namespace, NamespaceName, NamespaceOptions,
    ObjectStore, ObjectStoreConfiguration, ObjectStoreName, Tenant, TenantName, Table, TableName,
    TableOptions,
};

pub use self::{
    error::{ClusterMetadataError, Result},
    helpers::{CollectNamespaceTablesOptions, collect_namespace_tables},
    metrics::ClusterMetadataMetrics,
};

/// The cluster metadata trait provides methods for managing tenants, namespaces, and tables.
#[async_trait]
pub trait ClusterMetadata: Send + Sync {
    // Tenant operations

    /// Create a new tenant.
    async fn create_tenant(&self, name: TenantName) -> Result<Tenant>;

    /// Return the specified tenant.
    async fn get_tenant(&self, name: TenantName) -> Result<Tenant>;

    /// List all tenants.
    async fn list_tenants(&self, request: ListTenantsRequest) -> Result<ListTenantsResponse>;

    /// Delete a tenant.
    ///
    /// The request fails if the tenant has any namespace.
    async fn delete_tenant(&self, name: TenantName) -> Result<()>;

    // Namespace operations

    /// Create a new namespace belonging to a tenant.
    async fn create_namespace(
        &self,
        name: NamespaceName,
        options: NamespaceOptions,
    ) -> Result<Namespace>;

    /// Return the specified namespace.
    async fn get_namespace(&self, name: NamespaceName) -> Result<Namespace>;

    /// List all namespaces belonging to a tenant.
    async fn list_namespaces(
        &self,
        request: ListNamespacesRequest,
    ) -> Result<ListNamespacesResponse>;

    /// Delete a namespace.
    ///
    /// The request fails if the namespace has any table.
    async fn delete_namespace(&self, name: NamespaceName) -> Result<()>;

    // Table operations

    /// Create a new table belonging to a namespace.
    async fn create_table(&self, name: TableName, options: TableOptions) -> Result<Table>;

    /// Return the specified table.
    async fn get_table(&self, name: TableName, view: TableView) -> Result<Table>;

    /// List all tables belonging to a namespace.
    async fn list_tables(&self, request: ListTablesRequest) -> Result<ListTablesResponse>;

    /// Delete a table.
    ///
    /// This operation may take a long time to complete as it involves deleting
    /// data from object storage.
    async fn delete_table(&self, name: TableName, force: bool) -> Result<()>;

    // Object store operations

    /// Create a new object store belonging to a tenant.
    async fn create_object_store(
        &self,
        name: ObjectStoreName,
        configuration: ObjectStoreConfiguration,
    ) -> Result<ObjectStore>;

    /// Return the specified object store.
    async fn get_object_store(&self, name: ObjectStoreName) -> Result<ObjectStore>;

    /// List all object stores belonging to a tenant.
    async fn list_object_stores(
        &self,
        request: ListObjectStoresRequest,
    ) -> Result<ListObjectStoresResponse>;

    /// Delete an object store.
    async fn delete_object_store(&self, name: ObjectStoreName) -> Result<()>;

    // Data lake operations

    /// Create a new data lake belonging to a tenant.
    async fn create_data_lake(
        &self,
        name: DataLakeName,
        configuration: DataLakeConfiguration,
    ) -> Result<DataLake>;

    /// Return the specified data lake.
    async fn get_data_lake(&self, name: DataLakeName) -> Result<DataLake>;

    /// List all data lakes belonging to a tenant.
    async fn list_data_lakes(&self, request: ListDataLakesRequest)
    -> Result<ListDataLakesResponse>;

    /// Delete a data lake.
    async fn delete_data_lake(&self, name: DataLakeName) -> Result<()>;
}

#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub enum TableView {
    // Only return the basic table information.
    #[default]
    Basic,
    // Include the table status.
    Full,
}

/// Request to list tenants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListTenantsRequest {
    /// The number of tenants to return.
    /// Default: 100, Maximum: 1000.
    pub page_size: Option<i32>,
    /// The continuation token.
    pub page_token: Option<String>,
}

impl Default for ListTenantsRequest {
    fn default() -> Self {
        Self {
            page_size: Some(100),
            page_token: None,
        }
    }
}

/// Response from listing tenants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListTenantsResponse {
    /// The tenants.
    pub tenants: Vec<Tenant>,
    /// The continuation token.
    pub next_page_token: Option<String>,
}

/// Request to list namespaces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListNamespacesRequest {
    /// The parent tenant.
    pub parent: TenantName,
    /// The number of namespaces to return.
    /// Default: 100, Maximum: 1000.
    pub page_size: Option<i32>,
    /// The continuation token.
    pub page_token: Option<String>,
}

impl ListNamespacesRequest {
    /// Create a new request for the given parent tenant.
    pub fn new(parent: TenantName) -> Self {
        Self {
            parent,
            page_size: Some(100),
            page_token: None,
        }
    }
}

/// Response from listing namespaces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListNamespacesResponse {
    /// The namespaces.
    pub namespaces: Vec<Namespace>,
    /// The continuation token.
    pub next_page_token: Option<String>,
}

/// Request to list tables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListTablesRequest {
    /// The parent namespace.
    pub parent: NamespaceName,
    /// The number of tables to return.
    /// Default: 100, Maximum: 1000.
    pub page_size: Option<usize>,
    /// The continuation token.
    pub page_token: Option<String>,
}

impl ListTablesRequest {
    /// Create a new request for the given parent namespace.
    pub fn new(parent: NamespaceName) -> Self {
        Self {
            parent,
            page_size: Some(100),
            page_token: None,
        }
    }
}

/// Response from listing tables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListTablesResponse {
    /// The tables.
    pub tables: Vec<Table>,
    /// The continuation token.
    pub next_page_token: Option<String>,
}

/// Request to list object stores.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListObjectStoresRequest {
    /// The parent tenant.
    pub parent: TenantName,
    /// The number of object stores to return.
    /// Default: 100, Maximum: 1000.
    pub page_size: Option<i32>,
    /// The continuation token.
    pub page_token: Option<String>,
}

impl ListObjectStoresRequest {
    /// Create a new request for the given parent tenant.
    pub fn new(parent: TenantName) -> Self {
        Self {
            parent,
            page_size: Some(100),
            page_token: None,
        }
    }
}

/// Response from listing object stores.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListObjectStoresResponse {
    /// The object stores.
    pub object_stores: Vec<ObjectStore>,
    /// The continuation token.
    pub next_page_token: Option<String>,
}

/// Request to list data lakes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListDataLakesRequest {
    /// The parent tenant.
    pub parent: TenantName,
    /// The number of data lakes to return.
    /// Default: 100, Maximum: 1000.
    pub page_size: Option<i32>,
    /// The continuation token.
    pub page_token: Option<String>,
}

impl ListDataLakesRequest {
    /// Create a new request for the given parent tenant.
    pub fn new(parent: TenantName) -> Self {
        Self {
            parent,
            page_size: Some(100),
            page_token: None,
        }
    }
}

/// Response from listing data lakes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListDataLakesResponse {
    /// The data lakes.
    pub data_lakes: Vec<DataLake>,
    /// The continuation token.
    pub next_page_token: Option<String>,
}

/// Generic implementation for references
#[async_trait]
impl<T: ClusterMetadata + ?Sized> ClusterMetadata for &T {
    async fn create_tenant(&self, name: TenantName) -> Result<Tenant> {
        (**self).create_tenant(name).await
    }

    async fn get_tenant(&self, name: TenantName) -> Result<Tenant> {
        (**self).get_tenant(name).await
    }

    async fn list_tenants(&self, request: ListTenantsRequest) -> Result<ListTenantsResponse> {
        (**self).list_tenants(request).await
    }

    async fn delete_tenant(&self, name: TenantName) -> Result<()> {
        (**self).delete_tenant(name).await
    }

    async fn create_namespace(
        &self,
        name: NamespaceName,
        options: NamespaceOptions,
    ) -> Result<Namespace> {
        (**self).create_namespace(name, options).await
    }

    async fn get_namespace(&self, name: NamespaceName) -> Result<Namespace> {
        (**self).get_namespace(name).await
    }

    async fn list_namespaces(
        &self,
        request: ListNamespacesRequest,
    ) -> Result<ListNamespacesResponse> {
        (**self).list_namespaces(request).await
    }

    async fn delete_namespace(&self, name: NamespaceName) -> Result<()> {
        (**self).delete_namespace(name).await
    }

    async fn create_table(&self, name: TableName, options: TableOptions) -> Result<Table> {
        (**self).create_table(name, options).await
    }

    async fn get_table(&self, name: TableName, view: TableView) -> Result<Table> {
        (**self).get_table(name, view).await
    }

    async fn list_tables(&self, request: ListTablesRequest) -> Result<ListTablesResponse> {
        (**self).list_tables(request).await
    }

    async fn delete_table(&self, name: TableName, force: bool) -> Result<()> {
        (**self).delete_table(name, force).await
    }

    async fn create_object_store(
        &self,
        name: ObjectStoreName,
        configuration: ObjectStoreConfiguration,
    ) -> Result<ObjectStore> {
        (**self).create_object_store(name, configuration).await
    }

    async fn get_object_store(&self, name: ObjectStoreName) -> Result<ObjectStore> {
        (**self).get_object_store(name).await
    }

    async fn list_object_stores(
        &self,
        request: ListObjectStoresRequest,
    ) -> Result<ListObjectStoresResponse> {
        (**self).list_object_stores(request).await
    }

    async fn delete_object_store(&self, name: ObjectStoreName) -> Result<()> {
        (**self).delete_object_store(name).await
    }

    async fn create_data_lake(
        &self,
        name: DataLakeName,
        configuration: DataLakeConfiguration,
    ) -> Result<DataLake> {
        (**self).create_data_lake(name, configuration).await
    }

    async fn get_data_lake(&self, name: DataLakeName) -> Result<DataLake> {
        (**self).get_data_lake(name).await
    }

    async fn list_data_lakes(
        &self,
        request: ListDataLakesRequest,
    ) -> Result<ListDataLakesResponse> {
        (**self).list_data_lakes(request).await
    }

    async fn delete_data_lake(&self, name: DataLakeName) -> Result<()> {
        (**self).delete_data_lake(name).await
    }
}
