//! Cluster metadata.

pub mod cache;
mod error;
mod helpers;
mod memory;
mod metrics;
pub mod stream;
pub mod tonic;

use async_trait::async_trait;

use crate::resources::{
    Namespace, NamespaceName, NamespaceOptions, Tenant, TenantName, Topic, TopicName, TopicOptions,
};

pub use self::error::{ClusterMetadataError, Result};
pub use self::helpers::{CollectNamespaceTopicsOptions, collect_namespace_topics};
pub use self::memory::InMemoryClusterMetadata;

/// The cluster metadata trait provides methods for managing tenants, namespaces, and topics.
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
    /// The request fails if the namespace has any topic.
    async fn delete_namespace(&self, name: NamespaceName) -> Result<()>;

    // Topic operations

    /// Create a new topic belonging to a namespace.
    async fn create_topic(&self, name: TopicName, options: TopicOptions) -> Result<Topic>;

    /// Return the specified topic.
    async fn get_topic(&self, name: TopicName) -> Result<Topic>;

    /// List all topics belonging to a namespace.
    async fn list_topics(&self, request: ListTopicsRequest) -> Result<ListTopicsResponse>;

    /// Delete a topic.
    ///
    /// This operation may take a long time to complete as it involves deleting
    /// data from object storage.
    async fn delete_topic(&self, name: TopicName, force: bool) -> Result<()>;
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

/// Request to list topics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListTopicsRequest {
    /// The parent namespace.
    pub parent: NamespaceName,
    /// The number of topics to return.
    /// Default: 100, Maximum: 1000.
    pub page_size: Option<usize>,
    /// The continuation token.
    pub page_token: Option<String>,
}

impl ListTopicsRequest {
    /// Create a new request for the given parent namespace.
    pub fn new(parent: NamespaceName) -> Self {
        Self {
            parent,
            page_size: Some(100),
            page_token: None,
        }
    }
}

/// Response from listing topics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListTopicsResponse {
    /// The topics.
    pub topics: Vec<Topic>,
    /// The continuation token.
    pub next_page_token: Option<String>,
}
