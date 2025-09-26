//! In-memory implementation of the cluster metadata trait.
//!
//! This implementation stores all data in memory and is suitable for testing
//! and development. It uses a RwLock for thread-safe access.

use std::collections::HashMap;

use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::resources::{
    Namespace, NamespaceName, NamespaceOptions, Tenant, TenantName, Topic, TopicName, TopicOptions,
};

use super::{
    ClusterMetadata, ClusterMetadataError, ListNamespacesRequest, ListNamespacesResponse,
    ListTenantsRequest, ListTenantsResponse, ListTopicsRequest, ListTopicsResponse, Result,
};

#[derive(Debug, Default)]
struct ClusterMetadataStore {
    /// Map of tenant ID to tenant data.
    tenants: HashMap<String, Tenant>,
    /// Map of namespace name to namespace data.
    namespaces: HashMap<String, Namespace>,
    /// Map of topic name to topic data.
    topics: HashMap<String, Topic>,
}

/// In-memory implementation of the cluster metadata service.
///
#[derive(Debug)]
pub struct InMemoryClusterMetadata {
    store: RwLock<ClusterMetadataStore>,
}

impl InMemoryClusterMetadata {
    /// Create a new in-memory cluster metadata service.
    pub fn new() -> Self {
        Self {
            store: RwLock::new(ClusterMetadataStore::default()),
        }
    }
}

impl ClusterMetadataStore {
    fn create_tenant(&mut self, name: TenantName) -> Result<Tenant> {
        let tenant_id = name.id().to_string();

        if self.tenants.contains_key(&tenant_id) {
            return Err(ClusterMetadataError::AlreadyExists {
                resource: "tenant",
                message: tenant_id.clone(),
            });
        }

        let tenant = Tenant::new(name);
        self.tenants.insert(tenant_id, tenant.clone());

        Ok(tenant)
    }

    fn get_tenant(&self, name: TenantName) -> Result<Tenant> {
        let tenant_id = name.id();
        self.tenants
            .get(tenant_id)
            .cloned()
            .ok_or_else(|| ClusterMetadataError::NotFound {
                resource: "tenant",
                message: tenant_id.to_string(),
            })
    }

    fn list_tenants(&self, request: ListTenantsRequest) -> Result<ListTenantsResponse> {
        let page_size = request.page_size.unwrap_or(100).clamp(1, 1000) as usize;
        let page_token = request.page_token.as_deref().unwrap_or("");

        // For simplicity, we'll use the tenant ID as the page token
        // In a real implementation, you'd want a more sophisticated pagination system
        let mut tenant_ids: Vec<_> = self.tenants.keys().collect();
        tenant_ids.sort();

        let start_index = if page_token.is_empty() {
            0
        } else {
            tenant_ids
                .iter()
                .position(|id| *id == page_token)
                .map(|pos| pos + 1)
                .unwrap_or(0)
        };

        let end_index = (start_index + page_size).min(tenant_ids.len());
        let page_tenant_ids = &tenant_ids[start_index..end_index];

        let tenants: Vec<Tenant> = page_tenant_ids
            .iter()
            .filter_map(|id| self.tenants.get(*id).cloned())
            .collect();

        let next_page_token = if end_index < tenant_ids.len() {
            Some(tenant_ids[end_index - 1].clone())
        } else {
            None
        };

        Ok(ListTenantsResponse {
            tenants,
            next_page_token,
        })
    }

    fn delete_tenant(&mut self, name: TenantName) -> Result<()> {
        let tenant_id = name.id();

        if !self.tenants.contains_key(tenant_id) {
            return Err(ClusterMetadataError::NotFound {
                resource: "tenant",
                message: tenant_id.to_string(),
            });
        }

        let has_namespaces = self
            .namespaces
            .values()
            .any(|namespace| namespace.name.parent().id() == tenant_id);

        if has_namespaces {
            return Err(ClusterMetadataError::InvalidArgument {
                resource: "tenant",
                message: format!("{} has namespaces and cannot be deleted", tenant_id),
            });
        }

        self.tenants.remove(tenant_id);

        Ok(())
    }

    fn create_namespace(
        &mut self,
        name: NamespaceName,
        options: NamespaceOptions,
    ) -> Result<Namespace> {
        let namespace_key = name.name();
        let tenant_id = name.parent().id();

        if !self.tenants.contains_key(tenant_id) {
            return Err(ClusterMetadataError::NotFound {
                resource: "tenant",
                message: tenant_id.to_string(),
            });
        }

        if self.namespaces.contains_key(&namespace_key) {
            return Err(ClusterMetadataError::AlreadyExists {
                resource: "namespace",
                message: name.id().to_string(),
            });
        }

        let namespace = Namespace::new(name, options);
        self.namespaces.insert(namespace_key, namespace.clone());

        Ok(namespace)
    }

    fn get_namespace(&self, name: NamespaceName) -> Result<Namespace> {
        let namespace_key = name.name();
        self.namespaces
            .get(&namespace_key)
            .cloned()
            .ok_or_else(|| ClusterMetadataError::NotFound {
                resource: "namespace",
                message: name.id().to_string(),
            })
    }

    fn list_namespaces(&self, request: ListNamespacesRequest) -> Result<ListNamespacesResponse> {
        let tenant_id = request.parent.id();

        if !self.tenants.contains_key(tenant_id) {
            return Err(ClusterMetadataError::NotFound {
                resource: "tenant",
                message: tenant_id.to_string(),
            });
        }

        let page_size = request.page_size.unwrap_or(100).clamp(1, 1000) as usize;
        let page_token = request.page_token.as_deref().unwrap_or("");

        let mut namespace_keys: Vec<_> = self
            .namespaces
            .keys()
            .filter(|key| {
                if let Ok(ns_name) = NamespaceName::parse(key) {
                    ns_name.parent().id() == tenant_id
                } else {
                    false
                }
            })
            .collect();
        namespace_keys.sort();

        let start_index = if page_token.is_empty() {
            0
        } else {
            namespace_keys
                .iter()
                .position(|key| *key == page_token)
                .map(|pos| pos + 1)
                .unwrap_or(0)
        };

        let end_index = (start_index + page_size).min(namespace_keys.len());
        let page_namespace_keys = &namespace_keys[start_index..end_index];

        let namespaces: Vec<Namespace> = page_namespace_keys
            .iter()
            .filter_map(|key| self.namespaces.get(*key).cloned())
            .collect();

        let next_page_token = if end_index < namespace_keys.len() {
            Some(namespace_keys[end_index - 1].clone())
        } else {
            None
        };

        Ok(ListNamespacesResponse {
            namespaces,
            next_page_token,
        })
    }

    fn delete_namespace(&mut self, name: NamespaceName) -> Result<()> {
        let namespace_key = name.name();

        if !self.namespaces.contains_key(&namespace_key) {
            return Err(ClusterMetadataError::NotFound {
                resource: "namespace",
                message: name.id().to_string(),
            });
        }

        let has_topics = self
            .topics
            .values()
            .any(|topic| topic.name.parent().name() == namespace_key);

        if has_topics {
            return Err(ClusterMetadataError::InvalidArgument {
                resource: "namespace",
                message: format!("{} has topics and cannot be deleted", name.id()),
            });
        }

        self.namespaces.remove(&namespace_key);
        Ok(())
    }

    fn create_topic(&mut self, name: TopicName, options: TopicOptions) -> Result<Topic> {
        let topic_key = name.name();
        let namespace_key = name.parent().name();

        if !self.namespaces.contains_key(&namespace_key) {
            return Err(ClusterMetadataError::NotFound {
                resource: "namespace",
                message: name.parent().id().to_string(),
            });
        }

        if self.topics.contains_key(&topic_key) {
            return Err(ClusterMetadataError::AlreadyExists {
                resource: "topic",
                message: name.id().to_string(),
            });
        }

        if let Some(key_index) = options.partition_key {
            if key_index >= options.fields.len() {
                return Err(ClusterMetadataError::InvalidArgument {
                    resource: "topic",
                    message: format!(
                        "partition key index {} is out of bounds for fields (length: {})",
                        key_index,
                        options.fields.len()
                    ),
                });
            }
        }

        let topic = Topic::new(name, options);
        self.topics.insert(topic_key, topic.clone());

        Ok(topic)
    }

    fn get_topic(&self, name: TopicName) -> Result<Topic> {
        let topic_key = name.name();
        self.topics
            .get(&topic_key)
            .cloned()
            .ok_or_else(|| ClusterMetadataError::NotFound {
                resource: "topic",
                message: name.id().to_string(),
            })
    }

    fn list_topics(&self, request: ListTopicsRequest) -> Result<ListTopicsResponse> {
        let namespace_key = request.parent.name();

        if !self.namespaces.contains_key(&namespace_key) {
            return Err(ClusterMetadataError::NotFound {
                resource: "namespace",
                message: request.parent.id().to_string(),
            });
        }

        let page_size = request.page_size.unwrap_or(100).clamp(1, 1000);
        let page_token = request.page_token.as_deref().unwrap_or("");

        let mut topic_keys: Vec<_> = self
            .topics
            .keys()
            .filter(|key| {
                if let Ok(topic_name) = TopicName::parse(key) {
                    topic_name.parent().name() == namespace_key
                } else {
                    false
                }
            })
            .collect();
        topic_keys.sort();

        let start_index = if page_token.is_empty() {
            0
        } else {
            topic_keys
                .iter()
                .position(|key| *key == page_token)
                .map(|pos| pos + 1)
                .unwrap_or(0)
        };

        let end_index = (start_index + page_size).min(topic_keys.len());
        let page_topic_keys = &topic_keys[start_index..end_index];

        let topics: Vec<Topic> = page_topic_keys
            .iter()
            .filter_map(|key| self.topics.get(*key).cloned())
            .collect();

        let next_page_token = if end_index < topic_keys.len() {
            Some(topic_keys[end_index - 1].clone())
        } else {
            None
        };

        Ok(ListTopicsResponse {
            topics,
            next_page_token,
        })
    }

    fn delete_topic(&mut self, name: TopicName) -> Result<()> {
        let topic_key = name.name();

        if !self.topics.contains_key(&topic_key) {
            return Err(ClusterMetadataError::NotFound {
                resource: "topic",
                message: name.id().to_string(),
            });
        }

        self.topics.remove(&topic_key);
        Ok(())
    }
}

#[async_trait]
impl ClusterMetadata for InMemoryClusterMetadata {
    async fn create_tenant(&self, name: TenantName) -> Result<Tenant> {
        let mut store = self.store.write().await;
        store.create_tenant(name)
    }

    async fn get_tenant(&self, name: TenantName) -> Result<Tenant> {
        let store = self.store.read().await;
        store.get_tenant(name)
    }

    async fn list_tenants(&self, request: ListTenantsRequest) -> Result<ListTenantsResponse> {
        let store = self.store.read().await;
        store.list_tenants(request)
    }

    async fn delete_tenant(&self, name: TenantName) -> Result<()> {
        let mut store = self.store.write().await;
        store.delete_tenant(name)
    }

    async fn create_namespace(
        &self,
        name: NamespaceName,
        options: NamespaceOptions,
    ) -> Result<Namespace> {
        let mut store = self.store.write().await;
        store.create_namespace(name, options)
    }

    async fn get_namespace(&self, name: NamespaceName) -> Result<Namespace> {
        let store = self.store.read().await;
        store.get_namespace(name)
    }

    async fn list_namespaces(
        &self,
        request: ListNamespacesRequest,
    ) -> Result<ListNamespacesResponse> {
        let store = self.store.read().await;
        store.list_namespaces(request)
    }

    async fn delete_namespace(&self, name: NamespaceName) -> Result<()> {
        let mut store = self.store.write().await;
        store.delete_namespace(name)
    }

    async fn create_topic(&self, name: TopicName, options: TopicOptions) -> Result<Topic> {
        let mut store = self.store.write().await;
        store.create_topic(name, options)
    }

    async fn get_topic(&self, name: TopicName) -> Result<Topic> {
        let store = self.store.read().await;
        store.get_topic(name)
    }

    async fn list_topics(&self, request: ListTopicsRequest) -> Result<ListTopicsResponse> {
        let store = self.store.read().await;
        store.list_topics(request)
    }

    async fn delete_topic(&self, name: TopicName, _force: bool) -> Result<()> {
        let mut store = self.store.write().await;
        store.delete_topic(name)
    }
}

impl Default for InMemoryClusterMetadata {
    fn default() -> Self {
        Self::new()
    }
}
