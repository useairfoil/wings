//! In-memory implementation of the cluster metadata trait.
//!
//! This implementation stores all data in memory and is suitable for testing
//! and development. It uses a RwLock for thread-safe access.

use std::collections::HashMap;

use async_trait::async_trait;
use tokio::sync::RwLock;
use wings_control_plane_core::cluster_metadata::{
    ClusterMetadata, ClusterMetadataError, ClusterMetadataMetrics, ListDataLakesRequest,
    ListDataLakesResponse, ListNamespacesRequest, ListNamespacesResponse, ListObjectStoresRequest,
    ListObjectStoresResponse, ListTenantsRequest, ListTenantsResponse, ListTopicsRequest,
    ListTopicsResponse, Result,
};
use wings_observability::KeyValue;
use wings_resources::{
    DataLake, DataLakeConfiguration, DataLakeName, Namespace, NamespaceName, NamespaceOptions,
    ObjectStore, ObjectStoreConfiguration, ObjectStoreName, Tenant, TenantName, Topic, TopicName,
    TopicOptions, validate_compaction,
};

#[derive(Debug, Default)]
struct ClusterMetadataStore {
    /// Map of tenant ID to tenant data.
    tenants: HashMap<String, Tenant>,
    /// Map of namespace name to namespace data.
    namespaces: HashMap<String, Namespace>,
    /// Map of topic name to topic data.
    topics: HashMap<String, Topic>,
    /// Map of object store name to object store data.
    object_stores: HashMap<String, ObjectStore>,
    /// Map of data lake name to data lake data.
    data_lakes: HashMap<String, DataLake>,
}

/// In-memory implementation of the cluster metadata service.
///
#[derive(Debug)]
pub struct InMemoryClusterMetadata {
    store: RwLock<ClusterMetadataStore>,
    metrics: ClusterMetadataMetrics,
}

impl InMemoryClusterMetadata {
    /// Create a new in-memory cluster metadata service.
    pub fn new() -> Self {
        Self {
            store: RwLock::new(ClusterMetadataStore::default()),
            metrics: ClusterMetadataMetrics::default(),
        }
    }
}

impl ClusterMetadataStore {
    fn create_tenant(
        &mut self,
        name: TenantName,
        metrics: &ClusterMetadataMetrics,
    ) -> Result<Tenant> {
        let tenant_id = name.id().to_string();

        if self.tenants.contains_key(&tenant_id) {
            return Err(ClusterMetadataError::AlreadyExists {
                resource: "tenant",
                message: tenant_id.clone(),
            });
        }

        let tenant = Tenant::new(name);
        self.tenants.insert(tenant_id, tenant.clone());

        metrics.tenants_count.add(1, &[]);

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

    fn delete_tenant(&mut self, name: TenantName, metrics: &ClusterMetadataMetrics) -> Result<()> {
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

        metrics.tenants_count.add(-1, &[]);

        Ok(())
    }

    fn create_namespace(
        &mut self,
        name: NamespaceName,
        options: NamespaceOptions,
        metrics: &ClusterMetadataMetrics,
    ) -> Result<Namespace> {
        let namespace_key = name.name();
        let tenant_id = name.parent().id().to_string();

        if !self.tenants.contains_key(&tenant_id) {
            return Err(ClusterMetadataError::NotFound {
                resource: "tenant",
                message: tenant_id,
            });
        }

        if self.namespaces.contains_key(&namespace_key) {
            return Err(ClusterMetadataError::AlreadyExists {
                resource: "namespace",
                message: name.id().to_string(),
            });
        }

        let namespace = Namespace::new(name, options);

        if namespace.object_store.parent() != namespace.name.parent() {
            return Err(ClusterMetadataError::InvalidArgument {
                resource: "object_store",
                message: format!(
                    "Object store '{}' must be in the same tenant as the namespace",
                    namespace.object_store.name()
                ),
            });
        }

        if !self
            .object_stores
            .contains_key(&namespace.object_store.name())
        {
            return Err(ClusterMetadataError::InvalidArgument {
                resource: "object_store",
                message: format!("Object store '{}' not found", namespace.object_store.name()),
            });
        }

        if namespace.data_lake.parent() != namespace.name.parent() {
            return Err(ClusterMetadataError::InvalidArgument {
                resource: "data_lake",
                message: format!(
                    "Data lake '{}' must be in the same tenant as the namespace",
                    namespace.data_lake.name()
                ),
            });
        }

        if !self.data_lakes.contains_key(&namespace.data_lake.name()) {
            return Err(ClusterMetadataError::InvalidArgument {
                resource: "data_lake",
                message: format!("Data lake '{}' not found", namespace.data_lake.name()),
            });
        }

        self.namespaces.insert(namespace_key, namespace.clone());

        metrics
            .namespaces_count
            .add(1, &[KeyValue::new("tenant", tenant_id)]);

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

    fn delete_namespace(
        &mut self,
        name: NamespaceName,
        metrics: &ClusterMetadataMetrics,
    ) -> Result<()> {
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

        let tenant_id = name.parent.id().to_string();
        metrics
            .namespaces_count
            .add(-1, &[KeyValue::new("tenant", tenant_id)]);

        Ok(())
    }

    fn create_topic(
        &mut self,
        name: TopicName,
        options: TopicOptions,
        metrics: &ClusterMetadataMetrics,
    ) -> Result<Topic> {
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

        if let Some(partition_key) = options.partition_key
            && !options
                .schema
                .fields_iter()
                .any(|field| field.id == partition_key)
        {
            return Err(ClusterMetadataError::InvalidArgument {
                resource: "topic",
                message: format!("no field with id {partition_key} found in schema"),
            });
        }

        let namespace_id = name.parent().id().to_string();
        let tenant_id = name.parent().parent().id().to_string();

        let topic = Topic::new(name, options);

        if let Err(errors) = validate_compaction(&topic.compaction) {
            let message = errors.join(", ");
            return Err(ClusterMetadataError::InvalidArgument {
                resource: "topic",
                message: format!("compaction configuration is invalid: {message}"),
            });
        }

        self.topics.insert(topic_key, topic.clone());

        metrics.topics_count.add(
            1,
            &[
                KeyValue::new("tenant", tenant_id),
                KeyValue::new("namespace", namespace_id),
            ],
        );

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

    fn delete_topic(&mut self, name: TopicName, metrics: &ClusterMetadataMetrics) -> Result<()> {
        let topic_key = name.name();

        if !self.topics.contains_key(&topic_key) {
            return Err(ClusterMetadataError::NotFound {
                resource: "topic",
                message: name.id().to_string(),
            });
        }

        let namespace_id = name.parent().id().to_string();
        let tenant_id = name.parent().parent().id().to_string();

        self.topics.remove(&topic_key);

        metrics.topics_count.add(
            -1,
            &[
                KeyValue::new("tenant", tenant_id),
                KeyValue::new("namespace", namespace_id),
            ],
        );

        Ok(())
    }

    fn create_object_store(
        &mut self,
        name: ObjectStoreName,
        configuration: ObjectStoreConfiguration,
        metrics: &ClusterMetadataMetrics,
    ) -> Result<ObjectStore> {
        let object_store_key = name.name();

        if self.object_stores.contains_key(&object_store_key) {
            return Err(ClusterMetadataError::AlreadyExists {
                resource: "object store",
                message: name.id().to_string(),
            });
        }

        let object_store = ObjectStore {
            name: name.clone(),
            object_store: configuration,
        };

        self.object_stores
            .insert(object_store_key.clone(), object_store.clone());

        let tenant_id = name.parent().id().to_string();
        metrics
            .object_stores_count
            .add(1, &[KeyValue::new("tenant", tenant_id)]);

        Ok(object_store)
    }

    fn get_object_store(&self, name: ObjectStoreName) -> Result<ObjectStore> {
        let object_store_key = name.name();

        self.object_stores
            .get(&object_store_key)
            .cloned()
            .ok_or_else(|| ClusterMetadataError::NotFound {
                resource: "object store",
                message: name.id().to_string(),
            })
    }

    fn list_object_stores(
        &self,
        request: ListObjectStoresRequest,
    ) -> Result<ListObjectStoresResponse> {
        let tenant_id = request.parent.id();

        if !self.tenants.contains_key(tenant_id) {
            return Err(ClusterMetadataError::NotFound {
                resource: "tenant",
                message: tenant_id.to_string(),
            });
        }

        let page_size = request.page_size.unwrap_or(100).clamp(1, 1000) as usize;
        let page_token = request.page_token.as_deref().unwrap_or("");

        let mut object_store_keys: Vec<_> = self
            .object_stores
            .keys()
            .filter(|key| {
                if let Ok(object_store_name) = ObjectStoreName::parse(key) {
                    object_store_name.parent().id() == tenant_id
                } else {
                    false
                }
            })
            .collect();
        object_store_keys.sort();

        let start_index = if page_token.is_empty() {
            0
        } else {
            object_store_keys
                .iter()
                .position(|key| *key == page_token)
                .map(|pos| pos + 1)
                .unwrap_or(0)
        };

        let end_index = (start_index + page_size).min(object_store_keys.len());
        let page_object_store_keys = &object_store_keys[start_index..end_index];

        let object_stores: Vec<ObjectStore> = page_object_store_keys
            .iter()
            .filter_map(|key| self.object_stores.get(*key).cloned())
            .collect();

        let next_page_token = if end_index < object_store_keys.len() {
            Some(object_store_keys[end_index - 1].clone())
        } else {
            None
        };

        Ok(ListObjectStoresResponse {
            object_stores,
            next_page_token,
        })
    }

    fn delete_object_store(
        &mut self,
        name: ObjectStoreName,
        metrics: &ClusterMetadataMetrics,
    ) -> Result<()> {
        let object_store_key = name.name();

        if !self.object_stores.contains_key(&object_store_key) {
            return Err(ClusterMetadataError::NotFound {
                resource: "object store",
                message: name.id().to_string(),
            });
        }

        let tenant_id = name.parent().id().to_string();

        self.object_stores.remove(&object_store_key);

        metrics
            .object_stores_count
            .add(-1, &[KeyValue::new("tenant", tenant_id)]);

        Ok(())
    }

    fn create_data_lake(
        &mut self,
        name: DataLakeName,
        configuration: DataLakeConfiguration,
        metrics: &ClusterMetadataMetrics,
    ) -> Result<DataLake> {
        let data_lake_key = name.name();

        if self.data_lakes.contains_key(&data_lake_key) {
            return Err(ClusterMetadataError::AlreadyExists {
                resource: "data lake",
                message: name.id().to_string(),
            });
        }

        let data_lake = DataLake {
            name: name.clone(),
            data_lake: configuration,
        };

        self.data_lakes
            .insert(data_lake_key.clone(), data_lake.clone());

        let tenant_id = name.parent().id().to_string();
        metrics
            .data_lakes_count
            .add(1, &[KeyValue::new("tenant", tenant_id)]);

        Ok(data_lake)
    }

    fn get_data_lake(&self, name: DataLakeName) -> Result<DataLake> {
        let data_lake_key = name.name();

        self.data_lakes
            .get(&data_lake_key)
            .cloned()
            .ok_or_else(|| ClusterMetadataError::NotFound {
                resource: "data lake",
                message: name.id().to_string(),
            })
    }

    fn list_data_lakes(&self, request: ListDataLakesRequest) -> Result<ListDataLakesResponse> {
        let tenant_id = request.parent.id();

        if !self.tenants.contains_key(tenant_id) {
            return Err(ClusterMetadataError::NotFound {
                resource: "tenant",
                message: tenant_id.to_string(),
            });
        }

        let page_size = request.page_size.unwrap_or(100).clamp(1, 1000) as usize;
        let page_token = request.page_token.as_deref().unwrap_or("");

        let mut data_lake_keys: Vec<_> = self
            .data_lakes
            .keys()
            .filter(|key| {
                if let Ok(data_lake_name) = DataLakeName::parse(key) {
                    data_lake_name.parent().id() == tenant_id
                } else {
                    false
                }
            })
            .collect();
        data_lake_keys.sort();

        let start_index = if page_token.is_empty() {
            0
        } else {
            data_lake_keys
                .iter()
                .position(|key| *key == page_token)
                .map(|pos| pos + 1)
                .unwrap_or(0)
        };

        let end_index = (start_index + page_size).min(data_lake_keys.len());
        let page_data_lake_keys = &data_lake_keys[start_index..end_index];

        let data_lakes: Vec<DataLake> = page_data_lake_keys
            .iter()
            .filter_map(|key| self.data_lakes.get(*key).cloned())
            .collect();

        let next_page_token = if end_index < data_lake_keys.len() {
            Some(data_lake_keys[end_index - 1].clone())
        } else {
            None
        };

        Ok(ListDataLakesResponse {
            data_lakes,
            next_page_token,
        })
    }

    fn delete_data_lake(
        &mut self,
        name: DataLakeName,
        metrics: &ClusterMetadataMetrics,
    ) -> Result<()> {
        let data_lake_key = name.name();

        if !self.data_lakes.contains_key(&data_lake_key) {
            return Err(ClusterMetadataError::NotFound {
                resource: "data lake",
                message: name.id().to_string(),
            });
        }

        let tenant_id = name.parent().id().to_string();

        self.data_lakes.remove(&data_lake_key);

        metrics
            .data_lakes_count
            .add(-1, &[KeyValue::new("tenant", tenant_id)]);

        Ok(())
    }
}

#[async_trait]
impl ClusterMetadata for InMemoryClusterMetadata {
    async fn create_tenant(&self, name: TenantName) -> Result<Tenant> {
        let mut store = self.store.write().await;
        store.create_tenant(name, &self.metrics)
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
        store.delete_tenant(name, &self.metrics)
    }

    async fn create_namespace(
        &self,
        name: NamespaceName,
        options: NamespaceOptions,
    ) -> Result<Namespace> {
        let mut store = self.store.write().await;
        store.create_namespace(name, options, &self.metrics)
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
        store.delete_namespace(name, &self.metrics)
    }

    async fn create_topic(&self, name: TopicName, options: TopicOptions) -> Result<Topic> {
        let mut store = self.store.write().await;
        store.create_topic(name, options, &self.metrics)
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
        store.delete_topic(name, &self.metrics)
    }

    async fn create_object_store(
        &self,
        name: ObjectStoreName,
        configuration: ObjectStoreConfiguration,
    ) -> Result<ObjectStore> {
        let mut store = self.store.write().await;
        store.create_object_store(name, configuration, &self.metrics)
    }

    async fn get_object_store(&self, name: ObjectStoreName) -> Result<ObjectStore> {
        let store = self.store.read().await;
        store.get_object_store(name)
    }

    async fn list_object_stores(
        &self,
        request: ListObjectStoresRequest,
    ) -> Result<ListObjectStoresResponse> {
        let store = self.store.read().await;
        store.list_object_stores(request)
    }

    async fn delete_object_store(&self, name: ObjectStoreName) -> Result<()> {
        let mut store = self.store.write().await;
        store.delete_object_store(name, &self.metrics)
    }

    async fn create_data_lake(
        &self,
        name: DataLakeName,
        configuration: DataLakeConfiguration,
    ) -> Result<DataLake> {
        let mut store = self.store.write().await;
        store.create_data_lake(name, configuration, &self.metrics)
    }

    async fn get_data_lake(&self, name: DataLakeName) -> Result<DataLake> {
        let store = self.store.read().await;
        store.get_data_lake(name)
    }

    async fn list_data_lakes(
        &self,
        request: ListDataLakesRequest,
    ) -> Result<ListDataLakesResponse> {
        let store = self.store.read().await;
        store.list_data_lakes(request)
    }

    async fn delete_data_lake(&self, name: DataLakeName) -> Result<()> {
        let mut store = self.store.write().await;
        store.delete_data_lake(name, &self.metrics)
    }
}

impl Default for InMemoryClusterMetadata {
    fn default() -> Self {
        Self::new()
    }
}
