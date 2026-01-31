mod cluster_metadata;
mod log_metadata;

use async_trait::async_trait;
use tokio::sync::RwLock;
use wings_control_plane_core::{
    ClusterMetadata, ClusterMetadataError,
    cluster_metadata::{
        ClusterMetadataMetrics, ListDataLakesRequest, ListDataLakesResponse, ListNamespacesRequest,
        ListNamespacesResponse, ListObjectStoresRequest, ListObjectStoresResponse,
        ListTenantsRequest, ListTenantsResponse, ListTopicsRequest, ListTopicsResponse,
        Result as ClusterResult, TopicView,
    },
    log_metadata::{
        CommitPageRequest, CommitPageResponse, CompleteTaskRequest, CompleteTaskResponse,
        GetLogLocationRequest, ListPartitionsRequest, ListPartitionsResponse, LogLocation,
        LogMetadata, RequestTaskRequest, RequestTaskResponse, Result as LogResult,
    },
};
use wings_resources::{
    DataLake, DataLakeConfiguration, DataLakeName, Namespace, NamespaceName, NamespaceOptions,
    ObjectStore, ObjectStoreConfiguration, ObjectStoreName, Tenant, TenantName, Topic, TopicName,
    TopicOptions,
};

use crate::{cluster_metadata::ClusterMetadataStore, log_metadata::LogMetadataStore};

/// In-memory implementation of the control plane.
#[derive(Debug)]
pub struct InMemoryControlPlane {
    cluster_store: RwLock<ClusterMetadataStore>,
    log_store: LogMetadataStore,
    cluster_metrics: ClusterMetadataMetrics,
}

impl InMemoryControlPlane {
    pub fn new() -> Self {
        Self {
            cluster_store: RwLock::new(ClusterMetadataStore::default()),
            log_store: LogMetadataStore::new(),
            cluster_metrics: ClusterMetadataMetrics::default(),
        }
    }
}

impl Default for InMemoryControlPlane {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ClusterMetadata for InMemoryControlPlane {
    async fn create_tenant(&self, name: TenantName) -> ClusterResult<Tenant> {
        let mut store = self.cluster_store.write().await;
        store.create_tenant(name, &self.cluster_metrics)
    }

    async fn get_tenant(&self, name: TenantName) -> ClusterResult<Tenant> {
        let store = self.cluster_store.read().await;
        store.get_tenant(name)
    }

    async fn list_tenants(
        &self,
        request: ListTenantsRequest,
    ) -> ClusterResult<ListTenantsResponse> {
        let store = self.cluster_store.read().await;
        store.list_tenants(request)
    }

    async fn delete_tenant(&self, name: TenantName) -> ClusterResult<()> {
        let mut store = self.cluster_store.write().await;
        store.delete_tenant(name, &self.cluster_metrics)
    }

    async fn create_namespace(
        &self,
        name: NamespaceName,
        options: NamespaceOptions,
    ) -> ClusterResult<Namespace> {
        let mut store = self.cluster_store.write().await;
        store.create_namespace(name, options, &self.cluster_metrics)
    }

    async fn get_namespace(&self, name: NamespaceName) -> ClusterResult<Namespace> {
        let store = self.cluster_store.read().await;
        store.get_namespace(name)
    }

    async fn list_namespaces(
        &self,
        request: ListNamespacesRequest,
    ) -> ClusterResult<ListNamespacesResponse> {
        let store = self.cluster_store.read().await;
        store.list_namespaces(request)
    }

    async fn delete_namespace(&self, name: NamespaceName) -> ClusterResult<()> {
        let mut store = self.cluster_store.write().await;
        store.delete_namespace(name, &self.cluster_metrics)
    }

    async fn create_topic(&self, name: TopicName, options: TopicOptions) -> ClusterResult<Topic> {
        let mut store = self.cluster_store.write().await;
        store.create_topic(name, options, &self.cluster_metrics)
    }

    async fn get_topic(&self, name: TopicName, view: TopicView) -> ClusterResult<Topic> {
        let topic = {
            let store = self.cluster_store.read().await;
            store.get_topic(name)
        }?;

        if view == TopicView::Basic {
            return Ok(topic);
        }

        let status = self.log_store.topic_status(&topic).await.map_err(|_| {
            ClusterMetadataError::Internal {
                message: "failed to fetch topic status".to_string(),
            }
        })?;

        Ok(topic.with_status(status))
    }

    async fn list_topics(&self, request: ListTopicsRequest) -> ClusterResult<ListTopicsResponse> {
        let store = self.cluster_store.read().await;
        store.list_topics(request)
    }

    async fn delete_topic(&self, name: TopicName, _force: bool) -> ClusterResult<()> {
        let mut store = self.cluster_store.write().await;
        store.delete_topic(name, &self.cluster_metrics)
    }

    async fn create_object_store(
        &self,
        name: ObjectStoreName,
        configuration: ObjectStoreConfiguration,
    ) -> ClusterResult<ObjectStore> {
        let mut store = self.cluster_store.write().await;
        store.create_object_store(name, configuration, &self.cluster_metrics)
    }

    async fn get_object_store(&self, name: ObjectStoreName) -> ClusterResult<ObjectStore> {
        let store = self.cluster_store.read().await;
        store.get_object_store(name)
    }

    async fn list_object_stores(
        &self,
        request: ListObjectStoresRequest,
    ) -> ClusterResult<ListObjectStoresResponse> {
        let store = self.cluster_store.read().await;
        store.list_object_stores(request)
    }

    async fn delete_object_store(&self, name: ObjectStoreName) -> ClusterResult<()> {
        let mut store = self.cluster_store.write().await;
        store.delete_object_store(name, &self.cluster_metrics)
    }

    async fn create_data_lake(
        &self,
        name: DataLakeName,
        configuration: DataLakeConfiguration,
    ) -> ClusterResult<DataLake> {
        let mut store = self.cluster_store.write().await;
        store.create_data_lake(name, configuration, &self.cluster_metrics)
    }

    async fn get_data_lake(&self, name: DataLakeName) -> ClusterResult<DataLake> {
        let store = self.cluster_store.read().await;
        store.get_data_lake(name)
    }

    async fn list_data_lakes(
        &self,
        request: ListDataLakesRequest,
    ) -> ClusterResult<ListDataLakesResponse> {
        let store = self.cluster_store.read().await;
        store.list_data_lakes(request)
    }

    async fn delete_data_lake(&self, name: DataLakeName) -> ClusterResult<()> {
        let mut store = self.cluster_store.write().await;
        store.delete_data_lake(name, &self.cluster_metrics)
    }
}

#[async_trait]
impl LogMetadata for InMemoryControlPlane {
    async fn commit_folio(
        &self,
        namespace: NamespaceName,
        file_ref: String,
        pages: &[CommitPageRequest],
    ) -> LogResult<Vec<CommitPageResponse>> {
        let store = self.cluster_store.read().await;
        self.log_store
            .commit_folio(namespace, file_ref, pages, &store)
            .await
    }

    async fn get_log_location(
        &self,
        request: GetLogLocationRequest,
    ) -> LogResult<Vec<LogLocation>> {
        self.log_store.get_log_location(request).await
    }

    async fn list_partitions(
        &self,
        request: ListPartitionsRequest,
    ) -> LogResult<ListPartitionsResponse> {
        let store = self.cluster_store.read().await;
        self.log_store.list_partitions(request, &store).await
    }

    async fn request_task(&self, request: RequestTaskRequest) -> LogResult<RequestTaskResponse> {
        self.log_store.request_task(request).await
    }

    async fn complete_task(&self, request: CompleteTaskRequest) -> LogResult<CompleteTaskResponse> {
        self.log_store.complete_task(request).await
    }
}
