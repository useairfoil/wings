use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    catalog::{CatalogProvider, SchemaProvider, TableProvider},
    error::DataFusionError,
    execution::SessionStateBuilder,
    prelude::{SessionConfig, SessionContext},
};
use tracing::debug;
use wings_control_plane::{
    cluster_metadata::{ClusterMetadata, CollectNamespaceTopicsOptions, collect_namespace_topics},
    log_metadata::LogMetadata,
    resources::{Namespace, NamespaceName, Topic},
};
use wings_object_store::ObjectStoreFactory;
use wings_observability::MetricsExporter;

use crate::{
    options::FetchOptions, query::topic::TopicTableProvider, system_tables::SystemSchemaProvider,
};

pub const DEFAULT_CATALOG: &str = "wings";
pub const DEFAULT_SCHEMA: &str = "public";
pub const SYSTEM_SCHEMA: &str = "system";

#[derive(Clone)]
pub struct NamespaceProviderFactory {
    cluster_meta: Arc<dyn ClusterMetadata>,
    log_meta: Arc<dyn LogMetadata>,
    object_store_factory: Arc<dyn ObjectStoreFactory>,
    metrics_exporter: MetricsExporter,
}

#[derive(Clone)]
pub struct NamespaceProvider {
    log_meta: Arc<dyn LogMetadata>,
    object_store_factory: Arc<dyn ObjectStoreFactory>,
    namespace: Namespace,
    topics: Vec<Topic>,
    system_schema_provider: Arc<SystemSchemaProvider>,
}

impl NamespaceProviderFactory {
    pub fn new(
        cluster_meta: Arc<dyn ClusterMetadata>,
        log_meta: Arc<dyn LogMetadata>,
        metrics_exporter: MetricsExporter,
        object_store_factory: Arc<dyn ObjectStoreFactory>,
    ) -> Self {
        Self {
            cluster_meta,
            log_meta,
            object_store_factory,
            metrics_exporter,
        }
    }

    pub async fn create_provider(
        &self,
        namespace_name: NamespaceName,
    ) -> Result<NamespaceProvider, DataFusionError> {
        NamespaceProvider::new(
            self.cluster_meta.clone(),
            self.log_meta.clone(),
            self.object_store_factory.clone(),
            self.metrics_exporter.clone(),
            namespace_name,
        )
        .await
    }
}

impl NamespaceProvider {
    pub async fn new(
        cluster_meta: Arc<dyn ClusterMetadata>,
        log_meta: Arc<dyn LogMetadata>,
        object_store_factory: Arc<dyn ObjectStoreFactory>,
        metrics_exporter: MetricsExporter,
        namespace_name: NamespaceName,
    ) -> Result<Self, DataFusionError> {
        let namespace = cluster_meta.get_namespace(namespace_name.clone()).await?;
        // TODO: for now we collect all topics here. This becomes slow for large namespaces.
        let topics = collect_namespace_topics(
            &cluster_meta,
            &namespace_name,
            CollectNamespaceTopicsOptions::default(),
        )
        .await?;
        let system_schema_provider = SystemSchemaProvider::new(
            cluster_meta.clone(),
            log_meta.clone(),
            metrics_exporter,
            namespace_name,
        );

        Ok(Self {
            log_meta,
            object_store_factory,
            namespace,
            topics,
            system_schema_provider: Arc::new(system_schema_provider),
        })
    }

    pub async fn new_session_context(&self) -> Result<SessionContext, DataFusionError> {
        let config = SessionConfig::new()
            .with_information_schema(true)
            .with_default_catalog_and_schema(DEFAULT_CATALOG, DEFAULT_SCHEMA)
            .with_option_extension(FetchOptions::default());

        let state = SessionStateBuilder::new().with_config(config);

        let ctx = SessionContext::new_with_state(state.build());

        ctx.register_catalog(DEFAULT_CATALOG, Arc::new(self.clone()));

        let object_store = self
            .object_store_factory
            .create_object_store(self.namespace.default_object_store_credentials.clone())
            .await?;

        let object_store_url = self
            .namespace
            .default_object_store_credentials
            .wings_object_store_url()?;
        ctx.register_object_store(object_store_url.as_ref(), object_store);

        Ok(ctx)
    }
}

impl CatalogProvider for NamespaceProvider {
    fn as_any(&self) -> &dyn Any {
        self as _
    }

    fn schema_names(&self) -> Vec<String> {
        debug!("CatalogProvider::schema_names");
        vec![DEFAULT_SCHEMA.to_string(), SYSTEM_SCHEMA.to_string()]
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        debug!(name, "CatalogProvider::schema");
        match name {
            DEFAULT_SCHEMA => Some(Arc::new(self.clone())),
            SYSTEM_SCHEMA => Some(self.system_schema_provider.clone()),
            _ => None,
        }
    }
}

#[async_trait]
impl SchemaProvider for NamespaceProvider {
    fn as_any(&self) -> &dyn Any {
        self as _
    }

    fn table_names(&self) -> Vec<String> {
        self.topics
            .iter()
            .map(|topic| topic.name.id.clone())
            .collect()
    }

    fn table_exist(&self, name: &str) -> bool {
        self.topics.iter().any(|topic| topic.name.id == name)
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        let provider = self
            .topics
            .iter()
            .find(|topic| topic.name.id == name)
            .map(|topic| {
                TopicTableProvider::new_provider(
                    self.log_meta.clone(),
                    self.namespace.clone(),
                    topic.clone(),
                )
            });
        Ok(provider)
    }
}

impl std::fmt::Debug for NamespaceProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NamespaceProvider")
            .field("namespace", &self.namespace)
            .finish()
    }
}
