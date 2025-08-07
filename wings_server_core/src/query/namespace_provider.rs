use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    catalog::{CatalogProvider, SchemaProvider, TableProvider},
    error::DataFusionError,
    execution::SessionStateBuilder,
    prelude::{SessionConfig, SessionContext},
};
use tracing::debug;
use wings_metadata_core::admin::{Admin, NamespaceName, Topic, collect_namespace_topics};

use crate::system_tables::SystemSchemaProvider;

pub const DEFAULT_CATALOG: &str = "wings";
pub const DEFAULT_SCHEMA: &str = "public";
pub const SYSTEM_SCHEMA: &str = "system";

#[derive(Clone)]
pub struct NamespaceProvider {
    namespace: NamespaceName,
    topics: Vec<Topic>,
    system_schema_provider: Arc<SystemSchemaProvider>,
}

impl NamespaceProvider {
    pub async fn new(
        admin: Arc<dyn Admin>,
        namespace: NamespaceName,
    ) -> Result<Self, DataFusionError> {
        let topics = collect_namespace_topics(&admin, &namespace).await?;
        let system_schema_provider = SystemSchemaProvider::new(admin.clone(), namespace.clone());

        Ok(Self {
            namespace,
            topics,
            system_schema_provider: Arc::new(system_schema_provider),
        })
    }

    pub fn new_session_context(&self) -> SessionContext {
        let config = SessionConfig::new()
            .with_information_schema(true)
            .with_default_catalog_and_schema(DEFAULT_CATALOG, DEFAULT_SCHEMA);

        let state = SessionStateBuilder::new().with_config(config);

        let ctx = SessionContext::new_with_state(state.build());

        ctx.register_catalog(DEFAULT_CATALOG, Arc::new(self.clone()));

        ctx
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
        self.topics
            .iter()
            .find(|topic| topic.name.id == name)
            .is_some()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        todo!()
    }
}

impl std::fmt::Debug for NamespaceProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NamespaceProvider")
            .field("namespace", &self.namespace)
            .finish()
    }
}
