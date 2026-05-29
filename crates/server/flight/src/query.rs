use std::{any::Any, sync::Arc};

use datafusion::{
    catalog::{CatalogProvider, SchemaProvider},
    error::DataFusionError,
    execution::SessionStateBuilder,
    prelude::{SessionConfig, SessionContext},
};
use tracing::debug;
use wings_meta_db::{ClusterStore, StoredNamespace};
use wings_resources::NamespaceName;

use crate::system_tables::SystemSchemaProvider;

pub const DEFAULT_CATALOG: &str = "wings";
pub const DEFAULT_SCHEMA: &str = "public";
pub const SYSTEM_SCHEMA: &str = "system";

#[derive(Clone)]
pub struct NamespaceProvider {
    store: ClusterStore,
    namespace: StoredNamespace,
}

impl NamespaceProvider {
    pub async fn init(store: ClusterStore, name: NamespaceName) -> Result<Self, DataFusionError> {
        let namespace = store
            .namespace(name)
            .load()
            .await
            .map_err(|err| DataFusionError::External(Box::new(err)))?;

        Ok(Self { store, namespace })
    }

    pub async fn new_session_context(&self) -> Result<SessionContext, DataFusionError> {
        let config = SessionConfig::new()
            .with_information_schema(true)
            .with_default_catalog_and_schema(DEFAULT_CATALOG, DEFAULT_SCHEMA);

        let state = SessionStateBuilder::new().with_config(config);

        let ctx = SessionContext::new_with_state(state.build());

        ctx.register_catalog(DEFAULT_CATALOG, Arc::new(self.clone()));

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
            DEFAULT_SCHEMA => None,
            SYSTEM_SCHEMA => Some(Arc::new(SystemSchemaProvider::new(
                self.store.clone(),
                self.namespace.name().clone(),
            ))),
            _ => None,
        }
    }
}

impl std::fmt::Debug for NamespaceProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NamespaceProvider")
            .field("namespace", &self.namespace)
            .finish()
    }
}
