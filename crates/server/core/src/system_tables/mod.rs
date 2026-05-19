mod exec;
pub mod helpers;
mod metrics;
mod namespace_info;
mod provider;
mod table;
mod table_row_location;
mod table_partition_value;
mod table_schema;

use std::{any::Any, collections::HashMap, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    catalog::{SchemaProvider, TableProvider},
    error::DataFusionError,
};
use wings_control_plane_core::{cluster_metadata::ClusterMetadata, table_metadata::TableMetadata};
use wings_observability::MetricsExporter;
use wings_resources::NamespaceName;

use self::{
    metrics::MetricsSystemTable, namespace_info::NamespaceInfoTable, provider::SystemTableProvider,
    table::TableSystemTable, table_row_location::TableRowLocationSystemTable,
    table_partition_value::TablePartitionValueSystemTable, table_schema::TableSchemaTable,
};

pub const NAMESPACE_INFO_TABLE_NAME: &str = "namespace_info";
pub const TOPIC_TABLE_NAME: &str = "table";
pub const TOPIC_SCHEMA_TABLE_NAME: &str = "table_schema";
pub const TOPIC_PARTITION_VALUE_TABLE_NAME: &str = "table_partition_value";
pub const TABLE_ROW_LOCATION_TABLE_NAME: &str = "table_row_location";
pub const METRICS_TABLE_NAME: &str = "metrics";

pub struct SystemSchemaProvider {
    namespace: NamespaceName,
    tables: HashMap<&'static str, Arc<dyn TableProvider>>,
}

#[async_trait]
impl SchemaProvider for SystemSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self as _
    }

    fn table_names(&self) -> Vec<String> {
        self.tables.keys().map(|name| name.to_string()).collect()
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        Ok(self.tables.get(name).cloned())
    }
}

impl SystemSchemaProvider {
    pub fn new(
        cluster_meta: Arc<dyn ClusterMetadata>,
        table_metadata: Arc<dyn TableMetadata>,
        metrics_exporter: MetricsExporter,
        namespace: NamespaceName,
    ) -> Self {
        let mut tables = HashMap::<&'static str, Arc<dyn TableProvider>>::new();

        let namespace_info = Arc::new(SystemTableProvider::new(NamespaceInfoTable::new(
            cluster_meta.clone(),
            namespace.clone(),
        )));
        tables.insert(NAMESPACE_INFO_TABLE_NAME, namespace_info);

        let table = Arc::new(TableSystemTable::new(
            cluster_meta.clone(),
            namespace.clone(),
        ));
        tables.insert(TOPIC_TABLE_NAME, table);

        let table_schema = Arc::new(SystemTableProvider::new(TableSchemaTable::new(
            cluster_meta.clone(),
            namespace.clone(),
        )));
        tables.insert(TOPIC_SCHEMA_TABLE_NAME, table_schema);

        let table_partition_value = Arc::new(TablePartitionValueSystemTable::new(
            cluster_meta.clone(),
            table_metadata.clone(),
            namespace.clone(),
        ));
        tables.insert(TOPIC_PARTITION_VALUE_TABLE_NAME, table_partition_value);

        let table_row_location = Arc::new(TableRowLocationSystemTable::new(
            cluster_meta.clone(),
            table_metadata.clone(),
            namespace.clone(),
        ));
        tables.insert(TABLE_ROW_LOCATION_TABLE_NAME, table_row_location);

        let metrics = Arc::new(MetricsSystemTable::new(metrics_exporter));
        tables.insert(METRICS_TABLE_NAME, metrics);

        Self { namespace, tables }
    }
}

impl std::fmt::Debug for SystemSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SystemSchemaProvider")
            .field("namespace", &self.namespace)
            .finish()
    }
}
