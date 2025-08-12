mod exec;
mod helpers;
mod namespace_info;
mod provider;
mod topic;
mod topic_offset_location;
mod topic_partition_value;
mod topic_schema;

use std::{any::Any, collections::HashMap, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    catalog::{SchemaProvider, TableProvider},
    error::DataFusionError,
};
use namespace_info::NamespaceInfoTable;
use provider::SystemTableProvider;
use topic::TopicSystemTable;
use topic_offset_location::TopicOffsetLocationSystemTable;
use topic_partition_value::TopicPartitionValueSystemTable;
use topic_schema::TopicSchemaTable;
use wings_metadata_core::{
    admin::{Admin, NamespaceName},
    offset_registry::OffsetRegistry,
};

pub const NAMESPACE_INFO_TABLE_NAME: &str = "namespace_info";
pub const TOPIC_TABLE_NAME: &str = "topic";
pub const TOPIC_SCHEMA_TABLE_NAME: &str = "topic_schema";
pub const TOPIC_PARTITION_VALUE_TABLE_NAME: &str = "topic_partition_value";
pub const TOPIC_OFFSET_LOCATION_TABLE_NAME: &str = "topic_offset_location";

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
        admin: Arc<dyn Admin>,
        offset_registry: Arc<dyn OffsetRegistry>,
        namespace: NamespaceName,
    ) -> Self {
        let mut tables = HashMap::<&'static str, Arc<dyn TableProvider>>::new();

        let namespace_info = Arc::new(SystemTableProvider::new(NamespaceInfoTable::new(
            admin.clone(),
            namespace.clone(),
        )));
        tables.insert(NAMESPACE_INFO_TABLE_NAME, namespace_info);

        let topic = Arc::new(TopicSystemTable::new(admin.clone(), namespace.clone()));
        tables.insert(TOPIC_TABLE_NAME, topic);

        let topic_schema = Arc::new(SystemTableProvider::new(TopicSchemaTable::new(
            admin.clone(),
            namespace.clone(),
        )));
        tables.insert(TOPIC_SCHEMA_TABLE_NAME, topic_schema);

        let topic_partition_value = Arc::new(TopicPartitionValueSystemTable::new(
            admin.clone(),
            offset_registry.clone(),
            namespace.clone(),
        ));
        tables.insert(TOPIC_PARTITION_VALUE_TABLE_NAME, topic_partition_value);

        let topic_offset_location = Arc::new(TopicOffsetLocationSystemTable::new(
            admin.clone(),
            namespace.clone(),
        ));
        tables.insert(TOPIC_OFFSET_LOCATION_TABLE_NAME, topic_offset_location);

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
