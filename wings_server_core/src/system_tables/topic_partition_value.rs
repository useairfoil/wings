use std::{any::Any, sync::Arc};

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    catalog::{Session, TableProvider},
    datasource::TableType,
    error::DataFusionError,
    logical_expr::TableProviderFilterPushDown,
    physical_plan::ExecutionPlan,
    prelude::Expr,
};

use crate::{
    datafusion_helpers::apply_projection,
    system_tables::{exec::TopicPartitionValueDiscoveryExec, helpers::find_topic_name_in_filters},
};
use wings_metadata_core::{
    admin::{Admin, NamespaceName},
    offset_registry::OffsetRegistry,
};

/// System table for discovering partition values for topics.
pub struct TopicPartitionValueSystemTable {
    admin: Arc<dyn Admin>,
    offset_registry: Arc<dyn OffsetRegistry>,
    namespace: NamespaceName,
    schema: SchemaRef,
}

impl TopicPartitionValueSystemTable {
    pub fn new(
        admin: Arc<dyn Admin>,
        offset_registry: Arc<dyn OffsetRegistry>,
        namespace: NamespaceName,
    ) -> Self {
        Self {
            admin,
            offset_registry,
            namespace,
            schema: TopicPartitionValueDiscoveryExec::schema(),
        }
    }
}

#[async_trait]
impl TableProvider for TopicPartitionValueSystemTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> arrow_schema::SchemaRef {
        self.schema.clone()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let topics_filter = find_topic_name_in_filters(filters);

        let topic_partition_exec = TopicPartitionValueDiscoveryExec::new(
            self.admin.clone(),
            self.offset_registry.clone(),
            self.namespace.clone(),
            topics_filter,
        );

        apply_projection(Arc::new(topic_partition_exec), projection)
    }
}

impl std::fmt::Debug for TopicPartitionValueSystemTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TopicPartitionValueSystemTable")
            .field("namespace", &self.namespace)
            .finish()
    }
}
