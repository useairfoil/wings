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
use wings_metadata_core::{
    admin::{Admin, NamespaceName},
    offset_registry::OffsetRegistry,
};

use crate::datafusion_helpers::apply_projection;

use super::{exec::TopicOffsetLocationDiscoveryExec, helpers::find_topic_name_in_filters};

pub struct TopicOffsetLocationSystemTable {
    admin: Arc<dyn Admin>,
    offset_registry: Arc<dyn OffsetRegistry>,
    namespace: NamespaceName,
    schema: SchemaRef,
}

impl TopicOffsetLocationSystemTable {
    pub fn new(
        admin: Arc<dyn Admin>,
        offset_registry: Arc<dyn OffsetRegistry>,
        namespace: NamespaceName,
    ) -> Self {
        Self {
            admin,
            offset_registry,
            namespace,
            schema: TopicOffsetLocationDiscoveryExec::schema(),
        }
    }
}

#[async_trait]
impl TableProvider for TopicOffsetLocationSystemTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
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

        let topic_offset_exec = TopicOffsetLocationDiscoveryExec::new(
            self.admin.clone(),
            self.offset_registry.clone(),
            self.namespace.clone(),
            topics_filter,
        );

        apply_projection(Arc::new(topic_offset_exec), projection)
    }
}

impl std::fmt::Debug for TopicOffsetLocationSystemTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TopicOffsetLocationTable")
            .field("namespace", &self.namespace)
            .finish()
    }
}
