use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    catalog::{Session, TableProvider},
    common::arrow::datatypes::SchemaRef,
    datasource::TableType,
    error::DataFusionError,
    logical_expr::TableProviderFilterPushDown,
    physical_plan::ExecutionPlan,
    prelude::Expr,
};
use wings_control_plane::{cluster_metadata::ClusterMetadata, resources::NamespaceName};

use crate::datafusion_helpers::apply_projection;

use super::{exec::TopicDiscoveryExec, helpers::find_topic_name_in_filters};

pub struct TopicSystemTable {
    cluster_meta: Arc<dyn ClusterMetadata>,
    namespace: NamespaceName,
    schema: SchemaRef,
}

impl TopicSystemTable {
    pub fn new(cluster_meta: Arc<dyn ClusterMetadata>, namespace: NamespaceName) -> Self {
        Self {
            cluster_meta,
            namespace,
            schema: TopicDiscoveryExec::schema(),
        }
    }
}

#[async_trait]
impl TableProvider for TopicSystemTable {
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
        let topic_filters = find_topic_name_in_filters(filters);

        let topic_exec = TopicDiscoveryExec::new(
            self.cluster_meta.clone(),
            self.namespace.clone(),
            topic_filters,
        );
        let topic_exec = Arc::new(topic_exec);

        apply_projection(topic_exec, projection)
    }
}

impl std::fmt::Debug for TopicSystemTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TopicTable")
            .field("namespace", &self.namespace)
            .finish()
    }
}
