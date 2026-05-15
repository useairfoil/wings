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
use wings_control_plane_core::cluster_metadata::ClusterMetadata;
use wings_resources::NamespaceName;

use super::{exec::TableDiscoveryExec, helpers::find_table_name_in_filters};
use crate::datafusion_helpers::apply_projection;

pub struct TableSystemTable {
    cluster_meta: Arc<dyn ClusterMetadata>,
    namespace: NamespaceName,
    schema: SchemaRef,
}

impl TableSystemTable {
    pub fn new(cluster_meta: Arc<dyn ClusterMetadata>, namespace: NamespaceName) -> Self {
        Self {
            cluster_meta,
            namespace,
            schema: TableDiscoveryExec::schema(),
        }
    }
}

#[async_trait]
impl TableProvider for TableSystemTable {
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
        let table_filters = find_table_name_in_filters(filters);

        let table_exec = TableDiscoveryExec::new(
            self.cluster_meta.clone(),
            self.namespace.clone(),
            table_filters,
        );
        let table_exec = Arc::new(table_exec);

        apply_projection(table_exec, projection)
    }
}

impl std::fmt::Debug for TableSystemTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TopicTable")
            .field("namespace", &self.namespace)
            .finish()
    }
}
