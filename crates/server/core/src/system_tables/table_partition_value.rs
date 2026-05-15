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
use wings_control_plane_core::{cluster_metadata::ClusterMetadata, table_metadata::TableMetadata};
use wings_resources::NamespaceName;

use crate::{
    datafusion_helpers::apply_projection,
    system_tables::{exec::TablePartitionValueDiscoveryExec, helpers::find_table_name_in_filters},
};

/// System table for discovering partition values for tables.
pub struct TablePartitionValueSystemTable {
    cluster_meta: Arc<dyn ClusterMetadata>,
    table_metadata: Arc<dyn TableMetadata>,
    namespace: NamespaceName,
    schema: SchemaRef,
}

impl TablePartitionValueSystemTable {
    pub fn new(
        cluster_meta: Arc<dyn ClusterMetadata>,
        table_metadata: Arc<dyn TableMetadata>,
        namespace: NamespaceName,
    ) -> Self {
        Self {
            cluster_meta,
            table_metadata,
            namespace,
            schema: TablePartitionValueDiscoveryExec::schema(),
        }
    }
}

#[async_trait]
impl TableProvider for TablePartitionValueSystemTable {
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
        let tables_filter = find_table_name_in_filters(filters);

        let table_partition_exec = TablePartitionValueDiscoveryExec::new(
            self.cluster_meta.clone(),
            self.table_metadata.clone(),
            self.namespace.clone(),
            tables_filter,
        );

        apply_projection(Arc::new(table_partition_exec), projection)
    }
}

impl std::fmt::Debug for TablePartitionValueSystemTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TablePartitionValueSystemTable")
            .field("namespace", &self.namespace)
            .finish()
    }
}
