use std::{any::Any, sync::Arc};

use arrow::array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::{
    catalog::{Session, TableProvider},
    datasource::TableType,
    error::DataFusionError,
    logical_expr::TableProviderFilterPushDown,
    physical_plan::ExecutionPlan,
    prelude::Expr,
};
use wings_metadata_core::admin::{Admin, NamespaceName};

use crate::system_tables::exec::TopicDiscoveryExec;

use super::helpers::TOPIC_NAME_COLUMN;

pub struct TopicOffsetLocationSystemTable {
    admin: Arc<dyn Admin>,
    namespace: NamespaceName,
    schema: SchemaRef,
}

impl TopicOffsetLocationSystemTable {
    pub fn new(admin: Arc<dyn Admin>, namespace: NamespaceName) -> Self {
        Self {
            admin,
            namespace,
            schema: topic_offset_location_schema(),
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
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let schema = self.schema();

        todo!();
    }
}

impl std::fmt::Debug for TopicOffsetLocationSystemTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TopicOffsetLocationTable")
            .field("namespace", &self.namespace)
            .finish()
    }
}

fn topic_offset_location_schema() -> SchemaRef {
    let fields = vec![
        Field::new("tenant", DataType::Utf8View, false),
        Field::new("namespace", DataType::Utf8View, false),
        Field::new(TOPIC_NAME_COLUMN, DataType::Utf8View, false),
        Field::new("partition_value", DataType::Utf8View, false),
        Field::new("start_offset", DataType::UInt64, false),
        Field::new("end_offset", DataType::UInt64, false),
        Field::new("location_type", DataType::Utf8View, false),
        // Folio-specific columns
        Field::new("folio_file_ref", DataType::Utf8View, true),
        Field::new("folio_offset_bytes", DataType::UInt64, true),
        Field::new("folio_size_bytes", DataType::UInt64, true),
    ];

    Arc::new(Schema::new(fields))
}
