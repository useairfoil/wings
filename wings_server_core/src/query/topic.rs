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
use tracing::debug;
use wings_metadata_core::admin::Topic;

#[derive(Debug)]
pub struct TopicTableProvider {
    topic: Topic,
}

impl TopicTableProvider {
    pub fn new(topic: Topic) -> Self {
        Self { topic }
    }

    pub fn new_provider(topic: Topic) -> Arc<dyn TableProvider> {
        Arc::new(Self::new(topic))
    }
}

#[async_trait]
impl TableProvider for TopicTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn schema(&self) -> SchemaRef {
        self.topic.schema_with_offset_column()
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
        debug!(?projection, ?filters, ?limit, "TopicTableProvider::scan");
        let _filters = filters.to_vec();

        todo!();
    }
}
