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
use opentelemetry_sdk::metrics::{data::ResourceMetrics, reader::MetricReader};
use wings_observability::MetricsExporter;

use crate::{datafusion_helpers::apply_projection, system_tables::exec::MetricsExec};

pub struct MetricsSystemTable {
    exporter: MetricsExporter,
    schema: SchemaRef,
}

impl MetricsSystemTable {
    pub fn new(exporter: MetricsExporter) -> Self {
        Self {
            exporter,
            schema: MetricsExec::schema(),
        }
    }
}

#[async_trait]
impl TableProvider for MetricsSystemTable {
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
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let mut rm = ResourceMetrics::default();
        self.exporter
            .collect(&mut rm)
            .map_err(|err| DataFusionError::External(err.into()))?;

        let metrics_exec: Arc<_> = MetricsExec::new(rm).into();

        apply_projection(metrics_exec, projection)
    }
}

impl std::fmt::Debug for MetricsSystemTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricsSystemTable").finish()
    }
}
