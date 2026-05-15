// Inspired by the IoxSystemTable trait in influxdb3_core.
//
// Copyright 2025 InfluxData Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    catalog::{Session, TableProvider, memory::MemorySourceConfig},
    common::arrow::{array::RecordBatch, datatypes::SchemaRef},
    datasource::TableType,
    error::DataFusionError,
    logical_expr::TableProviderFilterPushDown,
    physical_plan::ExecutionPlan,
    prelude::Expr,
};

#[async_trait]
pub trait SystemTable: std::fmt::Debug + Send + Sync {
    fn schema(&self) -> SchemaRef;

    async fn scan(
        &self,
        filters: Vec<Expr>,
        limit: Option<usize>,
    ) -> Result<RecordBatch, DataFusionError>;
}

#[derive(Debug)]
pub struct SystemTableProvider<T: SystemTable> {
    inner: Arc<T>,
}

impl<T: SystemTable> SystemTableProvider<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

#[async_trait]
impl<T: SystemTable + 'static> TableProvider for SystemTableProvider<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
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

        let batch = self.inner.scan(filters.to_vec(), limit).await?;

        let exec_plan =
            MemorySourceConfig::try_new_exec(&[vec![batch]], schema, projection.cloned())?;

        Ok(exec_plan)
    }
}
