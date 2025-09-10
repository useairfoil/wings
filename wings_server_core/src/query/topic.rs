use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    catalog::{Session, TableProvider},
    common::arrow::datatypes::SchemaRef,
    datasource::TableType,
    error::DataFusionError,
    logical_expr::TableProviderFilterPushDown,
    physical_plan::{ExecutionPlan, empty::EmptyExec, union::UnionExec},
    prelude::Expr,
};
use futures::TryStreamExt;
use tracing::debug;
use wings_control_plane::{
    log_metadata::{LogLocation, LogMetadata, stream::PaginatedLogLocationStream},
    resources::{Namespace, PartitionValue, Topic},
};

use crate::query::{
    exec::FolioExec,
    helpers::{find_partition_column_value, validate_offset_filters},
};

pub struct TopicTableProvider {
    log_meta: Arc<dyn LogMetadata>,
    namespace: Namespace,
    topic: Topic,
}

impl TopicTableProvider {
    pub fn new(log_meta: Arc<dyn LogMetadata>, namespace: Namespace, topic: Topic) -> Self {
        Self {
            log_meta,
            namespace,
            topic,
        }
    }

    pub fn new_provider(
        log_meta: Arc<dyn LogMetadata>,
        namespace: Namespace,
        topic: Topic,
    ) -> Arc<dyn TableProvider> {
        Arc::new(Self::new(log_meta, namespace, topic))
    }

    pub fn output_schema(topic_schema: SchemaRef) -> SchemaRef {
        todo!();
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
        Self::output_schema(self.topic.schema())
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

        let offset_range = validate_offset_filters(filters)?;

        let (partition_value, partition_column) =
            if let Some(partition_column) = self.topic.partition_field() {
                let partition_value: PartitionValue =
                    find_partition_column_value(partition_column.name(), filters)?
                        .try_into()
                        .map_err(|err| {
                            DataFusionError::Plan(format!(
                                "Failed to parse partition column value: {err}"
                            ))
                        })?;

                if partition_column.data_type() != &partition_value.data_type() {
                    return Err(DataFusionError::Plan(format!(
                        "Partition column data type mismatch. Have {:?}, expected {:?}",
                        partition_value.data_type(),
                        partition_column.data_type()
                    )));
                }

                (Some(partition_value), Some(partition_column.clone()))
            } else {
                (None, None)
            };

        let offset_location_stream = PaginatedLogLocationStream::new_in_offset_range(
            self.log_meta.clone(),
            self.topic.name.clone(),
            partition_value,
            offset_range,
        );

        let locations = offset_location_stream.try_collect::<Vec<_>>().await?;

        let object_store_url = self
            .namespace
            .default_object_store_config
            .to_object_store_url()?;

        let schema = self.schema();
        let file_schema = self.topic.schema_without_partition_field();
        let locations_exec = locations
            .into_iter()
            .map(|(_, partition_value, location)| match location {
                LogLocation::Folio(folio) => FolioExec::try_new_exec(
                    schema.clone(),
                    file_schema.clone(),
                    partition_value,
                    partition_column.clone(),
                    folio,
                    object_store_url.clone(),
                ),
            })
            .collect::<Result<Vec<_>, DataFusionError>>()?;

        match locations_exec.as_slice() {
            [] => Ok(Arc::new(EmptyExec::new(schema))),
            [exec] => Ok(exec.clone()),
            _ => Ok(Arc::new(UnionExec::new(locations_exec))),
        }
    }
}

impl std::fmt::Debug for TopicTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TopicTableProvider")
            .field("topic", &self.topic.name)
            .finish()
    }
}
