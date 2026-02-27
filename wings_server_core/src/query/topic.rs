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
use wings_control_plane_core::log_metadata::{
    LogLocation, LogMetadata, stream::PaginatedLogLocationStream,
};
use wings_resources::{Namespace, PartitionPosition, PartitionValue, Topic};

use crate::{
    options::SessionConfigExt,
    query::{
        exec::{DataLakeExec, FolioExec},
        helpers::{find_partition_column_value, validate_offset_filters},
    },
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
        self.topic
            .arrow_schema_with_metadata(PartitionPosition::Last)
            .expect("schema should be valid")
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        debug!(
            topic = %self.topic.name,
            ?projection,
            ?filters,
            ?limit,
            "TopicTableProvider::scan"
        );

        let offset_range = validate_offset_filters(filters)?;

        let (partition_value, partition_column) =
            if let Some(partition_column) = self.topic.partition_field() {
                let partition_value: PartitionValue =
                    find_partition_column_value(&partition_column.name, filters)?
                        .try_into()
                        .map_err(|err| {
                            DataFusionError::Plan(format!(
                                "Failed to parse partition column value: {err}"
                            ))
                        })?;

                if partition_column.data_type != partition_value.data_type() {
                    return Err(DataFusionError::Plan(format!(
                        "Partition column data type mismatch. Have {:?}, expected {:?}",
                        partition_value.data_type(),
                        partition_column.data_type
                    )));
                }

                (Some(partition_value), Some(partition_column.clone()))
            } else {
                (None, None)
            };

        let fetch_options = state.config().fetch_options();

        let offset_location_stream = PaginatedLogLocationStream::new_in_offset_range(
            self.log_meta.clone(),
            self.topic.name.clone(),
            partition_value,
            offset_range,
            fetch_options.get_log_location_options(),
        );

        let locations = offset_location_stream.try_collect::<Vec<_>>().await?;

        let object_store_url = self.namespace.object_store.wings_object_store_url()?;

        let output_schema = self.schema();
        let folio_schema = self.topic.arrow_schema_without_partition_field();
        let datalake_schema = self
            .topic
            .arrow_schema_with_metadata(PartitionPosition::Skip)
            .map_err(|err| DataFusionError::External(err.into()))?;

        let locations_exec = locations
            .into_iter()
            .map(|(_, partition_value, location)| match location {
                LogLocation::Folio(folio) => {
                    debug!(topic = %self.topic.name, partition = ?partition_value, ?folio, "TopicTableProvider::scan add folio");
                    FolioExec::try_new_exec(
                        state,
                        output_schema.clone(),
                        folio_schema.clone(),
                        partition_value,
                        partition_column.clone(),
                        folio,
                        object_store_url.clone(),
                    )
                }
                LogLocation::DataLake(file) => {
                    debug!(topic = %self.topic.name, partition = ?partition_value, ?file, "TopicTableProvider::scan add datalake file");
                    // Notice:
                    // file and output schema are the same.
                    // partition value is included in file.
                    DataLakeExec::try_new_exec(
                        state,
                        output_schema.clone(),
                        datalake_schema.clone(),
                        partition_value,
                        file,
                        object_store_url.clone(),
                    )
                }
            })
            .collect::<Result<Vec<_>, DataFusionError>>()?;

        match locations_exec.as_slice() {
            [] => Ok(Arc::new(EmptyExec::new(output_schema))),
            [exec] => Ok(exec.clone()),
            _ => Ok(UnionExec::try_new(locations_exec)?),
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
