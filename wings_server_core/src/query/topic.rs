use std::{any::Any, future, sync::Arc};

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    catalog::{Session, TableProvider, memory::DataSourceExec},
    datasource::{
        TableType,
        listing::PartitionedFile,
        physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource},
    },
    error::DataFusionError,
    execution::object_store::ObjectStoreUrl,
    logical_expr::TableProviderFilterPushDown,
    physical_plan::ExecutionPlan,
    prelude::Expr,
};
use futures::{StreamExt, TryStreamExt};
use tracing::debug;
use wings_metadata_core::{
    admin::{Namespace, Topic},
    offset_registry::{OffsetRegistry, OffsetRegistryError, PaginatedOffsetLocationStream},
    partition::PartitionValue,
};

use crate::query::helpers::{find_partition_column_value, validate_offset_filters};

pub struct TopicTableProvider {
    offset_registry: Arc<dyn OffsetRegistry>,
    namespace: Namespace,
    topic: Topic,
}

impl TopicTableProvider {
    pub fn new(
        offset_registry: Arc<dyn OffsetRegistry>,
        namespace: Namespace,
        topic: Topic,
    ) -> Self {
        Self {
            offset_registry,
            namespace,
            topic,
        }
    }

    pub fn new_provider(
        offset_registry: Arc<dyn OffsetRegistry>,
        namespace: Namespace,
        topic: Topic,
    ) -> Arc<dyn TableProvider> {
        Arc::new(Self::new(offset_registry, namespace, topic))
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

        let offset_range = validate_offset_filters(filters)?;

        let partition_value = if let Some(partition_column) = self.topic.partition_column() {
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

            Some(partition_value)
        } else {
            None
        };

        let offset_location_stream = PaginatedOffsetLocationStream::new_in_range(
            self.offset_registry.clone(),
            self.topic.name.clone(),
            partition_value,
            offset_range,
        );

        let locations = offset_location_stream.try_collect::<Vec<_>>().await?;

        let object_store_url = self
            .namespace
            .default_object_store_config
            .to_object_store_url()?;
        let file_source = Arc::new(ParquetSource::default());
        let file_group = FileGroup::new(
            locations
                .into_iter()
                .map(|(_, _, location)| {
                    let loc = location.as_folio().unwrap();
                    PartitionedFile::new(loc.file_ref.clone(), loc.size_bytes)
                })
                .collect(),
        );
        let config = FileScanConfigBuilder::new(
            object_store_url,
            self.topic.schema_without_partition_column(),
            file_source,
        )
        .with_limit(limit)
        // .with_projection(projection.cloned())
        .with_file_group(file_group)
        .build();

        Ok(DataSourceExec::from_data_source(config))
    }
}

impl std::fmt::Debug for TopicTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TopicTableProvider")
            .field("topic", &self.topic.name)
            .finish()
    }
}
