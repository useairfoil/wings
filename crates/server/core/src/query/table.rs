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
use wings_control_plane_core::table_metadata::{
    TableLocation, TableMetadata, stream::PaginatedTableLocationStream,
};
use wings_resources::{Namespace, PartitionPosition, PartitionValue, Table};

use crate::{
    datafusion_helpers::apply_projection,
    options::SessionConfigExt,
    query::{
        exec::{DataLakeExec, FolioExec},
        helpers::{find_partition_column_value, validate_seqnum_filters},
    },
};

pub struct WingsTableProvider {
    table_metadata: Arc<dyn TableMetadata>,
    namespace: Namespace,
    table: Table,
}

impl WingsTableProvider {
    pub fn new(table_metadata: Arc<dyn TableMetadata>, namespace: Namespace, table: Table) -> Self {
        Self {
            table_metadata,
            namespace,
            table,
        }
    }

    pub fn new_provider(
        table_metadata: Arc<dyn TableMetadata>,
        namespace: Namespace,
        table: Table,
    ) -> Arc<dyn TableProvider> {
        Arc::new(Self::new(table_metadata, namespace, table))
    }
}

#[async_trait]
impl TableProvider for WingsTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn schema(&self) -> SchemaRef {
        self.table
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
            table = %self.table.name,
            ?projection,
            ?filters,
            ?limit,
            "WingsTableProvider::scan"
        );

        let seqnum_range = validate_seqnum_filters(filters)?;

        let (partition_value, partition_column) =
            if let Some(partition_column) = self.table.partition_field() {
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

        let row_location_stream = PaginatedTableLocationStream::new_in_seqnum_range(
            self.table_metadata.clone(),
            self.table.name.clone(),
            partition_value,
            seqnum_range,
            fetch_options.get_table_location_options(),
        );

        let locations = row_location_stream.try_collect::<Vec<_>>().await?;

        let object_store_url = self.namespace.object_store.wings_object_store_url()?;

        let output_schema = self.schema();
        let folio_schema = self.table.arrow_schema_without_partition_field();
        let datalake_schema = self
            .table
            .arrow_schema_with_metadata(PartitionPosition::Skip)
            .map_err(|err| DataFusionError::External(err.into()))?;

        let locations_exec = locations
            .into_iter()
            .map(|(_, partition_value, location)| match location {
                TableLocation::Folio(folio) => {
                    debug!(table = %self.table.name, partition = ?partition_value, ?folio, "WingsTableProvider::scan add folio");
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
                TableLocation::DataLake(file) => {
                    debug!(table = %self.table.name, partition = ?partition_value, ?file, "WingsTableProvider::scan add datalake file");
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

        let exec = match locations_exec.as_slice() {
            [] => Arc::new(EmptyExec::new(output_schema)),
            [exec] => exec.clone(),
            _ => UnionExec::try_new(locations_exec)?,
        };

        apply_projection(exec, projection)
    }
}

impl std::fmt::Debug for WingsTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WingsTableProvider")
            .field("table", &self.table.name)
            .finish()
    }
}
