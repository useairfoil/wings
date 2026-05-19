use std::{any::Any, fmt, sync::Arc};

use datafusion::{
    common::arrow::{
        array::{ArrayRef, RecordBatch, StringBuilder, UInt32Builder, UInt64Builder},
        datatypes::{DataType, Field, Schema, SchemaRef},
    },
    error::{DataFusionError, Result},
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
    },
};
use futures::{StreamExt, TryStreamExt};
use tracing::debug;
use wings_control_plane_core::{
    cluster_metadata::{ClusterMetadata, stream::PaginatedTableStream},
    table_metadata::{
        TableLocation, TableMetadata,
        stream::{PaginatedTableLocationStream, PaginatedPartitionMetadataStream},
    },
};
use wings_resources::{NamespaceName, PartitionValue, TableName};

use crate::{options::FetchOptions, system_tables::helpers::TOPIC_NAME_COLUMN};

/// Execution plan for discovering the location of table rows.
pub struct TableRowLocationDiscoveryExec {
    cluster_meta: Arc<dyn ClusterMetadata>,
    table_metadata: Arc<dyn TableMetadata>,
    namespace: NamespaceName,
    tables: Option<Vec<String>>,
    properties: PlanProperties,
    fetch_options: FetchOptions,
}

impl TableRowLocationDiscoveryExec {
    pub fn new(
        cluster_meta: Arc<dyn ClusterMetadata>,
        table_metadata: Arc<dyn TableMetadata>,
        namespace: NamespaceName,
        tables: Option<Vec<String>>,
        fetch_options: FetchOptions,
    ) -> Self {
        let schema = Self::schema();
        let properties = Self::compute_properties(&schema);

        Self {
            cluster_meta,
            table_metadata,
            namespace,
            tables,
            properties,
            fetch_options,
        }
    }

    pub fn schema() -> SchemaRef {
        let fields = vec![
            Field::new("tenant", DataType::Utf8, false),
            Field::new("namespace", DataType::Utf8, false),
            Field::new(TOPIC_NAME_COLUMN, DataType::Utf8, false),
            Field::new("partition_value", DataType::Utf8, true),
            // TODO: add start and end timestamp
            Field::new("start_seqnum", DataType::UInt64, false),
            Field::new("end_seqnum", DataType::UInt64, false),
            Field::new("num_rows", DataType::UInt32, false),
            Field::new("location_type", DataType::Utf8, false),
            // Folio-specific columns
            Field::new("folio_file_ref", DataType::Utf8, true),
            Field::new("folio_offset_bytes", DataType::UInt64, true),
            Field::new("folio_size_bytes", DataType::UInt64, true),
            // DataLake-specific columns
            Field::new("datalake_file_ref", DataType::Utf8, true),
            Field::new("datalake_size_bytes", DataType::UInt64, true),
        ];

        Arc::new(Schema::new(fields))
    }

    fn compute_properties(schema: &SchemaRef) -> PlanProperties {
        let eq_properties = EquivalenceProperties::new(schema.clone());
        let partitioning = Partitioning::UnknownPartitioning(1);
        let emission_type = EmissionType::Incremental;
        let boundedness = Boundedness::Bounded;

        PlanProperties::new(eq_properties, partitioning, emission_type, boundedness)
    }
}

impl ExecutionPlan for TableRowLocationDiscoveryExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Self::schema()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        assert_eq!(partition, 0);
        let batch_size = context.session_config().batch_size();
        debug!(
            namespace = %self.namespace,
            "TablePartitionValueDiscoveryExec execute"
        );

        let tables = PaginatedTableStream::new(
            self.cluster_meta.clone(),
            self.namespace.clone(),
            batch_size,
            self.tables.clone(),
        );

        let table_metadata = self.table_metadata.clone();
        let table_partition_states = tables.flat_map_unordered(None, {
            let table_metadata_c = table_metadata.clone();
            move |table_result| {
                let tables = match table_result {
                    Ok(tables) => tables,
                    Err(err) => {
                        return futures::stream::once(async { Err(DataFusionError::from(err)) })
                            .boxed();
                    }
                };

                let stream_iter = tables.into_iter().map({
                    let table_metadata_c2 = table_metadata_c.clone();
                    move |table| {
                        let table_name = table.name;
                        PaginatedPartitionMetadataStream::new(
                            table_metadata_c2.clone(),
                            table_name.clone(),
                            batch_size,
                        )
                        .map_err(DataFusionError::from)
                    }
                });
                futures::stream::iter(stream_iter)
                    .flatten_unordered(None)
                    .boxed()
            }
        });

        let log_location_options = self.fetch_options.get_table_location_options();

        let row_locations = table_partition_states.flat_map_unordered(None, {
            let table_metadata_c = table_metadata.clone();
            let log_location_options = log_location_options.clone();
            move |state_result| {
                let (table_name, states) = match state_result {
                    Ok(v) => v,
                    Err(err) => {
                        return futures::stream::once(async { Err(err) }).boxed();
                    }
                };

                let stream_iter = states.into_iter().map({
                    let table_metadata_c2 = table_metadata_c.clone();
                    let log_location_options = log_location_options.clone();
                    move |state| {
                        PaginatedTableLocationStream::new(
                            table_metadata_c2.clone(),
                            table_name.clone(),
                            state.partition_value,
                            log_location_options.clone(),
                        )
                        .map_err(DataFusionError::from)
                    }
                });
                futures::stream::iter(stream_iter)
                    .flatten_unordered(None)
                    .boxed()
            }
        });

        let schema = self.schema();
        let stream = RecordBatchStreamAdapter::new(
            schema.clone(),
            row_locations.chunks(batch_size).map(move |chunk| {
                let chunk = chunk.into_iter().collect::<Result<Vec<_>, _>>();
                chunk.and_then(|offsets| from_row_location(schema.clone(), offsets.as_slice()))
            }),
        );

        Ok(Box::pin(stream))
    }
}

fn from_row_location(
    schema: SchemaRef,
    offsets: &[(TableName, Option<PartitionValue>, TableLocation)],
) -> Result<RecordBatch, DataFusionError> {
    if offsets.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }

    let mut tenant_arr = StringBuilder::with_capacity(offsets.len(), 0);
    let mut namespace_arr = StringBuilder::with_capacity(offsets.len(), 0);
    let mut table_arr = StringBuilder::with_capacity(offsets.len(), 0);
    let mut partition_value_arr = StringBuilder::with_capacity(offsets.len(), 0);
    let mut start_seqnum_arr = UInt64Builder::with_capacity(offsets.len());
    let mut end_seqnum_arr = UInt64Builder::with_capacity(offsets.len());
    let mut num_rows_arr = UInt32Builder::with_capacity(offsets.len());
    let mut location_type_arr = StringBuilder::with_capacity(offsets.len(), 0);
    let mut folio_file_ref_arr = StringBuilder::with_capacity(offsets.len(), 0);
    let mut folio_offset_bytes_arr = UInt64Builder::with_capacity(offsets.len());
    let mut folio_size_bytes_arr = UInt64Builder::with_capacity(offsets.len());
    let mut lake_file_ref_arr = StringBuilder::with_capacity(offsets.len(), 0);
    let mut lake_size_bytes_arr = UInt64Builder::with_capacity(offsets.len());

    for (table_name, partition_value, row_location) in offsets {
        table_arr.append_value(table_name.id.clone());
        let namespace_name = table_name.parent();
        namespace_arr.append_value(namespace_name.id.clone());
        let tenant_name = namespace_name.parent();
        tenant_arr.append_value(tenant_name.id.clone());
        if let Some(value) = partition_value {
            partition_value_arr.append_value(value.to_string());
        } else {
            partition_value_arr.append_null();
        }

        match row_location.start_seqnum() {
            None => start_seqnum_arr.append_null(),
            Some(seqnum) => start_seqnum_arr.append_value(seqnum.seqnum),
        }
        match row_location.end_seqnum() {
            None => end_seqnum_arr.append_null(),
            Some(seqnum) => end_seqnum_arr.append_value(seqnum.seqnum),
        }

        num_rows_arr.append_value(row_location.num_rows() as _);

        match row_location {
            TableLocation::Folio(folio) => {
                location_type_arr.append_value("folio");
                folio_file_ref_arr.append_value(&folio.file_ref);
                folio_offset_bytes_arr.append_value(folio.offset_bytes);
                folio_size_bytes_arr.append_value(folio.size_bytes);
                lake_file_ref_arr.append_null();
                lake_size_bytes_arr.append_null();
            }
            TableLocation::DataLake(file) => {
                location_type_arr.append_value("datalake");
                folio_file_ref_arr.append_null();
                folio_offset_bytes_arr.append_null();
                folio_size_bytes_arr.append_null();
                lake_file_ref_arr.append_value(&file.file_ref);
                lake_size_bytes_arr.append_value(file.size_bytes);
            }
        }
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(tenant_arr.finish()),
        Arc::new(namespace_arr.finish()),
        Arc::new(table_arr.finish()),
        Arc::new(partition_value_arr.finish()),
        Arc::new(start_seqnum_arr.finish()),
        Arc::new(end_seqnum_arr.finish()),
        Arc::new(num_rows_arr.finish()),
        Arc::new(location_type_arr.finish()),
        Arc::new(folio_file_ref_arr.finish()),
        Arc::new(folio_offset_bytes_arr.finish()),
        Arc::new(folio_size_bytes_arr.finish()),
        Arc::new(lake_file_ref_arr.finish()),
        Arc::new(lake_size_bytes_arr.finish()),
    ];

    RecordBatch::try_new(schema, columns).map_err(DataFusionError::from)
}

impl DisplayAs for TableRowLocationDiscoveryExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "TableRowLocationDiscoveryExec: namespace=[{}]",
            self.namespace
        )
    }
}

impl fmt::Debug for TableRowLocationDiscoveryExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TablePartitionValueDiscoveryExec")
            .field("namespace", &self.namespace)
            .finish()
    }
}
