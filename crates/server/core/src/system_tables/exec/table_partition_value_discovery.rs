use std::{any::Any, fmt, sync::Arc, time::SystemTime};

use datafusion::{
    common::arrow::{
        array::{ArrayRef, RecordBatch, StringBuilder, TimestampMillisecondBuilder, UInt64Builder},
        datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
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
    table_metadata::{PartitionMetadata, TableMetadata, stream::PaginatedPartitionMetadataStream},
};
use wings_resources::{NamespaceName, TableName};

use crate::system_tables::helpers::TOPIC_NAME_COLUMN;

/// Execution plan for discovering partition values for tables.
pub struct TablePartitionValueDiscoveryExec {
    cluster_meta: Arc<dyn ClusterMetadata>,
    table_metadata: Arc<dyn TableMetadata>,
    namespace: NamespaceName,
    tables: Option<Vec<String>>,
    properties: PlanProperties,
}

impl TablePartitionValueDiscoveryExec {
    pub fn new(
        cluster_meta: Arc<dyn ClusterMetadata>,
        table_metadata: Arc<dyn TableMetadata>,
        namespace: NamespaceName,
        tables: Option<Vec<String>>,
    ) -> Self {
        let schema = Self::schema();
        let properties = Self::compute_properties(&schema);

        Self {
            cluster_meta,
            table_metadata,
            namespace,
            tables,
            properties,
        }
    }

    pub fn schema() -> SchemaRef {
        let fields = vec![
            Field::new("tenant", DataType::Utf8, false),
            Field::new("namespace", DataType::Utf8, false),
            Field::new(TOPIC_NAME_COLUMN, DataType::Utf8, false),
            Field::new("partition_value", DataType::Utf8, true),
            Field::new("next_seqnum", DataType::UInt64, false),
            Field::new(
                "latest_timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
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

impl ExecutionPlan for TablePartitionValueDiscoveryExec {
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

        // Here we take the stream of tables and for each table, we create a stream of partition values.
        // Notice that the tables are paginated, so we need to further flatten the iterator of streams.
        let table_metadata = self.table_metadata.clone();
        let values = tables.flat_map_unordered(None, move |table_result| {
            let tables = match table_result {
                Ok(tables) => tables,
                Err(err) => {
                    return futures::stream::once(async { Err(DataFusionError::from(err)) })
                        .boxed();
                }
            };

            let stream_iter = tables.into_iter().map({
                let table_metadata_c = table_metadata.clone();
                move |table| {
                    let table_name = table.name;
                    PaginatedPartitionMetadataStream::new(
                        table_metadata_c.clone(),
                        table_name.clone(),
                        batch_size,
                    )
                    .map_err(DataFusionError::from)
                }
            });
            futures::stream::iter(stream_iter)
                .flatten_unordered(None)
                .boxed()
        });

        let schema = self.schema();
        let stream = RecordBatchStreamAdapter::new(
            self.schema(),
            values.map(move |result| {
                result.and_then(|(table_name, values)| {
                    from_partition_values(schema.clone(), table_name.clone(), values)
                })
            }),
        );

        Ok(Box::pin(stream))
    }
}

fn from_partition_values(
    schema: SchemaRef,
    table_name: TableName,
    states: Vec<PartitionMetadata>,
) -> Result<RecordBatch, DataFusionError> {
    if states.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }

    let mut tenant_arr = StringBuilder::with_capacity(states.len(), 0);
    let mut namespace_arr = StringBuilder::with_capacity(states.len(), 0);
    let mut table_arr = StringBuilder::with_capacity(states.len(), 0);
    let mut partition_value_arr = StringBuilder::with_capacity(states.len(), 0);
    let mut next_seqnum_arr = UInt64Builder::with_capacity(states.len());
    let mut latest_timestamp_arr = TimestampMillisecondBuilder::with_capacity(states.len());

    for state in states {
        table_arr.append_value(table_name.id.clone());
        let namespace_name = table_name.parent();
        namespace_arr.append_value(namespace_name.id.clone());
        let tenant_name = namespace_name.parent();
        tenant_arr.append_value(tenant_name.id.clone());
        if let Some(value) = state.partition_value {
            partition_value_arr.append_value(value.to_string());
        } else {
            partition_value_arr.append_null();
        }
        next_seqnum_arr.append_value(state.end_seqnum.seqnum);
        // TODO: refactor so it's easier to add timestamps arrays
        let latest_timestamp = state
            .end_seqnum
            .timestamp
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|err| {
                DataFusionError::Execution(format!(
                    "failed to get duration since UNIX_EPOCH: {}",
                    err
                ))
            })?;
        latest_timestamp_arr.append_value(latest_timestamp.as_millis() as _);
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(tenant_arr.finish()),
        Arc::new(namespace_arr.finish()),
        Arc::new(table_arr.finish()),
        Arc::new(partition_value_arr.finish()),
        Arc::new(next_seqnum_arr.finish()),
        Arc::new(latest_timestamp_arr.finish()),
    ];

    RecordBatch::try_new(schema, columns).map_err(DataFusionError::from)
}

impl DisplayAs for TablePartitionValueDiscoveryExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "TablePartitionValueDiscoveryExec: namespace=[{}]",
            self.namespace
        )
    }
}

impl fmt::Debug for TablePartitionValueDiscoveryExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TablePartitionValueDiscoveryExec")
            .field("namespace", &self.namespace)
            .finish()
    }
}
