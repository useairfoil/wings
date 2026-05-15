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
    cluster_metadata::{ClusterMetadata, stream::PaginatedTopicStream},
    log_metadata::{
        LogLocation, LogMetadata,
        stream::{PaginatedLogLocationStream, PaginatedPartitionMetadataStream},
    },
};
use wings_resources::{NamespaceName, PartitionValue, TopicName};

use crate::{options::FetchOptions, system_tables::helpers::TOPIC_NAME_COLUMN};

/// Execution plan for discovering the location of topic offsets.
pub struct TopicOffsetLocationDiscoveryExec {
    cluster_meta: Arc<dyn ClusterMetadata>,
    log_meta: Arc<dyn LogMetadata>,
    namespace: NamespaceName,
    topics: Option<Vec<String>>,
    properties: PlanProperties,
    fetch_options: FetchOptions,
}

impl TopicOffsetLocationDiscoveryExec {
    pub fn new(
        cluster_meta: Arc<dyn ClusterMetadata>,
        log_meta: Arc<dyn LogMetadata>,
        namespace: NamespaceName,
        topics: Option<Vec<String>>,
        fetch_options: FetchOptions,
    ) -> Self {
        let schema = Self::schema();
        let properties = Self::compute_properties(&schema);

        Self {
            cluster_meta,
            log_meta,
            namespace,
            topics,
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
            Field::new("start_offset", DataType::UInt64, false),
            Field::new("end_offset", DataType::UInt64, false),
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

impl ExecutionPlan for TopicOffsetLocationDiscoveryExec {
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
            "TopicPartitionValueDiscoveryExec execute"
        );

        let topics = PaginatedTopicStream::new(
            self.cluster_meta.clone(),
            self.namespace.clone(),
            batch_size,
            self.topics.clone(),
        );

        let offset_registry = self.log_meta.clone();
        let topic_partition_states = topics.flat_map_unordered(None, {
            let offset_registry = offset_registry.clone();
            move |topic_result| {
                let topics = match topic_result {
                    Ok(topics) => topics,
                    Err(err) => {
                        return futures::stream::once(async { Err(DataFusionError::from(err)) })
                            .boxed();
                    }
                };

                let stream_iter = topics.into_iter().map({
                    let offset_registry = offset_registry.clone();
                    move |topic| {
                        let topic_name = topic.name;
                        PaginatedPartitionMetadataStream::new(
                            offset_registry.clone(),
                            topic_name.clone(),
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

        let log_location_options = self.fetch_options.get_log_location_options();

        let offset_locations = topic_partition_states.flat_map_unordered(None, {
            let offset_registry = offset_registry.clone();
            let log_location_options = log_location_options.clone();
            move |state_result| {
                let (topic_name, states) = match state_result {
                    Ok(v) => v,
                    Err(err) => {
                        return futures::stream::once(async { Err(err) }).boxed();
                    }
                };

                let stream_iter = states.into_iter().map({
                    let offset_registry = offset_registry.clone();
                    let log_location_options = log_location_options.clone();
                    move |state| {
                        PaginatedLogLocationStream::new(
                            offset_registry.clone(),
                            topic_name.clone(),
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
            offset_locations.chunks(batch_size).map(move |chunk| {
                let chunk = chunk.into_iter().collect::<Result<Vec<_>, _>>();
                chunk.and_then(|offsets| from_offset_location(schema.clone(), offsets.as_slice()))
            }),
        );

        Ok(Box::pin(stream))
    }
}

fn from_offset_location(
    schema: SchemaRef,
    offsets: &[(TopicName, Option<PartitionValue>, LogLocation)],
) -> Result<RecordBatch, DataFusionError> {
    if offsets.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }

    let mut tenant_arr = StringBuilder::with_capacity(offsets.len(), 0);
    let mut namespace_arr = StringBuilder::with_capacity(offsets.len(), 0);
    let mut topic_arr = StringBuilder::with_capacity(offsets.len(), 0);
    let mut partition_value_arr = StringBuilder::with_capacity(offsets.len(), 0);
    let mut start_offset_arr = UInt64Builder::with_capacity(offsets.len());
    let mut end_offset_arr = UInt64Builder::with_capacity(offsets.len());
    let mut num_rows_arr = UInt32Builder::with_capacity(offsets.len());
    let mut location_type_arr = StringBuilder::with_capacity(offsets.len(), 0);
    let mut folio_file_ref_arr = StringBuilder::with_capacity(offsets.len(), 0);
    let mut folio_offset_bytes_arr = UInt64Builder::with_capacity(offsets.len());
    let mut folio_size_bytes_arr = UInt64Builder::with_capacity(offsets.len());
    let mut lake_file_ref_arr = StringBuilder::with_capacity(offsets.len(), 0);
    let mut lake_size_bytes_arr = UInt64Builder::with_capacity(offsets.len());

    for (topic_name, partition_value, offset_location) in offsets {
        topic_arr.append_value(topic_name.id.clone());
        let namespace_name = topic_name.parent();
        namespace_arr.append_value(namespace_name.id.clone());
        let tenant_name = namespace_name.parent();
        tenant_arr.append_value(tenant_name.id.clone());
        if let Some(value) = partition_value {
            partition_value_arr.append_value(value.to_string());
        } else {
            partition_value_arr.append_null();
        }

        match offset_location.start_offset() {
            None => start_offset_arr.append_null(),
            Some(offset) => start_offset_arr.append_value(offset.offset),
        }
        match offset_location.end_offset() {
            None => end_offset_arr.append_null(),
            Some(offset) => end_offset_arr.append_value(offset.offset),
        }

        num_rows_arr.append_value(offset_location.num_rows() as _);

        match offset_location {
            LogLocation::Folio(folio) => {
                location_type_arr.append_value("folio");
                folio_file_ref_arr.append_value(&folio.file_ref);
                folio_offset_bytes_arr.append_value(folio.offset_bytes);
                folio_size_bytes_arr.append_value(folio.size_bytes);
                lake_file_ref_arr.append_null();
                lake_size_bytes_arr.append_null();
            }
            LogLocation::DataLake(file) => {
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
        Arc::new(topic_arr.finish()),
        Arc::new(partition_value_arr.finish()),
        Arc::new(start_offset_arr.finish()),
        Arc::new(end_offset_arr.finish()),
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

impl DisplayAs for TopicOffsetLocationDiscoveryExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "TopicOffsetLocationDiscoveryExec: namespace=[{}]",
            self.namespace
        )
    }
}

impl fmt::Debug for TopicOffsetLocationDiscoveryExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TopicPartitionValueDiscoveryExec")
            .field("namespace", &self.namespace)
            .finish()
    }
}
