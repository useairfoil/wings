use std::{any::Any, fmt, sync::Arc, time::SystemTime};

use datafusion::{
    common::arrow::{
        array::{
            ArrayRef, RecordBatch, StringViewBuilder, TimestampMillisecondBuilder, UInt64Builder,
        },
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
use wings_metadata_core::{
    admin::{Admin, NamespaceName, PaginatedTopicStream, TopicName},
    offset_registry::{OffsetRegistry, PaginatedPartitionStateStream, PartitionValueState},
};

use crate::system_tables::helpers::TOPIC_NAME_COLUMN;

/// Execution plan for discovering partition values for topics.
pub struct TopicPartitionValueDiscoveryExec {
    admin: Arc<dyn Admin>,
    offset_registry: Arc<dyn OffsetRegistry>,
    namespace: NamespaceName,
    topics: Option<Vec<String>>,
    properties: PlanProperties,
}

impl TopicPartitionValueDiscoveryExec {
    pub fn new(
        admin: Arc<dyn Admin>,
        offset_registry: Arc<dyn OffsetRegistry>,
        namespace: NamespaceName,
        topics: Option<Vec<String>>,
    ) -> Self {
        let schema = Self::schema();
        let properties = Self::compute_properties(&schema);

        Self {
            admin,
            offset_registry,
            namespace,
            topics,
            properties,
        }
    }

    pub fn schema() -> SchemaRef {
        let fields = vec![
            Field::new("tenant", DataType::Utf8View, false),
            Field::new("namespace", DataType::Utf8View, false),
            Field::new(TOPIC_NAME_COLUMN, DataType::Utf8View, false),
            Field::new("partition_value", DataType::Utf8View, true),
            Field::new("next_offset", DataType::UInt64, false),
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

impl ExecutionPlan for TopicPartitionValueDiscoveryExec {
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
            self.admin.clone(),
            self.namespace.clone(),
            batch_size,
            self.topics.clone(),
        );

        // Here we take the stream of topics and for each topic, we create a stream of partition values.
        // Notice that the topics are paginated, so we need to further flatten the iterator of streams.
        let offset_registry = self.offset_registry.clone();
        let values = topics.flat_map_unordered(None, move |topic_result| {
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
                    PaginatedPartitionStateStream::new(
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
        });

        let schema = self.schema();
        let stream = RecordBatchStreamAdapter::new(
            self.schema(),
            values.map(move |result| {
                result.and_then(|(topic_name, values)| {
                    from_partition_values(schema.clone(), topic_name.clone(), values)
                })
            }),
        );

        Ok(Box::pin(stream))
    }
}

fn from_partition_values(
    schema: SchemaRef,
    topic_name: TopicName,
    states: Vec<PartitionValueState>,
) -> Result<RecordBatch, DataFusionError> {
    if states.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }

    let mut tenant_arr = StringViewBuilder::with_capacity(states.len());
    let mut namespace_arr = StringViewBuilder::with_capacity(states.len());
    let mut topic_arr = StringViewBuilder::with_capacity(states.len());
    let mut partition_value_arr = StringViewBuilder::with_capacity(states.len());
    let mut next_offset_arr = UInt64Builder::with_capacity(states.len());
    let mut latest_timestamp_arr = TimestampMillisecondBuilder::with_capacity(states.len());

    for state in states {
        topic_arr.append_value(topic_name.id.clone());
        let namespace_name = topic_name.parent();
        namespace_arr.append_value(namespace_name.id.clone());
        let tenant_name = namespace_name.parent();
        tenant_arr.append_value(tenant_name.id.clone());
        if let Some(value) = state.partition_value {
            partition_value_arr.append_value(value.to_string());
        } else {
            partition_value_arr.append_null();
        }
        next_offset_arr.append_value(state.next_offset.offset);
        let latest_timestamp = state
            .next_offset
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
        Arc::new(topic_arr.finish()),
        Arc::new(partition_value_arr.finish()),
        Arc::new(next_offset_arr.finish()),
        Arc::new(latest_timestamp_arr.finish()),
    ];

    RecordBatch::try_new(schema, columns).map_err(DataFusionError::from)
}

impl DisplayAs for TopicPartitionValueDiscoveryExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "TopicPartitionValueDiscoveryExec: namespace=[{}]",
            self.namespace
        )
    }
}

impl fmt::Debug for TopicPartitionValueDiscoveryExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TopicPartitionValueDiscoveryExec")
            .field("namespace", &self.namespace)
            .finish()
    }
}
