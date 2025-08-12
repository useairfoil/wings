use std::{any::Any, fmt, sync::Arc};

use arrow::array::{ArrayRef, RecordBatch, StringViewBuilder};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::{
    error::{DataFusionError, Result},
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
    },
};
use futures::{Stream, StreamExt};
use tracing::{debug, trace};
use wings_metadata_core::{
    admin::{Topic, TopicName},
    offset_registry::{ListTopicPartitionValuesRequest, OffsetRegistry, OffsetRegistryError},
    partition::PartitionValue,
};

use crate::system_tables::helpers::TOPIC_NAME_COLUMN;

/// Execution plan for discovering partition values for topics.
pub struct TopicPartitionValueDiscoveryExec {
    offset_registry: Arc<dyn OffsetRegistry>,
    topic: Topic,
    properties: PlanProperties,
}

impl TopicPartitionValueDiscoveryExec {
    pub fn new(offset_registry: Arc<dyn OffsetRegistry>, topic: Topic) -> Self {
        let schema = Self::schema();
        let properties = Self::compute_properties(&schema);

        Self {
            offset_registry,
            topic,
            properties,
        }
    }

    pub fn schema() -> SchemaRef {
        let fields = vec![
            Field::new("tenant", DataType::Utf8View, false),
            Field::new("namespace", DataType::Utf8View, false),
            Field::new(TOPIC_NAME_COLUMN, DataType::Utf8View, false),
            Field::new("partition_value", DataType::Utf8View, false),
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
        debug!(
            topic = %self.topic.name,
            "TopicPartitionValueDiscoveryExec execute"
        );
        let batch_size = context.session_config().batch_size();

        let stream = {
            let schema = self.schema();
            let offset_registry = self.offset_registry.clone();
            let topic = self.topic.name.clone();

            paginated_topic_value_stream(offset_registry, topic.clone(), batch_size).map(
                move |result| {
                    trace!("TopicPartitionValueDiscoveryExec send_page");
                    result.map_err(DataFusionError::from).and_then(|values| {
                        from_partition_values(schema.clone(), topic.clone(), values)
                    })
                },
            )
        };

        let stream = RecordBatchStreamAdapter::new(self.schema(), stream);

        Ok(Box::pin(stream))
    }
}

fn paginated_topic_value_stream(
    offset_registry: Arc<dyn OffsetRegistry>,
    topic: TopicName,
    page_size: usize,
) -> impl Stream<Item = Result<Vec<PartitionValue>, OffsetRegistryError>> {
    async_stream::stream! {
        let mut page_token = None;
        loop {
            let response = offset_registry
                .list_topic_partition_values(ListTopicPartitionValuesRequest {
                    topic_name: topic.clone(),
                    page_size: page_size.into(),
                    page_token,
                })
                .await?;

            page_token = response.next_page_token;
            yield Ok(response.values);

            if page_token.is_none() {
                break;
            }
        }
    }
}

fn from_partition_values(
    schema: SchemaRef,
    topic_name: TopicName,
    values: Vec<PartitionValue>,
) -> Result<RecordBatch, DataFusionError> {
    if values.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }

    let mut tenant_arr = StringViewBuilder::with_capacity(values.len());
    let mut namespace_arr = StringViewBuilder::with_capacity(values.len());
    let mut topic_arr = StringViewBuilder::with_capacity(values.len());
    let mut partition_value_arr = StringViewBuilder::with_capacity(values.len());

    for value in values {
        topic_arr.append_value(topic_name.id.clone());
        let namespace_name = topic_name.parent();
        namespace_arr.append_value(namespace_name.id.clone());
        let tenant_name = namespace_name.parent();
        tenant_arr.append_value(tenant_name.id.clone());
        partition_value_arr.append_value(value.to_string());
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(tenant_arr.finish()),
        Arc::new(namespace_arr.finish()),
        Arc::new(topic_arr.finish()),
        Arc::new(partition_value_arr.finish()),
    ];

    RecordBatch::try_new(schema, columns).map_err(DataFusionError::from)
}

impl DisplayAs for TopicPartitionValueDiscoveryExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "TopicPartitionValueDiscoveryExec: topic=[{}]",
            self.topic.name
        )
    }
}

impl fmt::Debug for TopicPartitionValueDiscoveryExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TopicPartitionValueDiscoveryExec")
            .field("topic", &self.topic.name)
            .finish()
    }
}
