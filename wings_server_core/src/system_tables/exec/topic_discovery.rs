use std::{any::Any, fmt, sync::Arc};

use arrow::array::{ArrayRef, RecordBatch, StringViewBuilder, UInt32Builder};
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
use futures::StreamExt;
use tracing::debug;
use wings_metadata_core::admin::{Admin, NamespaceName, PaginatedTopicStream, Topic};

use crate::system_tables::helpers::TOPIC_NAME_COLUMN;

pub struct TopicDiscoveryExec {
    admin: Arc<dyn Admin>,
    namespace: NamespaceName,
    topics: Option<Vec<String>>,
    properties: PlanProperties,
}

impl TopicDiscoveryExec {
    pub fn new(
        admin: Arc<dyn Admin>,
        namespace: NamespaceName,
        topics: Option<Vec<String>>,
    ) -> Self {
        let schema = Self::schema();
        let properties = Self::compute_properties(&schema);

        Self {
            admin,
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
            Field::new("partition_key", DataType::UInt32, true),
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

impl ExecutionPlan for TopicDiscoveryExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
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

    fn schema(&self) -> SchemaRef {
        Self::schema()
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        assert_eq!(partition, 0);
        let batch_size = context.session_config().batch_size();
        debug!(namespace = %self.namespace, "TopicDiscoveryExec::execute");

        let schema = self.schema();

        let topics = PaginatedTopicStream::new(
            self.admin.clone(),
            self.namespace.clone(),
            batch_size,
            self.topics.clone(),
        );

        let stream = RecordBatchStreamAdapter::new(
            self.schema(),
            topics.map(move |result| {
                result
                    .map_err(DataFusionError::from)
                    .and_then(|topics| from_topics(schema.clone(), &topics))
            }),
        );

        Ok(Box::pin(stream))
    }
}

impl DisplayAs for TopicDiscoveryExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TopicDiscovery: namespace=[{}]", self.namespace)
    }
}

impl fmt::Debug for TopicDiscoveryExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TopicDiscoveryExec")
            .field("namespace", &self.namespace)
            .finish()
    }
}

fn from_topics(schema: SchemaRef, topics: &[Topic]) -> Result<RecordBatch> {
    let mut tenant_arr = StringViewBuilder::with_capacity(topics.len());
    let mut namespace_arr = StringViewBuilder::with_capacity(topics.len());
    let mut topic_arr = StringViewBuilder::with_capacity(topics.len());
    let mut partition_key_arr = UInt32Builder::with_capacity(topics.len());

    for topic in topics {
        let topic_name = topic.name.clone();
        topic_arr.append_value(topic_name.id.clone());
        let namespace_name = topic_name.parent();
        namespace_arr.append_value(namespace_name.id.clone());
        let tenant_name = namespace_name.parent();
        tenant_arr.append_value(tenant_name.id.clone());
        if let Some(partition_key) = topic.partition_key {
            partition_key_arr.append_value(partition_key as _);
        } else {
            partition_key_arr.append_null();
        }
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(tenant_arr.finish()),
        Arc::new(namespace_arr.finish()),
        Arc::new(topic_arr.finish()),
        Arc::new(partition_key_arr.finish()),
    ];

    RecordBatch::try_new(schema, columns).map_err(DataFusionError::from)
}
