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
use snafu::ResultExt;
use tracing::debug;
use wings_metadata_core::admin::{
    Admin, ListTopicsRequest, NamespaceName, Topic, TopicName, error::InvalidResourceNameSnafu,
};

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
        let batch_size = context.session_config().batch_size() as i32;
        debug!(partition, batch_size, "TopicDiscoveryExec::execute");

        let schema = self.schema();
        let stream = {
            let schema = schema.clone();
            let admin = self.admin.clone();
            let namespace = self.namespace.clone();
            let topics_filter = self.topics.clone();

            async_stream::stream! {
                if let Some(topics_filter) = topics_filter {
                    for topic_id in topics_filter {
                        let topic_name = TopicName::new(topic_id, namespace.clone())
                            .context(InvalidResourceNameSnafu { resource: "topic" })?;

                        match admin.get_topic(topic_name).await {
                            Ok(topic) => {
                                let batch = from_topics(schema.clone(), &[topic])?;
                                yield Ok(batch);
                            }
                            Err(err) => {
                                if !err.is_not_found() {
                                    yield Err(DataFusionError::from(err));
                                }
                            }
                        }
                    }
                } else {
                    let mut page_token = None;
                    loop {
                        let response = admin
                            .list_topics(ListTopicsRequest {
                                parent: namespace.clone(),
                                page_size: batch_size.into(),
                                page_token: page_token.clone(),
                            })
                            .await?;
                        page_token = response.next_page_token;
                        let batch = from_topics(schema.clone(), &response.topics)?;

                        yield Ok(batch);

                        if page_token.is_none() {
                            break;
                        }
                    }
                }
            }
        };

        let stream = RecordBatchStreamAdapter::new(self.schema(), stream);

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
