use std::{sync::Arc, usize};

use arrow::array::{ArrayRef, RecordBatch, StringViewBuilder, UInt32Builder};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::{error::DataFusionError, prelude::Expr};
use wings_metadata_core::admin::{
    Admin, NamespaceName, Topic, collect_namespace_topics, collect_namespace_topics_from_ids,
};

use super::{
    helpers::{TOPIC_NAME_COLUMN, find_topic_name_in_filters},
    provider::SystemTable,
};

pub struct TopicTable {
    admin: Arc<dyn Admin>,
    namespace: NamespaceName,
    schema: SchemaRef,
}

impl TopicTable {
    pub fn new(admin: Arc<dyn Admin>, namespace: NamespaceName) -> Self {
        Self {
            admin,
            namespace,
            schema: topic_schema(),
        }
    }
}

#[async_trait]
impl SystemTable for TopicTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    async fn scan(
        &self,
        filters: Vec<Expr>,
        _limit: Option<usize>,
    ) -> Result<RecordBatch, DataFusionError> {
        let topics = if let Some(topic_names) = find_topic_name_in_filters(&filters) {
            collect_namespace_topics_from_ids(&self.admin, &self.namespace, &topic_names).await?
        } else {
            collect_namespace_topics(&self.admin, &self.namespace).await?
        };

        from_topics(self.schema.clone(), &topics)
    }
}

impl std::fmt::Debug for TopicTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TopicTable")
            .field("namespace", &self.namespace)
            .finish()
    }
}

fn topic_schema() -> SchemaRef {
    let fields = vec![
        Field::new("tenant", DataType::Utf8View, false),
        Field::new("namespace", DataType::Utf8View, false),
        Field::new(TOPIC_NAME_COLUMN, DataType::Utf8View, false),
        Field::new("partition_key", DataType::UInt32, true),
    ];

    Arc::new(Schema::new(fields))
}

fn from_topics(schema: SchemaRef, topics: &[Topic]) -> Result<RecordBatch, DataFusionError> {
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
