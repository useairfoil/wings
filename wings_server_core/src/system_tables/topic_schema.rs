use std::sync::Arc;

use async_trait::async_trait;
use datafusion::{
    common::arrow::{
        array::{ArrayRef, BooleanBuilder, RecordBatch, StringViewBuilder},
        datatypes::{DataType, Field, Schema, SchemaRef},
    },
    error::DataFusionError,
    prelude::Expr,
};
use wings_metadata_core::admin::{
    Admin, NamespaceName, Topic, collect_namespace_topics, collect_namespace_topics_from_ids,
};

use super::{
    helpers::{TOPIC_NAME_COLUMN, find_topic_name_in_filters},
    provider::SystemTable,
};

pub struct TopicSchemaTable {
    admin: Arc<dyn Admin>,
    namespace: NamespaceName,
    schema: SchemaRef,
}

impl TopicSchemaTable {
    pub fn new(admin: Arc<dyn Admin>, namespace: NamespaceName) -> Self {
        Self {
            admin,
            namespace,
            schema: topic_schema_schema(),
        }
    }
}

#[async_trait]
impl SystemTable for TopicSchemaTable {
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

impl std::fmt::Debug for TopicSchemaTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TopicSchemaTable")
            .field("namespace", &self.namespace)
            .finish()
    }
}

fn topic_schema_schema() -> SchemaRef {
    let fields = vec![
        Field::new("tenant", DataType::Utf8View, false),
        Field::new("namespace", DataType::Utf8View, false),
        Field::new(TOPIC_NAME_COLUMN, DataType::Utf8View, false),
        Field::new("field", DataType::Utf8View, false),
        Field::new("data_type", DataType::Utf8View, false),
        Field::new("nullable", DataType::Boolean, false),
        Field::new("is_partition_key", DataType::Boolean, false),
    ];

    Arc::new(Schema::new(fields))
}

fn from_topics(schema: SchemaRef, topics: &[Topic]) -> Result<RecordBatch, DataFusionError> {
    let mut tenant_arr = StringViewBuilder::with_capacity(topics.len());
    let mut namespace_arr = StringViewBuilder::with_capacity(topics.len());
    let mut topic_arr = StringViewBuilder::with_capacity(topics.len());
    let mut field_arr = StringViewBuilder::with_capacity(topics.len());
    let mut data_type_arr = StringViewBuilder::with_capacity(topics.len());
    let mut nullable_arr = BooleanBuilder::with_capacity(topics.len());
    let mut is_partition_key_arr = BooleanBuilder::with_capacity(topics.len());

    for topic in topics {
        let partition_key = topic.partition_key;
        for (index, field) in topic.schema().fields().iter().enumerate() {
            let topic_name = topic.name.clone();
            topic_arr.append_value(topic_name.id.clone());
            let namespace_name = topic_name.parent();
            namespace_arr.append_value(namespace_name.id.clone());
            let tenant_name = namespace_name.parent();
            tenant_arr.append_value(tenant_name.id.clone());

            field_arr.append_value(field.name());
            data_type_arr.append_value(field.data_type().to_string());
            nullable_arr.append_value(field.is_nullable());
            is_partition_key_arr.append_value(Some(index) == partition_key);
        }
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(tenant_arr.finish()),
        Arc::new(namespace_arr.finish()),
        Arc::new(topic_arr.finish()),
        Arc::new(field_arr.finish()),
        Arc::new(data_type_arr.finish()),
        Arc::new(nullable_arr.finish()),
        Arc::new(is_partition_key_arr.finish()),
    ];

    RecordBatch::try_new(schema, columns).map_err(DataFusionError::from)
}
