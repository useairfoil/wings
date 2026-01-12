use std::{sync::Arc, time::Duration};

use datafusion::common::arrow::datatypes::{
    DataType, Field as ArrowField, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef,
};

use crate::{
    resource_type,
    schema::{Field, Schema},
};

use super::namespace::NamespaceName;

resource_type!(Topic, "topics", Namespace);

/// A topic belonging to a namespace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Topic {
    /// The topic name.
    pub name: TopicName,
    /// The topic's schema.
    pub schema: Schema,
    /// The index of the field that is used to partition the topic.
    pub partition_key: Option<u64>,
    /// The topic description.
    pub description: Option<String>,
    /// The topic compaction configuration.
    pub compaction: CompactionConfiguration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionConfiguration {
    /// How often to compact the topic.
    pub freshness: Duration,
    /// How long to keep the topic data.
    pub ttl: Option<Duration>,
}

pub type TopicRef = Arc<Topic>;

impl Topic {
    /// Create a new topic with the given name and options.
    pub fn new(name: TopicName, options: TopicOptions) -> Self {
        Self {
            name,
            schema: options.schema,
            partition_key: options.partition_key,
            description: options.description,
            compaction: options.compaction,
        }
    }

    /// The topic's schema.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn arrow_schema(&self) -> ArrowSchemaRef {
        let schema = self.schema().clone().into();
        Arc::new(schema)
    }

    /// Returns the topic's schema without the partition field.
    ///
    /// Since partition fields are usually not stored in the physical Parquet
    /// file, this method returns a schema that excludes the partition field.
    pub fn arrow_schema_without_partition_field(&self) -> ArrowSchemaRef {
        let Some(partition_key) = self.partition_key else {
            return self.arrow_schema();
        };

        let fields = self
            .schema()
            .fields_iter()
            .filter(|field| field.id != partition_key)
            .cloned()
            .map(ArrowField::from)
            .collect::<Vec<_>>();

        Arc::new(ArrowSchema::new(fields))
    }

    /// Returns the partition field, if any.
    pub fn partition_field(&self) -> Option<&Field> {
        let partition_key = self.partition_key?;
        self.schema()
            .fields_iter()
            .find(|field| field.id == partition_key)
    }

    /// Returns the data type of the partition field, if any.
    pub fn partition_field_data_type(&self) -> Option<&DataType> {
        self.partition_field().map(|col| &col.data_type)
    }
}

/// Options for creating a topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicOptions {
    /// The topic's schema.
    pub schema: Schema,
    /// The index of the field that is used to partition the topic.
    pub partition_key: Option<u64>,
    /// The topic description.
    pub description: Option<String>,
    /// The topic compaction configuration.
    pub compaction: CompactionConfiguration,
}

impl TopicOptions {
    pub fn new(schema: Schema) -> Self {
        Self {
            schema,
            partition_key: None,
            description: None,
            compaction: Default::default(),
        }
    }

    pub fn new_with_partition_key(schema: Schema, partition_key: Option<u64>) -> Self {
        Self {
            schema,
            partition_key,
            description: None,
            compaction: Default::default(),
        }
    }

    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    pub fn with_compaction(mut self, compaction: CompactionConfiguration) -> Self {
        self.compaction = compaction;
        self
    }
}

impl Default for CompactionConfiguration {
    fn default() -> Self {
        Self {
            freshness: Duration::from_mins(5),
            ttl: None,
        }
    }
}

pub fn validate_compaction(compaction: &CompactionConfiguration) -> Result<(), Vec<String>> {
    let mut errors = Vec::new();

    if compaction.freshness < Duration::from_mins(1) {
        errors.push("freshness must be at least 1 minute".to_string());
    }

    if let Some(ttl) = compaction.ttl {
        if ttl < Duration::from_mins(1) {
            errors.push("ttl must be at least 1 minute".to_string());
        }

        if ttl <= compaction.freshness {
            errors.push("ttl must be greater than freshness".to_string());
        }
    }

    if !errors.is_empty() {
        Err(errors)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use datafusion::common::arrow::datatypes::DataType;

    use crate::{
        resources::{NamespaceName, TenantName, Topic, TopicName, TopicOptions},
        schema::{Field, Schema},
    };

    #[test]
    fn test_topic_creation() {
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        let topic_name = TopicName::new("test-topic", namespace_name.clone()).unwrap();
        let schema = Schema::new(vec![Field::new("test", 1, DataType::Utf8, false)]);
        let options = TopicOptions::new(schema);
        let topic = Topic::new(topic_name.clone(), options);

        assert_eq!(topic.name, topic_name);
        assert_eq!(topic.name.id(), "test-topic");
        assert_eq!(topic.name.parent(), &namespace_name);
        assert_eq!(
            topic.name.name(),
            "tenants/test-tenant/namespaces/test-namespace/topics/test-topic"
        );
        assert_eq!(topic.schema().fields.len(), 1);
        assert_eq!(topic.partition_key, None);
    }

    #[test]
    fn test_topic_with_partition_key() {
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        let topic_name = TopicName::new("test-topic", namespace_name.clone()).unwrap();
        let schema = Schema::new(vec![
            Field::new("id", 0, DataType::Int64, false),
            Field::new("message", 1, DataType::Utf8, false),
        ]);
        let options = TopicOptions::new_with_partition_key(schema, Some(0));
        let topic = Topic::new(topic_name.clone(), options);

        assert_eq!(topic.name, topic_name);
        assert_eq!(topic.schema.fields.len(), 2);
        assert_eq!(topic.partition_key, Some(0));
    }
}
