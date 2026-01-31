use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use bytesize::ByteSize;
use datafusion::common::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use wings_schema::{DataType, Field, Schema, SchemaBuilder, SchemaError, TimeUnit};

use crate::{NamespaceName, resource_type};

pub const OFFSET_COLUMN_NAME: &str = "__offset__";
pub const OFFSET_COLUMN_ID: u64 = u64::MAX;
pub const TIMESTAMP_COLUMN_NAME: &str = "__timestamp__";
pub const TIMESTAMP_COLUMN_ID: u64 = u64::MAX - 1;

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
    /// The topic status.
    pub status: Option<TopicStatus>,
}

/// The status of a topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicStatus {
    /// The table status.
    pub table_status: TableStatus,
    /// The number of partitions.
    pub num_partitions: u64,
    /// The conditions of the topic.
    pub conditions: Vec<TopicCondition>,
}

/// The table status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableStatus {
    /// No table status.
    None,
    /// Table is pending creation.
    Pending,
    /// Table has been created.
    Created {
        /// The table ID.
        table_id: String,
    },
    /// Table creation failed.
    Error {
        /// The error message.
        message: String,
    },
}

/// A condition on a topic, similar to Kubernetes conditions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicCondition {
    /// The condition type.
    pub condition_type: String,
    /// Whether the condition is operational.
    pub status: bool,
    /// The cause of the current status.
    pub reason: String,
    /// A human-friendly message.
    pub message: String,
    /// When the state changed.
    pub last_transition_time: SystemTime,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionConfiguration {
    /// How often to compact the topic.
    pub freshness: Duration,
    /// How long to keep the topic data.
    pub ttl: Option<Duration>,
    /// The target file size for compacted files.
    pub target_file_size: ByteSize,
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
            status: None,
        }
    }

    /// Set the topic status.
    pub fn with_status(mut self, status: TopicStatus) -> Self {
        self.status = Some(status);
        self
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

    /// The topic's schema.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn arrow_schema(&self) -> ArrowSchemaRef {
        self.schema().arrow_schema().into()
    }

    /// Returns the topic's schema without the partition field.
    ///
    /// Since partition fields are usually not stored in the physical Parquet
    /// file, this method returns a schema that excludes the partition field.
    pub fn schema_without_partition_field(&self) -> Schema {
        let Some(partition_key) = self.partition_key else {
            return self.schema().clone();
        };

        let fields = self
            .schema()
            .fields_iter()
            .filter(|field| field.id != partition_key)
            .cloned()
            .collect::<Vec<_>>();
        // PANIC: if the current schema is valid, then a schema without the partition field is also valid
        SchemaBuilder::new(fields)
            .build()
            .expect("derived schema is valid")
    }

    /// Returns the topic's schema without the partition field.
    ///
    /// Since partition fields are usually not stored in the physical Parquet
    /// file, this method returns a schema that excludes the partition field.
    pub fn arrow_schema_without_partition_field(&self) -> ArrowSchemaRef {
        self.schema_without_partition_field().arrow_schema().into()
    }

    /// Returns the topic's schema with the extra metadata columns (e.g. offset and timestamp).
    ///
    /// Optionally, include the partition column (if any).
    ///
    /// Notice that this method can fail if the topic's columns include one with
    /// the same id as the metadata columns.
    pub fn schema_with_metadata(
        &self,
        include_partition_field: bool,
    ) -> Result<Schema, SchemaError> {
        let mut fields = if let Some(partition_key) = self.partition_key
            && !include_partition_field
        {
            self.schema()
                .fields_iter()
                .filter(|field| field.id != partition_key)
                .cloned()
                .map(Arc::new)
                .collect::<Vec<_>>()
        } else {
            self.schema.fields.to_vec()
        };

        fields
            .push(Field::new(OFFSET_COLUMN_NAME, OFFSET_COLUMN_ID, DataType::UInt64, true).into());
        fields.push(
            Field::new(
                TIMESTAMP_COLUMN_NAME,
                TIMESTAMP_COLUMN_ID,
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            )
            .into(),
        );

        SchemaBuilder::new(fields).build()
    }

    pub fn arrow_schema_with_metadata(
        &self,
        include_partition_field: bool,
    ) -> Result<ArrowSchemaRef, SchemaError> {
        let schema = self.schema_with_metadata(include_partition_field)?;
        Ok(schema.arrow_schema().into())
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
            target_file_size: ByteSize::mb(512),
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
    use wings_schema::{DataType, Field, SchemaBuilder};

    use crate::{NamespaceName, TenantName, Topic, TopicName, TopicOptions};

    #[test]
    fn test_topic_creation() {
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        let topic_name = TopicName::new("test-topic", namespace_name.clone()).unwrap();
        let schema = SchemaBuilder::new(vec![Field::new("test", 1, DataType::Utf8, false)])
            .build()
            .unwrap();
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
        let schema = SchemaBuilder::new(vec![
            Field::new("id", 0, DataType::Int64, false),
            Field::new("message", 1, DataType::Utf8, false),
        ])
        .build()
        .unwrap();
        let options = TopicOptions::new_with_partition_key(schema, Some(0));
        let topic = Topic::new(topic_name.clone(), options);

        assert_eq!(topic.name, topic_name);
        assert_eq!(topic.schema.fields.len(), 2);
        assert_eq!(topic.partition_key, Some(0));
    }
}
