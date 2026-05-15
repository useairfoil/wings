use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use bytesize::ByteSize;
use datafusion::common::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use wings_schema::{
    DataType, Field, Schema, SchemaBuilder, SchemaError, TimeUnit, schema_without_partition_field,
};

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
#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct TopicStatus {
    /// The number of partitions.
    pub num_partitions: u64,
    /// The conditions of the topic.
    pub conditions: Vec<TopicCondition>,
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

/// Whether to include the partition field (if any) in the schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionPosition {
    /// Don't include it.
    Skip,
    /// Include it in the original position.
    Original,
    /// Include it after all topic's columns, but before the metadata columns.
    Last,
}

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
        schema_without_partition_field(self.schema(), self.partition_key)
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
        partition: PartitionPosition,
    ) -> Result<Schema, SchemaError> {
        let mut fields = if let Some(partition_key) = self.partition_key {
            match partition {
                PartitionPosition::Skip => self
                    .schema()
                    .fields_iter()
                    .filter(|field| field.id != partition_key)
                    .cloned()
                    .map(Arc::new)
                    .collect::<Vec<_>>(),
                PartitionPosition::Original => self.schema.fields.to_vec(),
                PartitionPosition::Last => {
                    let mut fields = self
                        .schema()
                        .fields_iter()
                        .filter(|field| field.id != partition_key)
                        .cloned()
                        .map(Arc::new)
                        .collect::<Vec<_>>();
                    // PANIC: if we made it this far we must have a partition field.
                    let partition_field = self
                        .partition_field()
                        .expect("must have partition field")
                        .clone();
                    fields.push(partition_field.into());
                    fields
                }
            }
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
        partition: PartitionPosition,
    ) -> Result<ArrowSchemaRef, SchemaError> {
        let schema = self.schema_with_metadata(partition)?;
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
    use std::time::Duration;

    use bytesize::ByteSize;
    use wings_schema::{DataType, Field, SchemaBuilder};

    use crate::{
        CompactionConfiguration, NamespaceName, PartitionPosition, TenantName, TopicName,
        TopicOptions, TopicStatus, validate_compaction,
    };

    #[test]
    fn test_topic_name_creation() {
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        let topic_name = TopicName::new("test-topic", namespace_name.clone()).unwrap();

        assert_eq!(topic_name.id(), "test-topic");
        assert_eq!(topic_name.parent(), &namespace_name);
        assert_eq!(
            topic_name.name(),
            "tenants/test-tenant/namespaces/test-namespace/topics/test-topic"
        );
        assert_eq!(
            topic_name.to_string(),
            "tenants/test-tenant/namespaces/test-namespace/topics/test-topic"
        );
    }

    #[test]
    fn test_topic_name_parse() {
        let topic_name =
            TopicName::parse("tenants/test-tenant/namespaces/test-namespace/topics/test-topic")
                .unwrap();
        assert_eq!(topic_name.id(), "test-topic");

        // Test parse with invalid format
        let result = TopicName::parse("invalid-format");
        assert!(result.is_err());

        // Test parse with missing parent
        let result = TopicName::parse("topics/test-topic");
        assert!(result.is_err());
    }

    #[test]
    fn test_topic_name_from_str() {
        let topic_name: TopicName =
            "tenants/test-tenant/namespaces/test-namespace/topics/test-topic"
                .parse()
                .unwrap();
        assert_eq!(topic_name.id(), "test-topic");

        let result: Result<TopicName, _> = "invalid".parse();
        assert!(result.is_err());
    }

    #[test]
    fn test_topic_name_new_unchecked() {
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        let topic_name = TopicName::new_unchecked("test-topic", namespace_name);
        assert_eq!(topic_name.id(), "test-topic");
    }

    #[test]
    fn test_compaction_configuration_default() {
        let config = CompactionConfiguration::default();

        assert_eq!(config.freshness, Duration::from_mins(5));
        assert_eq!(config.ttl, None);
        assert_eq!(config.target_file_size, ByteSize::mb(512));
    }

    #[test]
    fn test_validate_compaction_valid() {
        let config = CompactionConfiguration::default();
        let result = validate_compaction(&config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_compaction_invalid_freshness() {
        let config = CompactionConfiguration {
            freshness: Duration::from_secs(30), // Less than 1 minute
            ttl: None,
            target_file_size: ByteSize::mb(512),
        };
        let result = validate_compaction(&config);
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.contains("freshness")));
    }

    #[test]
    fn test_validate_compaction_invalid_ttl_too_short() {
        let config = CompactionConfiguration {
            freshness: Duration::from_mins(5),
            ttl: Some(Duration::from_secs(30)), // Less than 1 minute
            target_file_size: ByteSize::mb(512),
        };
        let result = validate_compaction(&config);
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.contains("ttl")));
    }

    #[test]
    fn test_validate_compaction_invalid_ttl_less_than_freshness() {
        let config = CompactionConfiguration {
            freshness: Duration::from_mins(10),
            ttl: Some(Duration::from_mins(5)), // Less than freshness
            target_file_size: ByteSize::mb(512),
        };
        let result = validate_compaction(&config);
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.contains("ttl")));
    }

    #[test]
    fn test_validate_compaction_multiple_errors() {
        let config = CompactionConfiguration {
            freshness: Duration::from_secs(30), // Invalid
            ttl: Some(Duration::from_secs(15)), // Invalid: too short and <= freshness
            target_file_size: ByteSize::mb(512),
        };
        let result = validate_compaction(&config);
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.len() >= 2); // Should have at least freshness and ttl errors
    }

    #[test]
    fn test_topic_options_with_description() {
        let schema = SchemaBuilder::new(vec![Field::new("test", 1, DataType::Utf8, false)])
            .build()
            .unwrap();
        let options = TopicOptions::new(schema).with_description("Test topic description");

        assert_eq!(
            options.description,
            Some("Test topic description".to_string())
        );
    }

    #[test]
    fn test_topic_options_with_compaction() {
        let schema = SchemaBuilder::new(vec![Field::new("test", 1, DataType::Utf8, false)])
            .build()
            .unwrap();
        let compaction = CompactionConfiguration {
            freshness: Duration::from_mins(10),
            ttl: Some(Duration::from_hours(24)),
            target_file_size: ByteSize::mb(256),
        };
        let options = TopicOptions::new(schema).with_compaction(compaction.clone());

        assert_eq!(options.compaction.freshness, compaction.freshness);
        assert_eq!(options.compaction.ttl, compaction.ttl);
        assert_eq!(
            options.compaction.target_file_size,
            compaction.target_file_size
        );
    }

    #[test]
    fn test_topic_with_status() {
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        let topic_name = TopicName::new("test-topic", namespace_name).unwrap();
        let schema = SchemaBuilder::new(vec![Field::new("test", 1, DataType::Utf8, false)])
            .build()
            .unwrap();
        let options = TopicOptions::new(schema);
        let status = TopicStatus {
            num_partitions: 10,
            conditions: vec![],
        };
        let topic = crate::Topic::new(topic_name, options).with_status(status);

        assert!(topic.status.is_some());
        assert_eq!(topic.status.as_ref().unwrap().num_partitions, 10);
    }

    #[test]
    fn test_topic_partition_field() {
        let schema = SchemaBuilder::new(vec![
            Field::new("id", 0, DataType::Int64, false),
            Field::new("message", 1, DataType::Utf8, false),
        ])
        .build()
        .unwrap();
        let options = TopicOptions::new_with_partition_key(schema, Some(0));
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        let topic_name = TopicName::new("test-topic", namespace_name).unwrap();
        let topic = crate::Topic::new(topic_name, options);

        let partition_field = topic.partition_field();
        assert!(partition_field.is_some());
        assert_eq!(partition_field.unwrap().id, 0);
        assert_eq!(partition_field.unwrap().name, "id");
    }

    #[test]
    fn test_topic_partition_field_none() {
        let schema = SchemaBuilder::new(vec![
            Field::new("id", 0, DataType::Int64, false),
            Field::new("message", 1, DataType::Utf8, false),
        ])
        .build()
        .unwrap();
        let options = TopicOptions::new(schema); // No partition key
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        let topic_name = TopicName::new("test-topic", namespace_name).unwrap();
        let topic = crate::Topic::new(topic_name, options);

        let partition_field = topic.partition_field();
        assert!(partition_field.is_none());
    }

    #[test]
    fn test_topic_partition_field_data_type() {
        let schema = SchemaBuilder::new(vec![
            Field::new("id", 0, DataType::Int64, false),
            Field::new("message", 1, DataType::Utf8, false),
        ])
        .build()
        .unwrap();
        let options = TopicOptions::new_with_partition_key(schema, Some(0));
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        let topic_name = TopicName::new("test-topic", namespace_name).unwrap();
        let topic = crate::Topic::new(topic_name, options);

        let data_type = topic.partition_field_data_type();
        assert!(data_type.is_some());
        assert_eq!(data_type.unwrap(), &DataType::Int64);
    }

    #[test]
    fn test_topic_arrow_schema() {
        let schema = SchemaBuilder::new(vec![Field::new("test", 1, DataType::Utf8, false)])
            .build()
            .unwrap();
        let options = TopicOptions::new(schema);
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        let topic_name = TopicName::new("test-topic", namespace_name).unwrap();
        let topic = crate::Topic::new(topic_name, options);

        let arrow_schema = topic.arrow_schema();
        assert_eq!(arrow_schema.fields().len(), 1);
    }

    #[test]
    fn test_topic_schema_without_partition_field() {
        let schema = SchemaBuilder::new(vec![
            Field::new("id", 0, DataType::Int64, false),
            Field::new("message", 1, DataType::Utf8, false),
        ])
        .build()
        .unwrap();
        let options = TopicOptions::new_with_partition_key(schema, Some(0));
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        let topic_name = TopicName::new("test-topic", namespace_name).unwrap();
        let topic = crate::Topic::new(topic_name, options);

        let schema_without_partition = topic.schema_without_partition_field();
        assert_eq!(schema_without_partition.fields.len(), 1); // Only message field
        assert!(
            schema_without_partition
                .fields_iter()
                .all(|f| f.name != "id")
        );
    }

    #[test]
    fn test_topic_arrow_schema_without_partition_field() {
        let schema = SchemaBuilder::new(vec![
            Field::new("id", 0, DataType::Int64, false),
            Field::new("message", 1, DataType::Utf8, false),
        ])
        .build()
        .unwrap();
        let options = TopicOptions::new_with_partition_key(schema, Some(0));
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        let topic_name = TopicName::new("test-topic", namespace_name).unwrap();
        let topic = crate::Topic::new(topic_name, options);

        let arrow_schema = topic.arrow_schema_without_partition_field();
        assert_eq!(arrow_schema.fields().len(), 1);
    }

    #[test]
    fn test_topic_schema_with_metadata_skip() {
        let schema = SchemaBuilder::new(vec![Field::new("test", 1, DataType::Utf8, false)])
            .build()
            .unwrap();
        let options = TopicOptions::new(schema);
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        let topic_name = TopicName::new("test-topic", namespace_name).unwrap();
        let topic = crate::Topic::new(topic_name, options);

        let schema_with_metadata = topic.schema_with_metadata(PartitionPosition::Skip).unwrap();
        // Should have original field + offset + timestamp = 3 fields
        assert_eq!(schema_with_metadata.fields.len(), 3);
    }

    #[test]
    fn test_topic_schema_with_metadata_original() {
        let schema = SchemaBuilder::new(vec![
            Field::new("id", 0, DataType::Int64, false),
            Field::new("message", 1, DataType::Utf8, false),
        ])
        .build()
        .unwrap();
        let options = TopicOptions::new_with_partition_key(schema, Some(0));
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        let topic_name = TopicName::new("test-topic", namespace_name).unwrap();
        let topic = crate::Topic::new(topic_name, options);

        let schema_with_metadata = topic
            .schema_with_metadata(PartitionPosition::Original)
            .unwrap();
        // Should have all original fields + offset + timestamp
        assert_eq!(schema_with_metadata.fields.len(), 4);
    }

    #[test]
    fn test_topic_schema_with_metadata_last() {
        let schema = SchemaBuilder::new(vec![
            Field::new("id", 0, DataType::Int64, false),
            Field::new("message", 1, DataType::Utf8, false),
        ])
        .build()
        .unwrap();
        let options = TopicOptions::new_with_partition_key(schema, Some(0));
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        let topic_name = TopicName::new("test-topic", namespace_name).unwrap();
        let topic = crate::Topic::new(topic_name, options);

        let schema_with_metadata = topic.schema_with_metadata(PartitionPosition::Last).unwrap();
        // Should have all fields with partition last + offset + timestamp = 4
        assert_eq!(schema_with_metadata.fields.len(), 4);
    }

    #[test]
    fn test_topic_arrow_schema_with_metadata() {
        let schema = SchemaBuilder::new(vec![Field::new("test", 1, DataType::Utf8, false)])
            .build()
            .unwrap();
        let options = TopicOptions::new(schema);
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        let topic_name = TopicName::new("test-topic", namespace_name).unwrap();
        let topic = crate::Topic::new(topic_name, options);

        let arrow_schema = topic
            .arrow_schema_with_metadata(PartitionPosition::Skip)
            .unwrap();
        assert_eq!(arrow_schema.fields().len(), 3); // test + offset + timestamp
    }
}
