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

pub const SEQNUM_COLUMN_NAME: &str = "__seqnum__";
pub const SEQNUM_COLUMN_ID: u64 = u64::MAX;
pub const TIMESTAMP_COLUMN_NAME: &str = "__timestamp__";
pub const TIMESTAMP_COLUMN_ID: u64 = u64::MAX - 1;

resource_type!(Table, "tables", Namespace);

/// A table belonging to a namespace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Table {
    /// The table name.
    pub name: TableName,
    /// The table's schema.
    pub schema: Schema,
    /// The index of the field that is used to partition the table.
    pub partition_key: Option<u64>,
    /// The table description.
    pub description: Option<String>,
    /// The table compaction configuration.
    pub compaction: CompactionConfiguration,
    /// The table status.
    pub status: Option<TableStatus>,
}

/// The status of a table.
#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct TableStatus {
    /// The number of partitions.
    pub num_partitions: u64,
    /// The conditions of the table.
    pub conditions: Vec<TableCondition>,
}

/// A condition on a table, similar to Kubernetes conditions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableCondition {
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
    /// How often to compact the table.
    pub freshness: Duration,
    /// How long to keep the table data.
    pub ttl: Option<Duration>,
    /// The target file size for compacted files.
    pub target_file_size: ByteSize,
}

pub type TableRef = Arc<Table>;

/// Whether to include the partition field (if any) in the schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionPosition {
    /// Don't include it.
    Skip,
    /// Include it in the original position.
    Original,
    /// Include it after all table's columns, but before the metadata columns.
    Last,
}

impl Table {
    /// Create a new table with the given name and options.
    pub fn new(name: TableName, options: TableOptions) -> Self {
        Self {
            name,
            schema: options.schema,
            partition_key: options.partition_key,
            description: options.description,
            compaction: options.compaction,
            status: None,
        }
    }

    /// Set the table status.
    pub fn with_status(mut self, status: TableStatus) -> Self {
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

    /// The table's schema.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn arrow_schema(&self) -> ArrowSchemaRef {
        self.schema().arrow_schema().into()
    }

    /// Returns the table's schema without the partition field.
    ///
    /// Since partition fields are usually not stored in the physical Parquet
    /// file, this method returns a schema that excludes the partition field.
    pub fn schema_without_partition_field(&self) -> Schema {
        schema_without_partition_field(self.schema(), self.partition_key)
    }

    /// Returns the table's schema without the partition field.
    ///
    /// Since partition fields are usually not stored in the physical Parquet
    /// file, this method returns a schema that excludes the partition field.
    pub fn arrow_schema_without_partition_field(&self) -> ArrowSchemaRef {
        self.schema_without_partition_field().arrow_schema().into()
    }

    /// Returns the table's schema with the extra metadata columns (e.g. seqnum and timestamp).
    ///
    /// Optionally, include the partition column (if any).
    ///
    /// Notice that this method can fail if the table's columns include one with
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
            .push(Field::new(SEQNUM_COLUMN_NAME, SEQNUM_COLUMN_ID, DataType::UInt64, true).into());
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

/// Options for creating a table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableOptions {
    /// The table's schema.
    pub schema: Schema,
    /// The index of the field that is used to partition the table.
    pub partition_key: Option<u64>,
    /// The table description.
    pub description: Option<String>,
    /// The table compaction configuration.
    pub compaction: CompactionConfiguration,
}

impl TableOptions {
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
        CompactionConfiguration, NamespaceName, PartitionPosition, TableName, TableOptions,
        TableStatus, TenantName, validate_compaction,
    };

    #[test]
    fn test_table_name_creation() {
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        let table_name = TableName::new("test-table", namespace_name.clone()).unwrap();

        assert_eq!(table_name.id(), "test-table");
        assert_eq!(table_name.parent(), &namespace_name);
        assert_eq!(
            table_name.name(),
            "tenants/test-tenant/namespaces/test-namespace/tables/test-table"
        );
        assert_eq!(
            table_name.to_string(),
            "tenants/test-tenant/namespaces/test-namespace/tables/test-table"
        );
    }

    #[test]
    fn test_table_name_parse() {
        let table_name =
            TableName::parse("tenants/test-tenant/namespaces/test-namespace/tables/test-table")
                .unwrap();
        assert_eq!(table_name.id(), "test-table");

        // Test parse with invalid format
        let result = TableName::parse("invalid-format");
        assert!(result.is_err());

        // Test parse with missing parent
        let result = TableName::parse("tables/test-table");
        assert!(result.is_err());
    }

    #[test]
    fn test_table_name_from_str() {
        let table_name: TableName =
            "tenants/test-tenant/namespaces/test-namespace/tables/test-table"
                .parse()
                .unwrap();
        assert_eq!(table_name.id(), "test-table");

        let result: Result<TableName, _> = "invalid".parse();
        assert!(result.is_err());
    }

    #[test]
    fn test_table_name_new_unchecked() {
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        let table_name = TableName::new_unchecked("test-table", namespace_name);
        assert_eq!(table_name.id(), "test-table");
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
    fn test_table_options_with_description() {
        let schema = SchemaBuilder::new(vec![Field::new("test", 1, DataType::Utf8, false)])
            .build()
            .unwrap();
        let options = TableOptions::new(schema).with_description("Test table description");

        assert_eq!(
            options.description,
            Some("Test table description".to_string())
        );
    }

    #[test]
    fn test_table_options_with_compaction() {
        let schema = SchemaBuilder::new(vec![Field::new("test", 1, DataType::Utf8, false)])
            .build()
            .unwrap();
        let compaction = CompactionConfiguration {
            freshness: Duration::from_mins(10),
            ttl: Some(Duration::from_hours(24)),
            target_file_size: ByteSize::mb(256),
        };
        let options = TableOptions::new(schema).with_compaction(compaction.clone());

        assert_eq!(options.compaction.freshness, compaction.freshness);
        assert_eq!(options.compaction.ttl, compaction.ttl);
        assert_eq!(
            options.compaction.target_file_size,
            compaction.target_file_size
        );
    }

    #[test]
    fn test_table_with_status() {
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        let table_name = TableName::new("test-table", namespace_name).unwrap();
        let schema = SchemaBuilder::new(vec![Field::new("test", 1, DataType::Utf8, false)])
            .build()
            .unwrap();
        let options = TableOptions::new(schema);
        let status = TableStatus {
            num_partitions: 10,
            conditions: vec![],
        };
        let table = crate::Table::new(table_name, options).with_status(status);

        assert!(table.status.is_some());
        assert_eq!(table.status.as_ref().unwrap().num_partitions, 10);
    }

    #[test]
    fn test_table_partition_field() {
        let schema = SchemaBuilder::new(vec![
            Field::new("id", 0, DataType::Int64, false),
            Field::new("message", 1, DataType::Utf8, false),
        ])
        .build()
        .unwrap();
        let options = TableOptions::new_with_partition_key(schema, Some(0));
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        let table_name = TableName::new("test-table", namespace_name).unwrap();
        let table = crate::Table::new(table_name, options);

        let partition_field = table.partition_field();
        assert!(partition_field.is_some());
        assert_eq!(partition_field.unwrap().id, 0);
        assert_eq!(partition_field.unwrap().name, "id");
    }

    #[test]
    fn test_table_partition_field_none() {
        let schema = SchemaBuilder::new(vec![
            Field::new("id", 0, DataType::Int64, false),
            Field::new("message", 1, DataType::Utf8, false),
        ])
        .build()
        .unwrap();
        let options = TableOptions::new(schema); // No partition key
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        let table_name = TableName::new("test-table", namespace_name).unwrap();
        let table = crate::Table::new(table_name, options);

        let partition_field = table.partition_field();
        assert!(partition_field.is_none());
    }

    #[test]
    fn test_table_partition_field_data_type() {
        let schema = SchemaBuilder::new(vec![
            Field::new("id", 0, DataType::Int64, false),
            Field::new("message", 1, DataType::Utf8, false),
        ])
        .build()
        .unwrap();
        let options = TableOptions::new_with_partition_key(schema, Some(0));
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        let table_name = TableName::new("test-table", namespace_name).unwrap();
        let table = crate::Table::new(table_name, options);

        let data_type = table.partition_field_data_type();
        assert!(data_type.is_some());
        assert_eq!(data_type.unwrap(), &DataType::Int64);
    }

    #[test]
    fn test_table_arrow_schema() {
        let schema = SchemaBuilder::new(vec![Field::new("test", 1, DataType::Utf8, false)])
            .build()
            .unwrap();
        let options = TableOptions::new(schema);
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        let table_name = TableName::new("test-table", namespace_name).unwrap();
        let table = crate::Table::new(table_name, options);

        let arrow_schema = table.arrow_schema();
        assert_eq!(arrow_schema.fields().len(), 1);
    }

    #[test]
    fn test_table_schema_without_partition_field() {
        let schema = SchemaBuilder::new(vec![
            Field::new("id", 0, DataType::Int64, false),
            Field::new("message", 1, DataType::Utf8, false),
        ])
        .build()
        .unwrap();
        let options = TableOptions::new_with_partition_key(schema, Some(0));
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        let table_name = TableName::new("test-table", namespace_name).unwrap();
        let table = crate::Table::new(table_name, options);

        let schema_without_partition = table.schema_without_partition_field();
        assert_eq!(schema_without_partition.fields.len(), 1); // Only message field
        assert!(
            schema_without_partition
                .fields_iter()
                .all(|f| f.name != "id")
        );
    }

    #[test]
    fn test_table_arrow_schema_without_partition_field() {
        let schema = SchemaBuilder::new(vec![
            Field::new("id", 0, DataType::Int64, false),
            Field::new("message", 1, DataType::Utf8, false),
        ])
        .build()
        .unwrap();
        let options = TableOptions::new_with_partition_key(schema, Some(0));
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        let table_name = TableName::new("test-table", namespace_name).unwrap();
        let table = crate::Table::new(table_name, options);

        let arrow_schema = table.arrow_schema_without_partition_field();
        assert_eq!(arrow_schema.fields().len(), 1);
    }

    #[test]
    fn test_table_schema_with_metadata_skip() {
        let schema = SchemaBuilder::new(vec![Field::new("test", 1, DataType::Utf8, false)])
            .build()
            .unwrap();
        let options = TableOptions::new(schema);
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        let table_name = TableName::new("test-table", namespace_name).unwrap();
        let table = crate::Table::new(table_name, options);

        let schema_with_metadata = table.schema_with_metadata(PartitionPosition::Skip).unwrap();
        // Should have original field + seqnum + timestamp = 3 fields
        assert_eq!(schema_with_metadata.fields.len(), 3);
    }

    #[test]
    fn test_table_schema_with_metadata_original() {
        let schema = SchemaBuilder::new(vec![
            Field::new("id", 0, DataType::Int64, false),
            Field::new("message", 1, DataType::Utf8, false),
        ])
        .build()
        .unwrap();
        let options = TableOptions::new_with_partition_key(schema, Some(0));
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        let table_name = TableName::new("test-table", namespace_name).unwrap();
        let table = crate::Table::new(table_name, options);

        let schema_with_metadata = table
            .schema_with_metadata(PartitionPosition::Original)
            .unwrap();
        // Should have all original fields + seqnum + timestamp
        assert_eq!(schema_with_metadata.fields.len(), 4);
    }

    #[test]
    fn test_table_schema_with_metadata_last() {
        let schema = SchemaBuilder::new(vec![
            Field::new("id", 0, DataType::Int64, false),
            Field::new("message", 1, DataType::Utf8, false),
        ])
        .build()
        .unwrap();
        let options = TableOptions::new_with_partition_key(schema, Some(0));
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        let table_name = TableName::new("test-table", namespace_name).unwrap();
        let table = crate::Table::new(table_name, options);

        let schema_with_metadata = table.schema_with_metadata(PartitionPosition::Last).unwrap();
        // Should have all fields with partition last + seqnum + timestamp = 4
        assert_eq!(schema_with_metadata.fields.len(), 4);
    }

    #[test]
    fn test_table_arrow_schema_with_metadata() {
        let schema = SchemaBuilder::new(vec![Field::new("test", 1, DataType::Utf8, false)])
            .build()
            .unwrap();
        let options = TableOptions::new(schema);
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        let table_name = TableName::new("test-table", namespace_name).unwrap();
        let table = crate::Table::new(table_name, options);

        let arrow_schema = table
            .arrow_schema_with_metadata(PartitionPosition::Skip)
            .unwrap();
        assert_eq!(arrow_schema.fields().len(), 3); // test + seqnum + timestamp
    }
}
