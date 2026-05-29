use std::sync::Arc;

use datafusion::common::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use serde::{Deserialize, Serialize};
use snafu::Snafu;
use wings_schema::{DataType, Field, Schema};

use crate::{NamespaceName, resource_type};

resource_type!(Table, "tables", Namespace);

#[derive(Debug, Clone, PartialEq, Eq, Snafu)]
pub enum Error {
    #[snafu(display("key field id {field_id} is not a root schema field"))]
    MissingKeyField { field_id: u64 },
    #[snafu(display("version field id {field_id} is not a root schema field"))]
    MissingVersionField { field_id: u64 },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A table belonging to a namespace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Table {
    /// The table name.
    pub name: TableName,
    /// The table's schema.
    pub schema: Schema,
    /// The id of the key field.
    pub key_field_id: u64,
    /// The id of the version field.
    pub version_field_id: u64,
    /// The id of the field that is used to partition the table.
    pub partition_field_id: Option<u64>,
    /// The table description.
    pub description: Option<String>,
}

pub type TableRef = Arc<Table>;

impl Table {
    /// Create a new table with the given name and options.
    pub fn new(name: TableName, options: TableOptions) -> Result<Self> {
        options
            .schema
            .root_field_by_id(options.key_field_id)
            .ok_or(Error::MissingKeyField {
                field_id: options.key_field_id,
            })?;

        options
            .schema
            .root_field_by_id(options.version_field_id)
            .ok_or(Error::MissingVersionField {
                field_id: options.version_field_id,
            })?;

        Ok(Self {
            name,
            schema: options.schema,
            key_field_id: options.key_field_id,
            version_field_id: options.version_field_id,
            partition_field_id: options.partition_field_id,
            description: options.description,
        })
    }

    /// Returns the key field.
    pub fn key_field(&self) -> &Field {
        self.schema()
            .root_field_by_id(self.key_field_id)
            .expect("table key field is valid")
    }

    /// Returns the version field.
    pub fn version_field(&self) -> &Field {
        self.schema()
            .root_field_by_id(self.version_field_id)
            .expect("table version field is valid")
    }

    /// Returns the partition field, if any.
    pub fn partition_field(&self) -> Option<&Field> {
        let partition_key = self.partition_field_id?;
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
}

/// Options for creating a table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableOptions {
    /// The table's schema.
    pub schema: Schema,
    /// The table description.
    pub description: Option<String>,
    /// The id of the key field.
    pub key_field_id: u64,
    /// The id of the version field.
    pub version_field_id: u64,
    /// The id of the field that is used to partition the table.
    pub partition_field_id: Option<u64>,
}

impl TableOptions {
    pub fn new(schema: Schema, key_field_id: u64, version_field_id: u64) -> Self {
        Self {
            schema,
            key_field_id,
            version_field_id,
            partition_field_id: None,
            description: None,
        }
    }

    pub fn with_partition_field(mut self, partition_field: Option<u64>) -> Self {
        self.partition_field_id = partition_field;
        self
    }

    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use wings_schema::{DataType, Field, SchemaBuilder};

    use crate::{NamespaceName, TableName, TableOptions, table::Error};

    fn table_name() -> TableName {
        let namespace_name = NamespaceName::new("test-namespace").unwrap();
        TableName::new("test-table", namespace_name).unwrap()
    }

    fn test_schema() -> wings_schema::Schema {
        SchemaBuilder::new(vec![
            Field::new("id", 0, DataType::Int64, false),
            Field::new("version", 1, DataType::UInt64, false),
            Field::new("message", 2, DataType::Utf8, false),
        ])
        .build()
        .unwrap()
    }

    #[test]
    fn test_table_arrow_schema() {
        let options = TableOptions::new(test_schema(), 0, 1);
        let table = crate::Table::new(table_name(), options).unwrap();

        let arrow_schema = table.arrow_schema();
        assert_eq!(arrow_schema.fields().len(), 3);
    }

    #[test]
    fn test_table_key_and_version_fields() {
        let options = TableOptions::new(test_schema(), 0, 1);
        let table = crate::Table::new(table_name(), options).unwrap();

        assert_eq!(table.key_field().name(), "id");
        assert_eq!(table.version_field().name(), "version");
    }

    #[test]
    fn test_table_partition_field() {
        let options = TableOptions::new(test_schema(), 0, 1).with_partition_field(Some(2));
        let table = crate::Table::new(table_name(), options).unwrap();

        let partition_field = table.partition_field().unwrap();
        assert_eq!(partition_field.id, 2);
        assert_eq!(partition_field.name(), "message");
        assert_eq!(table.partition_field_data_type(), Some(&DataType::Utf8));
    }

    #[test]
    fn test_table_new_missing_key_field() {
        let options = TableOptions::new(test_schema(), 42, 1);
        let error = crate::Table::new(table_name(), options).unwrap_err();

        assert_eq!(error, Error::MissingKeyField { field_id: 42 });
    }

    #[test]
    fn test_table_new_missing_version_field() {
        let options = TableOptions::new(test_schema(), 0, 42);
        let error = crate::Table::new(table_name(), options).unwrap_err();

        assert_eq!(error, Error::MissingVersionField { field_id: 42 });
    }

    #[test]
    fn test_table_new_requires_root_key_field() {
        let schema = SchemaBuilder::new(vec![
            Field::new(
                "nested",
                0,
                DataType::Struct(vec![Field::new("id", 42, DataType::Int64, false)].into()),
                false,
            ),
            Field::new("version", 1, DataType::UInt64, false),
        ])
        .build()
        .unwrap();
        let options = TableOptions::new(schema, 42, 1);
        let error = crate::Table::new(table_name(), options).unwrap_err();

        assert_eq!(error, Error::MissingKeyField { field_id: 42 });
    }

    #[test]
    fn test_table_new_requires_root_version_field() {
        let schema = SchemaBuilder::new(vec![
            Field::new("id", 0, DataType::Int64, false),
            Field::new(
                "nested",
                1,
                DataType::Struct(vec![Field::new("version", 42, DataType::UInt64, false)].into()),
                false,
            ),
        ])
        .build()
        .unwrap();
        let options = TableOptions::new(schema, 0, 42);
        let error = crate::Table::new(table_name(), options).unwrap_err();

        assert_eq!(error, Error::MissingVersionField { field_id: 42 });
    }
}
