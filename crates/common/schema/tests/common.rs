#![allow(dead_code)]
//! Common test utilities for wings_schema tests

use std::{collections::HashMap, sync::Arc};

use wings_schema::{DataType, Field, Fields, Schema, SchemaBuilder, SchemaError};

/// Create a simple test field
pub fn test_field(name: &str, id: u64, data_type: DataType, nullable: bool) -> Field {
    Field::new(name, id, data_type, nullable)
}

/// Create a field with metadata
pub fn test_field_with_metadata(
    name: &str,
    id: u64,
    data_type: DataType,
    nullable: bool,
    metadata: HashMap<String, String>,
) -> Field {
    Field::new(name, id, data_type, nullable).with_metadata(metadata)
}

/// Create a simple schema with the given fields
pub fn test_schema(fields: Vec<Field>) -> Schema {
    SchemaBuilder::new(fields).build().unwrap()
}

/// Create a schema with metadata
pub fn test_schema_with_metadata(
    fields: Vec<Field>,
    metadata: HashMap<String, String>,
) -> Result<Schema, SchemaError> {
    SchemaBuilder::new(fields).with_metadata(metadata).build()
}

/// Sample field IDs for testing
pub const FIELD_ID_NAME: u64 = 1;
pub const FIELD_ID_AGE: u64 = 2;
pub const FIELD_ID_EMAIL: u64 = 3;
pub const FIELD_ID_ACTIVE: u64 = 4;
pub const FIELD_ID_SCORE: u64 = 5;
pub const FIELD_ID_DATA: u64 = 6;
pub const FIELD_ID_CREATED_AT: u64 = 7;
pub const FIELD_ID_AMOUNT: u64 = 8;
pub const FIELD_ID_TAGS: u64 = 9;
pub const FIELD_ID_ADDRESS: u64 = 10;
pub const FIELD_ID_STREET: u64 = 11;
pub const FIELD_ID_CITY: u64 = 12;

/// Create sample primitive type fields
pub fn primitive_fields() -> Vec<Field> {
    vec![
        test_field("name", FIELD_ID_NAME, DataType::Utf8, false),
        test_field("age", FIELD_ID_AGE, DataType::Int32, true),
        test_field("email", FIELD_ID_EMAIL, DataType::Utf8, true),
        test_field("active", FIELD_ID_ACTIVE, DataType::Boolean, true),
        test_field("score", FIELD_ID_SCORE, DataType::Float64, true),
        test_field("data", FIELD_ID_DATA, DataType::Binary, true),
    ]
}

/// Create sample numeric type fields
pub fn numeric_fields() -> Vec<Field> {
    vec![
        test_field("int8_field", 101, DataType::Int8, true),
        test_field("int16_field", 102, DataType::Int16, true),
        test_field("int32_field", 103, DataType::Int32, true),
        test_field("int64_field", 104, DataType::Int64, true),
        test_field("uint8_field", 105, DataType::UInt8, true),
        test_field("uint16_field", 106, DataType::UInt16, true),
        test_field("uint32_field", 107, DataType::UInt32, true),
        test_field("uint64_field", 108, DataType::UInt64, true),
        test_field("float32_field", 109, DataType::Float32, true),
        test_field("float64_field", 110, DataType::Float64, true),
        test_field("float16_field", 111, DataType::Float16, true),
    ]
}

/// Create sample timestamp fields
pub fn timestamp_fields() -> Vec<Field> {
    use wings_schema::TimeUnit;
    vec![
        test_field(
            "timestamp_sec",
            201,
            DataType::Timestamp(TimeUnit::Second, None),
            true,
        ),
        test_field(
            "timestamp_ms",
            202,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        test_field(
            "timestamp_us",
            203,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        test_field(
            "timestamp_ns",
            204,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
        test_field(
            "timestamp_utc",
            205,
            DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
            true,
        ),
    ]
}

/// Create a list field
pub fn list_field(name: &str, id: u64, item_data_type: DataType) -> Field {
    let item_field = Arc::new(Field::new("item", id + 1000, item_data_type, true));
    test_field(name, id, DataType::List(item_field), true)
}

/// Create a struct field with nested fields
pub fn struct_field(name: &str, id: u64, nested_fields: Vec<Field>) -> Field {
    let fields: Fields = nested_fields.into();
    test_field(name, id, DataType::Struct(fields), true)
}

/// Create a complex nested schema
pub fn nested_schema() -> Schema {
    let address_fields = vec![
        test_field("street", FIELD_ID_STREET, DataType::Utf8, false),
        test_field("city", FIELD_ID_CITY, DataType::Utf8, false),
    ];

    let fields = vec![
        test_field("name", FIELD_ID_NAME, DataType::Utf8, false),
        test_field("age", FIELD_ID_AGE, DataType::Int32, true),
        struct_field("address", FIELD_ID_ADDRESS, address_fields),
        list_field("tags", FIELD_ID_TAGS, DataType::Utf8),
    ];

    test_schema(fields)
}
