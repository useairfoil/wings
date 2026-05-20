//! Integration tests for Schema and Field functionality

use std::{collections::HashMap, sync::Arc};

mod common;

use common::*;
use wings_schema::{
    DataType, Field, Fields, SchemaBuilder, SchemaError, SchemaRef, SchemaVisitor, TimeUnit,
    schema_without_partition_field,
    visitor::{visit_field, visit_schema},
};

// ============================================
// SchemaBuilder Tests
// ============================================

#[test]
fn test_schema_builder_basic() {
    let fields = primitive_fields();
    let schema = SchemaBuilder::new(fields.clone()).build().unwrap();

    assert_eq!(schema.fields.len(), 6);
    assert!(schema.metadata.is_empty());

    // Verify all fields are present
    let field_names: Vec<&str> = schema.fields_iter().map(|f| f.name()).collect();
    assert!(field_names.contains(&"name"));
    assert!(field_names.contains(&"age"));
    assert!(field_names.contains(&"email"));
}

#[test]
fn test_schema_builder_with_metadata() {
    let fields = primitive_fields();
    let mut metadata = HashMap::new();
    metadata.insert("key1".to_string(), "value1".to_string());
    metadata.insert("key2".to_string(), "value2".to_string());

    let schema = SchemaBuilder::new(fields)
        .with_metadata(metadata.clone())
        .build()
        .unwrap();

    assert_eq!(schema.metadata.len(), 2);
    assert_eq!(schema.metadata.get("key1"), Some(&"value1".to_string()));
    assert_eq!(schema.metadata.get("key2"), Some(&"value2".to_string()));
}

#[test]
fn test_schema_builder_with_field_refs() {
    let fields: Vec<Arc<Field>> = primitive_fields().into_iter().map(Arc::new).collect();

    let schema = SchemaBuilder::new(fields).build().unwrap();
    assert_eq!(schema.fields.len(), 6);
}

#[test]
fn test_schema_builder_duplicate_field_ids() {
    let fields = vec![
        test_field("field1", 1, DataType::Utf8, false),
        test_field("field2", 1, DataType::Int32, true), // Same ID
    ];

    let result = SchemaBuilder::new(fields).build();
    assert!(matches!(result, Err(SchemaError::DuplicateFieldId { .. })));
}

#[test]
fn test_schema_builder_from_array() {
    let field1 = Arc::new(test_field("field1", 1, DataType::Utf8, false));
    let field2 = Arc::new(test_field("field2", 2, DataType::Int32, true));

    // Test building from array
    let schema = SchemaBuilder::new([field1.clone(), field2.clone()])
        .build()
        .unwrap();
    assert_eq!(schema.fields.len(), 2);
}

#[test]
fn test_schema_builder_from_slice() {
    let field1 = Arc::new(test_field("field1", 1, DataType::Utf8, false));
    let field2 = Arc::new(test_field("field2", 2, DataType::Int32, true));
    let slice: &[Arc<Field>] = &[field1.clone(), field2.clone()];

    let schema = SchemaBuilder::new(slice).build().unwrap();
    assert_eq!(schema.fields.len(), 2);
}

// ============================================
// Schema Tests
// ============================================

#[test]
fn test_schema_with_metadata() {
    let fields = primitive_fields();
    let schema = test_schema(fields);

    let mut new_metadata = HashMap::new();
    new_metadata.insert("version".to_string(), "1.0".to_string());

    let schema_with_meta = schema.with_metadata(new_metadata.clone());
    assert_eq!(
        schema_with_meta.metadata.get("version"),
        Some(&"1.0".to_string())
    );
}

#[test]
fn test_schema_fields_iter() {
    let fields = primitive_fields();
    let schema = test_schema(fields);

    let count = schema.fields_iter().count();
    assert_eq!(count, 6);
}

#[test]
fn test_schema_field_by_id() {
    let fields = primitive_fields();
    let schema = test_schema(fields);

    let field = schema.field_by_id(FIELD_ID_NAME).unwrap();
    assert_eq!(field.name(), "name");
    assert_eq!(field.id, FIELD_ID_NAME);

    let not_found = schema.field_by_id(999);
    assert!(not_found.is_none());
}

#[test]
fn test_schema_root_field_by_id() {
    let schema = nested_schema();

    let root_field = schema.root_field_by_id(FIELD_ID_ADDRESS).unwrap();
    assert_eq!(root_field.name(), "address");

    assert!(schema.field_by_id(FIELD_ID_STREET).is_some());
    assert!(schema.root_field_by_id(FIELD_ID_STREET).is_none());
}

#[test]
fn test_schema_arrow_schema_conversion() {
    let fields = vec![
        test_field("name", 1, DataType::Utf8, false),
        test_field("age", 2, DataType::Int32, true),
    ];
    let schema = test_schema(fields);

    let arrow_schema = schema.arrow_schema();
    assert_eq!(arrow_schema.fields().len(), 2);
    assert_eq!(arrow_schema.field(0).name(), "name");
    assert_eq!(arrow_schema.field(1).name(), "age");
}

#[test]
fn test_schema_ref() {
    let fields = primitive_fields();
    let schema = test_schema(fields);
    let schema_ref: SchemaRef = Arc::new(schema);

    assert_eq!(schema_ref.fields.len(), 6);
}

// ============================================
// Schema without partition field Tests
// ============================================

#[test]
fn test_schema_without_partition_field_with_none() {
    let fields = primitive_fields();
    let schema = test_schema(fields);

    let result = schema_without_partition_field(&schema, None);
    assert_eq!(result.fields.len(), 6);
}

#[test]
fn test_schema_without_partition_field_removes_field() {
    let fields = primitive_fields();
    let schema = test_schema(fields);

    let result = schema_without_partition_field(&schema, Some(FIELD_ID_AGE));
    assert_eq!(result.fields.len(), 5);

    let field_names: Vec<&str> = result.fields_iter().map(|f| f.name()).collect();
    assert!(!field_names.contains(&"age"));
    assert!(field_names.contains(&"name"));
}

#[test]
fn test_schema_without_partition_field_nonexistent() {
    let fields = primitive_fields();
    let schema = test_schema(fields);

    let result = schema_without_partition_field(&schema, Some(999));
    assert_eq!(result.fields.len(), 6);
}

// ============================================
// Field Tests
// ============================================

#[test]
fn test_field_creation() {
    let field = Field::new("test_field", 1, DataType::Utf8, false);
    assert_eq!(field.name(), "test_field");
    assert_eq!(field.id, 1);
    assert_eq!(field.data_type(), &DataType::Utf8);
    assert!(!field.is_nullable());
    assert!(field.metadata().is_empty());
}

#[test]
fn test_field_with_metadata() {
    let mut metadata = HashMap::new();
    metadata.insert("description".to_string(), "A test field".to_string());

    let field = Field::new("test", 1, DataType::Int32, true).with_metadata(metadata.clone());

    assert_eq!(
        field.metadata().get("description"),
        Some(&"A test field".to_string())
    );
}

#[test]
fn test_field_into_arrow_field() {
    let field = Field::new("test", 42, DataType::Utf8, true);
    let arrow_field = field.into_arrow_field();

    assert_eq!(arrow_field.name(), "test");
    assert!(arrow_field.is_nullable());
    assert_eq!(
        arrow_field.metadata().get("PARQUET:field_id"),
        Some(&"42".to_string())
    );
}

#[test]
fn test_field_to_arrow_field() {
    let field = Field::new("test", 42, DataType::Int64, false);
    let arrow_field = field.to_arrow_field();

    assert_eq!(arrow_field.name(), "test");
    assert!(!arrow_field.is_nullable());
}

// ============================================
// Fields Collection Tests
// ============================================

#[test]
fn test_fields_from_vec() {
    let fields_vec = vec![
        test_field("field1", 1, DataType::Utf8, false),
        test_field("field2", 2, DataType::Int32, true),
    ];

    let fields: Fields = fields_vec.into();
    assert_eq!(fields.len(), 2);
}

#[test]
fn test_fields_from_vec_refs() {
    let field1 = Arc::new(test_field("field1", 1, DataType::Utf8, false));
    let field2 = Arc::new(test_field("field2", 2, DataType::Int32, true));

    let fields_vec = vec![field1, field2];
    let fields: Fields = fields_vec.into();
    assert_eq!(fields.len(), 2);
}

#[test]
fn test_fields_from_iterator() {
    let fields: Fields = primitive_fields().into_iter().collect();
    assert_eq!(fields.len(), 6);
}

#[test]
fn test_fields_from_field_refs_iterator() {
    let field_refs: Vec<Arc<Field>> = primitive_fields().into_iter().map(Arc::new).collect();

    let fields: Fields = field_refs.into_iter().collect();
    assert_eq!(fields.len(), 6);
}

#[test]
fn test_fields_deref() {
    let fields: Fields = primitive_fields().into();
    assert_eq!(fields.len(), 6);
    assert!(!fields.is_empty());
}

#[test]
fn test_fields_empty() {
    let fields: Fields = Vec::<Field>::new().into();
    assert!(fields.is_empty());
}

#[test]
fn test_fields_into_iter() {
    let fields: Fields = primitive_fields().into();
    let count = fields.into_iter().count();
    assert_eq!(count, 6);
}

#[test]
fn test_fields_ref_into_iter() {
    let fields: Fields = primitive_fields().into();
    let count = (&fields).into_iter().count();
    assert_eq!(count, 6);
}

// ============================================
// Complex Schema Tests
// ============================================

#[test]
fn test_nested_schema_building() {
    let schema = nested_schema();
    assert_eq!(schema.fields.len(), 4);

    // Check address field
    let address_field = schema.field_by_id(FIELD_ID_ADDRESS).unwrap();
    assert_eq!(address_field.name(), "address");
    assert!(matches!(address_field.data_type(), DataType::Struct(_)));

    // Check tags field (list)
    let tags_field = schema.field_by_id(FIELD_ID_TAGS).unwrap();
    assert_eq!(tags_field.name(), "tags");
    assert!(matches!(tags_field.data_type(), DataType::List(_)));
}

#[test]
fn test_schema_with_all_data_types() {
    let fields = vec![
        test_field("null_field", 1, DataType::Null, true),
        test_field("bool_field", 2, DataType::Boolean, true),
        test_field("int8_field", 3, DataType::Int8, true),
        test_field("int16_field", 4, DataType::Int16, true),
        test_field("int32_field", 5, DataType::Int32, true),
        test_field("int64_field", 6, DataType::Int64, true),
        test_field("uint8_field", 7, DataType::UInt8, true),
        test_field("uint16_field", 8, DataType::UInt16, true),
        test_field("uint32_field", 9, DataType::UInt32, true),
        test_field("uint64_field", 10, DataType::UInt64, true),
        test_field("float16_field", 11, DataType::Float16, true),
        test_field("float32_field", 12, DataType::Float32, true),
        test_field("float64_field", 13, DataType::Float64, true),
        test_field(
            "timestamp_field",
            14,
            DataType::Timestamp(TimeUnit::Second, None),
            true,
        ),
        test_field("date32_field", 15, DataType::Date32, true),
        test_field("date64_field", 16, DataType::Date64, true),
        test_field(
            "duration_field",
            17,
            DataType::Duration(TimeUnit::Millisecond),
            true,
        ),
        test_field("binary_field", 18, DataType::Binary, true),
        test_field("utf8_field", 19, DataType::Utf8, true),
    ];

    let schema = test_schema(fields);
    assert_eq!(schema.fields.len(), 19);
}

// ============================================
// SchemaVisitor Tests (in lib.rs)
// ============================================

#[derive(Debug, Default)]
struct FieldCounter {
    count: usize,
    list_count: usize,
    struct_count: usize,
}

impl SchemaVisitor for FieldCounter {
    type Error = SchemaError;

    fn field(&mut self, _field: &Arc<Field>) -> Result<(), Self::Error> {
        self.count += 1;
        Ok(())
    }

    fn before_list_element(&mut self, _field: &Arc<Field>) -> Result<(), Self::Error> {
        self.list_count += 1;
        Ok(())
    }

    fn after_list_element(&mut self, _field: &Arc<Field>) -> Result<(), Self::Error> {
        Ok(())
    }

    fn before_struct_field(&mut self, _field: &Arc<Field>) -> Result<(), Self::Error> {
        self.struct_count += 1;
        Ok(())
    }

    fn after_struct_field(&mut self, _field: &Arc<Field>) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[test]
fn test_visit_schema_basic() {
    let schema = test_schema(primitive_fields());
    let mut counter = FieldCounter::default();

    visit_schema(&schema, &mut counter).unwrap();
    assert_eq!(counter.count, 6);
}

#[test]
fn test_visit_field_basic() {
    let field = Arc::new(test_field("simple", 1, DataType::Int32, true));
    let mut counter = FieldCounter::default();

    visit_field(&field, &mut counter).unwrap();
    assert_eq!(counter.count, 1);
    assert_eq!(counter.list_count, 0);
    assert_eq!(counter.struct_count, 0);
}

#[test]
fn test_visit_field_list() {
    let field = Arc::new(list_field("tags", 1, DataType::Utf8));
    let mut counter = FieldCounter::default();

    visit_field(&field, &mut counter).unwrap();
    assert_eq!(counter.count, 2); // The list field and its item
    assert_eq!(counter.list_count, 1);
}

#[test]
fn test_visit_field_struct() {
    let nested = vec![
        test_field("a", 10, DataType::Utf8, false),
        test_field("b", 11, DataType::Int32, true),
    ];
    let field = Arc::new(struct_field("struct_field", 1, nested));
    let mut counter = FieldCounter::default();

    visit_field(&field, &mut counter).unwrap();
    assert_eq!(counter.count, 3); // The struct field + 2 nested
    assert_eq!(counter.struct_count, 2); // before_struct called for each nested
}
