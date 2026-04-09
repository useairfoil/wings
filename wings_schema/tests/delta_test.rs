//! Integration tests for DeltaLake schema conversions

mod common;

use wings_schema::{DataType, Field, SchemaError, TimeUnit};

// ============================================
// Primitive Type Conversion Tests
// ============================================

#[test]
fn test_datatype_boolean_to_delta() {
    let dt = DataType::Boolean;
    let delta_dt: deltalake_core::DataType = dt.try_into().unwrap();
    assert!(matches!(
        delta_dt,
        deltalake_core::DataType::Primitive(deltalake_core::PrimitiveType::Boolean)
    ));
}

#[test]
fn test_datatype_int8_to_delta() {
    let dt = DataType::Int8;
    let delta_dt: deltalake_core::DataType = dt.try_into().unwrap();
    assert!(matches!(
        delta_dt,
        deltalake_core::DataType::Primitive(deltalake_core::PrimitiveType::Byte)
    ));
}

#[test]
fn test_datatype_int16_to_delta() {
    let dt = DataType::Int16;
    let delta_dt: deltalake_core::DataType = dt.try_into().unwrap();
    assert!(matches!(
        delta_dt,
        deltalake_core::DataType::Primitive(deltalake_core::PrimitiveType::Short)
    ));
}

#[test]
fn test_datatype_int32_to_delta() {
    let dt = DataType::Int32;
    let delta_dt: deltalake_core::DataType = dt.try_into().unwrap();
    assert!(matches!(
        delta_dt,
        deltalake_core::DataType::Primitive(deltalake_core::PrimitiveType::Integer)
    ));
}

#[test]
fn test_datatype_int64_to_delta() {
    let dt = DataType::Int64;
    let delta_dt: deltalake_core::DataType = dt.try_into().unwrap();
    assert!(matches!(
        delta_dt,
        deltalake_core::DataType::Primitive(deltalake_core::PrimitiveType::Long)
    ));
}

#[test]
fn test_datatype_uint8_to_delta() {
    let dt = DataType::UInt8;
    let delta_dt: deltalake_core::DataType = dt.try_into().unwrap();
    // UInt8 maps to Byte in Delta
    assert!(matches!(
        delta_dt,
        deltalake_core::DataType::Primitive(deltalake_core::PrimitiveType::Byte)
    ));
}

#[test]
fn test_datatype_uint16_to_delta() {
    let dt = DataType::UInt16;
    let delta_dt: deltalake_core::DataType = dt.try_into().unwrap();
    // UInt16 maps to Short in Delta
    assert!(matches!(
        delta_dt,
        deltalake_core::DataType::Primitive(deltalake_core::PrimitiveType::Short)
    ));
}

#[test]
fn test_datatype_uint32_to_delta() {
    let dt = DataType::UInt32;
    let delta_dt: deltalake_core::DataType = dt.try_into().unwrap();
    // UInt32 maps to Integer in Delta
    assert!(matches!(
        delta_dt,
        deltalake_core::DataType::Primitive(deltalake_core::PrimitiveType::Integer)
    ));
}

#[test]
fn test_datatype_uint64_to_delta() {
    let dt = DataType::UInt64;
    let delta_dt: deltalake_core::DataType = dt.try_into().unwrap();
    // UInt64 maps to Long in Delta
    assert!(matches!(
        delta_dt,
        deltalake_core::DataType::Primitive(deltalake_core::PrimitiveType::Long)
    ));
}

#[test]
fn test_datatype_date32_to_delta() {
    let dt = DataType::Date32;
    let delta_dt: deltalake_core::DataType = dt.try_into().unwrap();
    assert!(matches!(
        delta_dt,
        deltalake_core::DataType::Primitive(deltalake_core::PrimitiveType::Date)
    ));
}

#[test]
fn test_datatype_date64_to_delta() {
    let dt = DataType::Date64;
    let delta_dt: deltalake_core::DataType = dt.try_into().unwrap();
    // Date64 also maps to Date in Delta
    assert!(matches!(
        delta_dt,
        deltalake_core::DataType::Primitive(deltalake_core::PrimitiveType::Date)
    ));
}

#[test]
fn test_datatype_binary_to_delta() {
    let dt = DataType::Binary;
    let delta_dt: deltalake_core::DataType = dt.try_into().unwrap();
    assert!(matches!(
        delta_dt,
        deltalake_core::DataType::Primitive(deltalake_core::PrimitiveType::Binary)
    ));
}

#[test]
fn test_datatype_utf8_to_delta() {
    let dt = DataType::Utf8;
    let delta_dt: deltalake_core::DataType = dt.try_into().unwrap();
    assert!(matches!(
        delta_dt,
        deltalake_core::DataType::Primitive(deltalake_core::PrimitiveType::String)
    ));
}

// ============================================
// Timestamp Conversion Tests
// ============================================

#[test]
fn test_datatype_timestamp_microsecond_utc_to_delta() {
    let dt = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    let delta_dt: deltalake_core::DataType = dt.try_into().unwrap();
    assert!(matches!(
        delta_dt,
        deltalake_core::DataType::Primitive(deltalake_core::PrimitiveType::Timestamp)
    ));
}

#[test]
fn test_datatype_timestamp_nanosecond_utc_to_delta() {
    let dt = DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()));
    let delta_dt: deltalake_core::DataType = dt.try_into().unwrap();
    assert!(matches!(
        delta_dt,
        deltalake_core::DataType::Primitive(deltalake_core::PrimitiveType::Timestamp)
    ));
}

#[test]
fn test_datatype_timestamp_microsecond_ntz_to_delta() {
    let dt = DataType::Timestamp(TimeUnit::Microsecond, None);
    let delta_dt: deltalake_core::DataType = dt.try_into().unwrap();
    assert!(matches!(
        delta_dt,
        deltalake_core::DataType::Primitive(deltalake_core::PrimitiveType::TimestampNtz)
    ));
}

#[test]
fn test_datatype_timestamp_nanosecond_ntz_to_delta() {
    let dt = DataType::Timestamp(TimeUnit::Nanosecond, None);
    let delta_dt: deltalake_core::DataType = dt.try_into().unwrap();
    assert!(matches!(
        delta_dt,
        deltalake_core::DataType::Primitive(deltalake_core::PrimitiveType::TimestampNtz)
    ));
}

#[test]
fn test_datatype_timestamp_case_insensitive_utc() {
    let dt = DataType::Timestamp(TimeUnit::Microsecond, Some("utc".into()));
    let delta_dt: deltalake_core::DataType = dt.try_into().unwrap();
    assert!(matches!(
        delta_dt,
        deltalake_core::DataType::Primitive(deltalake_core::PrimitiveType::Timestamp)
    ));
}

// ============================================
// List Conversion Tests
// ============================================

#[test]
fn test_datatype_list_to_delta() {
    use std::sync::Arc;

    let item_field = Arc::new(Field::new("item", 1, DataType::Int32, true));
    let dt = DataType::List(item_field);
    let delta_dt: deltalake_core::DataType = dt.try_into().unwrap();

    match delta_dt {
        deltalake_core::DataType::Array(array_type) => {
            assert!(matches!(
                array_type.element_type,
                deltalake_core::DataType::Primitive(deltalake_core::PrimitiveType::Integer)
            ));
            assert!(array_type.contains_null);
        }
        _ => panic!("Expected Array type"),
    }
}

#[test]
fn test_datatype_list_non_nullable_to_delta() {
    use std::sync::Arc;

    let item_field = Arc::new(Field::new("item", 1, DataType::Utf8, false));
    let dt = DataType::List(item_field);
    let delta_dt: deltalake_core::DataType = dt.try_into().unwrap();

    match delta_dt {
        deltalake_core::DataType::Array(array_type) => {
            assert!(matches!(
                array_type.element_type,
                deltalake_core::DataType::Primitive(deltalake_core::PrimitiveType::String)
            ));
            assert!(!array_type.contains_null);
        }
        _ => panic!("Expected Array type"),
    }
}

// ============================================
// Struct Conversion Tests
// ============================================

#[test]
fn test_datatype_struct_to_delta() {
    let fields = vec![
        Field::new("name", 1, DataType::Utf8, false),
        Field::new("age", 2, DataType::Int32, true),
    ];
    let dt = DataType::Struct(fields.into());
    let delta_dt: deltalake_core::DataType = dt.try_into().unwrap();

    match delta_dt {
        deltalake_core::DataType::Struct(struct_type) => {
            let delta_fields: Vec<_> = struct_type.fields().collect();
            assert_eq!(delta_fields.len(), 2);
            assert_eq!(delta_fields[0].name(), "name");
            assert_eq!(delta_fields[1].name(), "age");
        }
        _ => panic!("Expected Struct type"),
    }
}

#[test]
fn test_datatype_nested_struct_to_delta() {
    let inner_fields = vec![Field::new("x", 10, DataType::Int64, false)];
    let point_field = Field::new("location", 2, DataType::Struct(inner_fields.into()), true);

    let fields = vec![Field::new("id", 1, DataType::Int64, false), point_field];
    let dt = DataType::Struct(fields.into());
    let delta_dt: deltalake_core::DataType = dt.try_into().unwrap();

    match delta_dt {
        deltalake_core::DataType::Struct(struct_type) => {
            let delta_fields: Vec<_> = struct_type.fields().collect();
            assert_eq!(delta_fields.len(), 2);
            assert_eq!(delta_fields[0].name(), "id");
            assert_eq!(delta_fields[1].name(), "location");
        }
        _ => panic!("Expected Struct type"),
    }
}

// ============================================
// Field Conversion Tests
// ============================================

#[test]
fn test_field_to_delta() {
    let field = Field::new("test_field", 1, DataType::Int32, false);
    let delta_field: deltalake_core::StructField = (&field).try_into().unwrap();

    assert_eq!(delta_field.name(), "test_field");
    assert!(!delta_field.is_nullable());
    assert!(matches!(
        delta_field.data_type(),
        deltalake_core::DataType::Primitive(deltalake_core::PrimitiveType::Integer)
    ));
}

#[test]
fn test_field_to_delta_nullable() {
    let field = Field::new("nullable_field", 1, DataType::Utf8, true);
    let delta_field: deltalake_core::StructField = field.try_into().unwrap();

    assert_eq!(delta_field.name(), "nullable_field");
    assert!(delta_field.is_nullable());
    assert!(matches!(
        delta_field.data_type(),
        deltalake_core::DataType::Primitive(deltalake_core::PrimitiveType::String)
    ));
}

// ============================================
// Unsupported Type Tests
// ============================================

#[test]
fn test_datatype_null_unsupported() {
    let dt = DataType::Null;
    let result: Result<deltalake_core::DataType, SchemaError> = dt.try_into();
    assert!(matches!(
        result,
        Err(SchemaError::UnsupportedDataType { .. })
    ));
}

#[test]
fn test_datatype_float16_unsupported() {
    let dt = DataType::Float16;
    let result: Result<deltalake_core::DataType, SchemaError> = dt.try_into();
    assert!(matches!(
        result,
        Err(SchemaError::UnsupportedDataType { .. })
    ));
}

#[test]
fn test_datatype_float32_unsupported() {
    // Float32 is also unsupported according to the current implementation
    let dt = DataType::Float32;
    let result: Result<deltalake_core::DataType, SchemaError> = dt.try_into();
    assert!(matches!(
        result,
        Err(SchemaError::UnsupportedDataType { .. })
    ));
}

#[test]
fn test_datatype_duration_unsupported() {
    let dt = DataType::Duration(TimeUnit::Millisecond);
    let result: Result<deltalake_core::DataType, SchemaError> = dt.try_into();
    assert!(matches!(
        result,
        Err(SchemaError::UnsupportedDataType { .. })
    ));
}

#[test]
fn test_datatype_timestamp_second_unsupported() {
    // Second precision timestamp is unsupported
    let dt = DataType::Timestamp(TimeUnit::Second, None);
    let result: Result<deltalake_core::DataType, SchemaError> = dt.try_into();
    assert!(matches!(
        result,
        Err(SchemaError::UnsupportedDataType { .. })
    ));
}

#[test]
fn test_datatype_timestamp_millisecond_unsupported() {
    // Millisecond precision timestamp is unsupported
    let dt = DataType::Timestamp(TimeUnit::Millisecond, None);
    let result: Result<deltalake_core::DataType, SchemaError> = dt.try_into();
    assert!(matches!(
        result,
        Err(SchemaError::UnsupportedDataType { .. })
    ));
}

#[test]
fn test_datatype_timestamp_non_utc_timezone_unsupported() {
    // Non-UTC timezone is unsupported
    let dt = DataType::Timestamp(TimeUnit::Microsecond, Some("America/New_York".into()));
    let result: Result<deltalake_core::DataType, SchemaError> = dt.try_into();
    assert!(matches!(
        result,
        Err(SchemaError::UnsupportedDataType { .. })
    ));
}

#[test]
fn test_datatype_struct_with_unsupported_field() {
    let fields = vec![
        Field::new("good", 1, DataType::Int32, false),
        Field::new("bad", 2, DataType::Float16, true),
    ];
    let dt = DataType::Struct(fields.into());
    let result: Result<deltalake_core::DataType, SchemaError> = dt.try_into();
    assert!(matches!(
        result,
        Err(SchemaError::UnsupportedDataType { .. })
    ));
}

// ============================================
// Reference Conversion Tests (owned vs borrowed)
// ============================================

#[test]
fn test_datatype_ref_conversion() {
    let dt = DataType::Int32;
    let delta_dt: deltalake_core::DataType = (&dt).try_into().unwrap();
    assert!(matches!(
        delta_dt,
        deltalake_core::DataType::Primitive(deltalake_core::PrimitiveType::Integer)
    ));
}

#[test]
fn test_datatype_owned_conversion() {
    let dt = DataType::Int32;
    let delta_dt: deltalake_core::DataType = dt.try_into().unwrap();
    assert!(matches!(
        delta_dt,
        deltalake_core::DataType::Primitive(deltalake_core::PrimitiveType::Integer)
    ));
}

#[test]
fn test_field_ref_conversion() {
    let field = Field::new("test", 1, DataType::Utf8, false);
    let delta_field: deltalake_core::StructField = (&field).try_into().unwrap();
    assert_eq!(delta_field.name(), "test");
}

#[test]
fn test_field_owned_conversion() {
    let field = Field::new("test", 1, DataType::Utf8, false);
    let delta_field: deltalake_core::StructField = field.try_into().unwrap();
    assert_eq!(delta_field.name(), "test");
}
