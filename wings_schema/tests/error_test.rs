//! Integration tests for error handling

use std::array::TryFromSliceError;

use wings_observability::{ErrorExt, StatusCode};
use wings_schema::{DataType, SchemaError};

// ============================================
// SchemaError Construction Tests
// ============================================

#[test]
fn test_duplicate_field_id_error() {
    let error = SchemaError::DuplicateFieldId {
        id: 1,
        f1_name: "field1".to_string(),
        f2_name: "field2".to_string(),
    };

    let message = format!("{}", error);
    assert!(message.contains("duplicate field id 1"));
    assert!(message.contains("field1"));
    assert!(message.contains("field2"));
}

#[test]
fn test_number_deserialization_error() {
    // Create a TryFromSliceError by attempting to convert a slice of wrong size
    // Need to use slice (not array) to get TryFromSliceError
    let bytes: &[u8] = &[1u8, 2u8];
    let result: Result<&[u8; 4], TryFromSliceError> = bytes.try_into();
    let source = result.unwrap_err();

    let error = SchemaError::NumberDeserializationError { source };
    let message = format!("{}", error);
    assert!(message.contains("failed to deserialize number"));
}

#[test]
fn test_string_deserialization_error() {
    // Create invalid UTF-8 bytes
    let invalid_bytes: Vec<u8> = vec![0x80, 0x81, 0x82];
    let result = String::from_utf8(invalid_bytes);
    let source = result.unwrap_err();

    let error = SchemaError::StringDeserializationError { source };
    let message = format!("{}", error);
    assert!(message.contains("failed to deserialize string"));
}

#[test]
fn test_unsupported_data_type_error() {
    let data_type = DataType::Float16;
    let error = SchemaError::UnsupportedDataType { data_type };

    let message = format!("{}", error);
    assert!(message.contains("unsupported data type"));
    assert!(message.contains("Float16"));
}

// ============================================
// From Trait Tests
// ============================================

#[test]
fn test_from_try_from_slice_error() {
    // Need to use slice (not array) to get TryFromSliceError
    let bytes: &[u8] = &[1u8, 2u8];
    let result: Result<&[u8; 4], TryFromSliceError> = bytes.try_into();
    let source = result.unwrap_err();

    let error: SchemaError = source.into();
    let message = format!("{}", error);
    assert!(message.contains("failed to deserialize number"));
}

#[test]
fn test_from_from_utf8_error() {
    let invalid_bytes: Vec<u8> = vec![0x80, 0x81, 0x82];
    let result = String::from_utf8(invalid_bytes);
    let source = result.unwrap_err();

    let error: SchemaError = source.into();
    let message = format!("{}", error);
    assert!(message.contains("failed to deserialize string"));
}

// ============================================
// ErrorExt Trait Tests
// ============================================

#[test]
fn test_status_code_duplicate_field_id() {
    let error = SchemaError::DuplicateFieldId {
        id: 1,
        f1_name: "field1".to_string(),
        f2_name: "field2".to_string(),
    };

    assert_eq!(error.status_code(), StatusCode::DuplicateField);
}

#[test]
fn test_status_code_number_deserialization_error() {
    let bytes: &[u8] = &[1u8, 2u8];
    let result: Result<&[u8; 4], TryFromSliceError> = bytes.try_into();
    let source = result.unwrap_err();

    let error = SchemaError::NumberDeserializationError { source };
    assert_eq!(error.status_code(), StatusCode::Schema);
}

#[test]
fn test_status_code_string_deserialization_error() {
    let invalid_bytes: Vec<u8> = vec![0x80, 0x81, 0x82];
    let result = String::from_utf8(invalid_bytes);
    let source = result.unwrap_err();

    let error = SchemaError::StringDeserializationError { source };
    assert_eq!(error.status_code(), StatusCode::Schema);
}

#[test]
fn test_status_code_unsupported_data_type() {
    let error = SchemaError::UnsupportedDataType {
        data_type: DataType::Null,
    };
    assert_eq!(error.status_code(), StatusCode::DataType);
}
