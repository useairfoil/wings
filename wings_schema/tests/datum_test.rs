//! Integration tests for Datum serialization and deserialization

mod common;

use wings_schema::{DataType, Datum, Field, SchemaError, TimeUnit};

// ============================================
// Datum Factory Method Tests
// ============================================

#[test]
fn test_datum_null() {
    let datum = Datum::Null;
    assert_eq!(datum.data_type(), DataType::Null);
}

#[test]
fn test_datum_bool() {
    let datum = Datum::bool(true);
    assert_eq!(datum, Datum::Boolean(true));
    assert_eq!(datum.data_type(), DataType::Boolean);

    let datum = Datum::bool(false);
    assert_eq!(datum, Datum::Boolean(false));
}

#[test]
fn test_datum_u8() {
    let datum = Datum::u8(42);
    assert_eq!(datum, Datum::UInt8(42));
    assert_eq!(datum.data_type(), DataType::UInt8);
}

#[test]
fn test_datum_i8() {
    let datum = Datum::i8(-42);
    assert_eq!(datum, Datum::Int8(-42));
    assert_eq!(datum.data_type(), DataType::Int8);
}

#[test]
fn test_datum_u16() {
    let datum = Datum::u16(1000);
    assert_eq!(datum, Datum::UInt16(1000));
    assert_eq!(datum.data_type(), DataType::UInt16);
}

#[test]
fn test_datum_i16() {
    let datum = Datum::i16(-1000);
    assert_eq!(datum, Datum::Int16(-1000));
    assert_eq!(datum.data_type(), DataType::Int16);
}

#[test]
fn test_datum_u32() {
    let datum = Datum::u32(100000);
    assert_eq!(datum, Datum::UInt32(100000));
    assert_eq!(datum.data_type(), DataType::UInt32);
}

#[test]
fn test_datum_i32() {
    let datum = Datum::i32(-100000);
    assert_eq!(datum, Datum::Int32(-100000));
    assert_eq!(datum.data_type(), DataType::Int32);
}

#[test]
fn test_datum_u64() {
    let datum = Datum::u64(10000000000);
    assert_eq!(datum, Datum::UInt64(10000000000));
    assert_eq!(datum.data_type(), DataType::UInt64);
}

#[test]
fn test_datum_i64() {
    let datum = Datum::i64(-10000000000);
    assert_eq!(datum, Datum::Int64(-10000000000));
    assert_eq!(datum.data_type(), DataType::Int64);
}

#[test]
fn test_datum_f32() {
    let datum = Datum::f32(1.5);
    assert!(matches!(datum, Datum::Float32(v) if (v - 1.5).abs() < 0.001));
    assert_eq!(datum.data_type(), DataType::Float32);
}

#[test]
fn test_datum_f64() {
    let datum = Datum::f64(2.5);
    assert!(matches!(datum, Datum::Float64(v) if (v - 2.5).abs() < 0.0001));
    assert_eq!(datum.data_type(), DataType::Float64);
}

#[test]
fn test_datum_utf8() {
    let datum = Datum::utf8("hello world");
    assert_eq!(datum, Datum::Utf8("hello world".to_string()));
    assert_eq!(datum.data_type(), DataType::Utf8);

    let datum = Datum::utf8(String::from("test"));
    assert_eq!(datum, Datum::Utf8("test".to_string()));
}

#[test]
fn test_datum_binary() {
    let bytes = vec![0u8, 1u8, 2u8, 3u8];
    let datum = Datum::binary(bytes.clone());
    assert_eq!(datum, Datum::Binary(bytes));
    assert_eq!(datum.data_type(), DataType::Binary);
}

#[test]
fn test_datum_date32() {
    let datum = Datum::date32(1000);
    assert_eq!(datum, Datum::Date32(1000));
    assert_eq!(datum.data_type(), DataType::Date32);
}

#[test]
fn test_datum_date64() {
    let datum = Datum::date64(1000000);
    assert_eq!(datum, Datum::Date64(1000000));
    assert_eq!(datum.data_type(), DataType::Date64);
}

#[test]
fn test_datum_timestamp() {
    let datum = Datum::timestamp(TimeUnit::Second, 1000000);
    assert_eq!(datum, Datum::Timestamp(TimeUnit::Second, 1000000));
    assert_eq!(
        datum.data_type(),
        DataType::Timestamp(TimeUnit::Second, None)
    );

    let datum = Datum::timestamp(TimeUnit::Millisecond, 1000000000);
    assert_eq!(
        datum.data_type(),
        DataType::Timestamp(TimeUnit::Millisecond, None)
    );
}

#[test]
fn test_datum_duration() {
    let datum = Datum::duration(TimeUnit::Millisecond, 5000);
    assert_eq!(datum, Datum::Duration(TimeUnit::Millisecond, 5000));
    assert_eq!(datum.data_type(), DataType::Duration(TimeUnit::Millisecond));
}

// ============================================
// Serialization Tests (to_bytes)
// ============================================

#[test]
fn test_datum_to_bytes_null() {
    let datum = Datum::Null;
    let bytes = datum.to_bytes();
    assert!(bytes.is_empty());
}

#[test]
fn test_datum_to_bytes_bool_true() {
    let datum = Datum::bool(true);
    let bytes = datum.to_bytes();
    assert_eq!(bytes, vec![1u8]);
}

#[test]
fn test_datum_to_bytes_bool_false() {
    let datum = Datum::bool(false);
    let bytes = datum.to_bytes();
    assert_eq!(bytes, vec![0u8]);
}

#[test]
fn test_datum_to_bytes_u8() {
    let datum = Datum::u8(42);
    let bytes = datum.to_bytes();
    assert_eq!(bytes, vec![42u8]);
}

#[test]
fn test_datum_to_bytes_i8() {
    let datum = Datum::i8(-42);
    let bytes = datum.to_bytes();
    assert_eq!(bytes, (-42i8).to_le_bytes().to_vec());
}

#[test]
fn test_datum_to_bytes_u16() {
    let datum = Datum::u16(1000);
    let bytes = datum.to_bytes();
    assert_eq!(bytes, 1000u16.to_le_bytes().to_vec());
}

#[test]
fn test_datum_to_bytes_i16() {
    let datum = Datum::i16(-1000);
    let bytes = datum.to_bytes();
    assert_eq!(bytes, (-1000i16).to_le_bytes().to_vec());
}

#[test]
fn test_datum_to_bytes_u32() {
    let datum = Datum::u32(100000);
    let bytes = datum.to_bytes();
    assert_eq!(bytes, 100000u32.to_le_bytes().to_vec());
}

#[test]
fn test_datum_to_bytes_i32() {
    let datum = Datum::i32(-100000);
    let bytes = datum.to_bytes();
    assert_eq!(bytes, (-100000i32).to_le_bytes().to_vec());
}

#[test]
fn test_datum_to_bytes_u64() {
    let datum = Datum::u64(10000000000);
    let bytes = datum.to_bytes();
    assert_eq!(bytes, 10000000000u64.to_le_bytes().to_vec());
}

#[test]
fn test_datum_to_bytes_i64() {
    let datum = Datum::i64(-10000000000);
    let bytes = datum.to_bytes();
    assert_eq!(bytes, (-10000000000i64).to_le_bytes().to_vec());
}

#[test]
fn test_datum_to_bytes_f32() {
    let datum = Datum::f32(1.5);
    let bytes = datum.to_bytes();
    assert_eq!(bytes, 1.5f32.to_le_bytes().to_vec());
}

#[test]
fn test_datum_to_bytes_f64() {
    let datum = Datum::f64(2.5);
    let bytes = datum.to_bytes();
    assert_eq!(bytes, 2.5f64.to_le_bytes().to_vec());
}

#[test]
fn test_datum_to_bytes_utf8() {
    let datum = Datum::utf8("hello");
    let bytes = datum.to_bytes();
    assert_eq!(bytes, vec![104u8, 101u8, 108u8, 108u8, 111u8]); // "hello" in ASCII
}

#[test]
fn test_datum_to_bytes_binary() {
    let datum = Datum::binary(vec![0u8, 1u8, 2u8, 3u8]);
    let bytes = datum.to_bytes();
    assert_eq!(bytes, vec![0u8, 1u8, 2u8, 3u8]);
}

#[test]
fn test_datum_to_bytes_date32() {
    let datum = Datum::date32(1000);
    let bytes = datum.to_bytes();
    assert_eq!(bytes, 1000i32.to_le_bytes().to_vec());
}

#[test]
fn test_datum_to_bytes_date64() {
    let datum = Datum::date64(1000000);
    let bytes = datum.to_bytes();
    assert_eq!(bytes, 1000000i64.to_le_bytes().to_vec());
}

#[test]
fn test_datum_to_bytes_timestamp() {
    let datum = Datum::timestamp(TimeUnit::Second, 1000000);
    let bytes = datum.to_bytes();
    assert_eq!(bytes, 1000000i64.to_le_bytes().to_vec());
}

#[test]
fn test_datum_to_bytes_duration() {
    let datum = Datum::duration(TimeUnit::Millisecond, 5000);
    let bytes = datum.to_bytes();
    assert_eq!(bytes, 5000i64.to_le_bytes().to_vec());
}

// ============================================
// Deserialization Tests (try_from_bytes)
// ============================================

#[test]
fn test_datum_from_bytes_null() {
    let datum = Datum::try_from_bytes(DataType::Null, &[]).unwrap();
    assert_eq!(datum, Datum::Null);
}

#[test]
fn test_datum_from_bytes_bool_true() {
    let datum = Datum::try_from_bytes(DataType::Boolean, &[1u8]).unwrap();
    assert_eq!(datum, Datum::bool(true));
}

#[test]
fn test_datum_from_bytes_bool_false() {
    let datum = Datum::try_from_bytes(DataType::Boolean, &[0u8]).unwrap();
    assert_eq!(datum, Datum::bool(false));
}

#[test]
fn test_datum_from_bytes_bool_nonzero() {
    // Any non-zero value should be true
    let datum = Datum::try_from_bytes(DataType::Boolean, &[255u8]).unwrap();
    assert_eq!(datum, Datum::bool(true));
}

#[test]
fn test_datum_from_bytes_u8() {
    let bytes = vec![42u8];
    let datum = Datum::try_from_bytes(DataType::UInt8, &bytes).unwrap();
    assert_eq!(datum, Datum::u8(42));
}

#[test]
fn test_datum_from_bytes_i8() {
    let bytes = (-42i8).to_le_bytes().to_vec();
    let datum = Datum::try_from_bytes(DataType::Int8, &bytes).unwrap();
    assert_eq!(datum, Datum::i8(-42));
}

#[test]
fn test_datum_from_bytes_u16() {
    let bytes = 1000u16.to_le_bytes().to_vec();
    let datum = Datum::try_from_bytes(DataType::UInt16, &bytes).unwrap();
    assert_eq!(datum, Datum::u16(1000));
}

#[test]
fn test_datum_from_bytes_i16() {
    let bytes = (-1000i16).to_le_bytes().to_vec();
    let datum = Datum::try_from_bytes(DataType::Int16, &bytes).unwrap();
    assert_eq!(datum, Datum::i16(-1000));
}

#[test]
fn test_datum_from_bytes_u32() {
    let bytes = 100000u32.to_le_bytes().to_vec();
    let datum = Datum::try_from_bytes(DataType::UInt32, &bytes).unwrap();
    assert_eq!(datum, Datum::u32(100000));
}

#[test]
fn test_datum_from_bytes_i32() {
    let bytes = (-100000i32).to_le_bytes().to_vec();
    let datum = Datum::try_from_bytes(DataType::Int32, &bytes).unwrap();
    assert_eq!(datum, Datum::i32(-100000));
}

#[test]
fn test_datum_from_bytes_u64() {
    let bytes = 10000000000u64.to_le_bytes().to_vec();
    let datum = Datum::try_from_bytes(DataType::UInt64, &bytes).unwrap();
    assert_eq!(datum, Datum::u64(10000000000));
}

#[test]
fn test_datum_from_bytes_i64() {
    let bytes = (-10000000000i64).to_le_bytes().to_vec();
    let datum = Datum::try_from_bytes(DataType::Int64, &bytes).unwrap();
    assert_eq!(datum, Datum::i64(-10000000000));
}

#[test]
fn test_datum_from_bytes_f32() {
    let bytes = 1.5f32.to_le_bytes().to_vec();
    let datum = Datum::try_from_bytes(DataType::Float32, &bytes).unwrap();
    assert!(matches!(datum, Datum::Float32(v) if (v - 1.5).abs() < 0.001));
}

#[test]
fn test_datum_from_bytes_f64() {
    let bytes = 2.5f64.to_le_bytes().to_vec();
    let datum = Datum::try_from_bytes(DataType::Float64, &bytes).unwrap();
    assert!(matches!(datum, Datum::Float64(v) if (v - 2.5).abs() < 0.0001));
}

#[test]
fn test_datum_from_bytes_utf8() {
    let bytes = vec![104u8, 101u8, 108u8, 108u8, 111u8];
    let datum = Datum::try_from_bytes(DataType::Utf8, &bytes).unwrap();
    assert_eq!(datum, Datum::utf8("hello"));
}

#[test]
fn test_datum_from_bytes_binary() {
    let bytes = vec![0u8, 1u8, 2u8, 3u8];
    let datum = Datum::try_from_bytes(DataType::Binary, &bytes).unwrap();
    assert_eq!(datum, Datum::binary(vec![0u8, 1u8, 2u8, 3u8]));
}

#[test]
fn test_datum_from_bytes_date32() {
    let bytes = 1000i32.to_le_bytes().to_vec();
    let datum = Datum::try_from_bytes(DataType::Date32, &bytes).unwrap();
    assert_eq!(datum, Datum::date32(1000));
}

#[test]
fn test_datum_from_bytes_date64() {
    let bytes = 1000000i64.to_le_bytes().to_vec();
    let datum = Datum::try_from_bytes(DataType::Date64, &bytes).unwrap();
    assert_eq!(datum, Datum::date64(1000000));
}

#[test]
fn test_datum_from_bytes_timestamp() {
    let bytes = 1000000i64.to_le_bytes().to_vec();
    let datum = Datum::try_from_bytes(DataType::Timestamp(TimeUnit::Second, None), &bytes).unwrap();
    assert_eq!(datum, Datum::timestamp(TimeUnit::Second, 1000000));
}

#[test]
fn test_datum_from_bytes_duration() {
    let bytes = 5000i64.to_le_bytes().to_vec();
    let datum = Datum::try_from_bytes(DataType::Duration(TimeUnit::Millisecond), &bytes).unwrap();
    assert_eq!(datum, Datum::duration(TimeUnit::Millisecond, 5000));
}

// ============================================
// Round-trip Tests
// ============================================

#[test]
fn test_datum_roundtrip_bool() {
    let original = Datum::bool(true);
    let bytes = original.to_bytes();
    let deserialized = Datum::try_from_bytes(DataType::Boolean, &bytes).unwrap();
    assert_eq!(original, deserialized);
}

#[test]
fn test_datum_roundtrip_u8() {
    let original = Datum::u8(42);
    let bytes = original.to_bytes();
    let deserialized = Datum::try_from_bytes(DataType::UInt8, &bytes).unwrap();
    assert_eq!(original, deserialized);
}

#[test]
fn test_datum_roundtrip_i8() {
    let original = Datum::i8(-42);
    let bytes = original.to_bytes();
    let deserialized = Datum::try_from_bytes(DataType::Int8, &bytes).unwrap();
    assert_eq!(original, deserialized);
}

#[test]
fn test_datum_roundtrip_u16() {
    let original = Datum::u16(1000);
    let bytes = original.to_bytes();
    let deserialized = Datum::try_from_bytes(DataType::UInt16, &bytes).unwrap();
    assert_eq!(original, deserialized);
}

#[test]
fn test_datum_roundtrip_i16() {
    let original = Datum::i16(-1000);
    let bytes = original.to_bytes();
    let deserialized = Datum::try_from_bytes(DataType::Int16, &bytes).unwrap();
    assert_eq!(original, deserialized);
}

#[test]
fn test_datum_roundtrip_u32() {
    let original = Datum::u32(100000);
    let bytes = original.to_bytes();
    let deserialized = Datum::try_from_bytes(DataType::UInt32, &bytes).unwrap();
    assert_eq!(original, deserialized);
}

#[test]
fn test_datum_roundtrip_i32() {
    let original = Datum::i32(-100000);
    let bytes = original.to_bytes();
    let deserialized = Datum::try_from_bytes(DataType::Int32, &bytes).unwrap();
    assert_eq!(original, deserialized);
}

#[test]
fn test_datum_roundtrip_u64() {
    let original = Datum::u64(10000000000);
    let bytes = original.to_bytes();
    let deserialized = Datum::try_from_bytes(DataType::UInt64, &bytes).unwrap();
    assert_eq!(original, deserialized);
}

#[test]
fn test_datum_roundtrip_i64() {
    let original = Datum::i64(-10000000000);
    let bytes = original.to_bytes();
    let deserialized = Datum::try_from_bytes(DataType::Int64, &bytes).unwrap();
    assert_eq!(original, deserialized);
}

#[test]
fn test_datum_roundtrip_f32() {
    let original = Datum::f32(1.5);
    let bytes = original.to_bytes();
    let deserialized = Datum::try_from_bytes(DataType::Float32, &bytes).unwrap();
    assert!(matches!(deserialized, Datum::Float32(v) if (v - 1.5).abs() < 0.001));
}

#[test]
fn test_datum_roundtrip_f64() {
    let original = Datum::f64(2.5);
    let bytes = original.to_bytes();
    let deserialized = Datum::try_from_bytes(DataType::Float64, &bytes).unwrap();
    assert!(matches!(deserialized, Datum::Float64(v) if (v - 2.5).abs() < 0.0001));
}

#[test]
fn test_datum_roundtrip_utf8() {
    let original = Datum::utf8("hello world");
    let bytes = original.to_bytes();
    let deserialized = Datum::try_from_bytes(DataType::Utf8, &bytes).unwrap();
    assert_eq!(original, deserialized);
}

#[test]
fn test_datum_roundtrip_binary() {
    let original = Datum::binary(vec![0u8, 1u8, 2u8, 3u8]);
    let bytes = original.to_bytes();
    let deserialized = Datum::try_from_bytes(DataType::Binary, &bytes).unwrap();
    assert_eq!(original, deserialized);
}

#[test]
fn test_datum_roundtrip_date32() {
    let original = Datum::date32(1000);
    let bytes = original.to_bytes();
    let deserialized = Datum::try_from_bytes(DataType::Date32, &bytes).unwrap();
    assert_eq!(original, deserialized);
}

#[test]
fn test_datum_roundtrip_date64() {
    let original = Datum::date64(1000000);
    let bytes = original.to_bytes();
    let deserialized = Datum::try_from_bytes(DataType::Date64, &bytes).unwrap();
    assert_eq!(original, deserialized);
}

#[test]
fn test_datum_roundtrip_timestamp() {
    let original = Datum::timestamp(TimeUnit::Second, 1000000);
    let bytes = original.to_bytes();
    let deserialized =
        Datum::try_from_bytes(DataType::Timestamp(TimeUnit::Second, None), &bytes).unwrap();
    assert_eq!(original, deserialized);
}

#[test]
fn test_datum_roundtrip_duration() {
    let original = Datum::duration(TimeUnit::Millisecond, 5000);
    let bytes = original.to_bytes();
    let deserialized =
        Datum::try_from_bytes(DataType::Duration(TimeUnit::Millisecond), &bytes).unwrap();
    assert_eq!(original, deserialized);
}

// ============================================
// Error Tests
// ============================================

#[test]
fn test_datum_from_bytes_unsupported_float16() {
    let bytes = vec![0u8, 0u8]; // 2 bytes for f16
    let result = Datum::try_from_bytes(DataType::Float16, &bytes);
    assert!(matches!(
        result,
        Err(SchemaError::UnsupportedDataType { .. })
    ));
}

#[test]
fn test_datum_from_bytes_unsupported_list() {
    use std::sync::Arc;

    use wings_schema::Field;

    let item_field = Arc::new(Field::new("item", 1, DataType::Int32, true));
    let result = Datum::try_from_bytes(DataType::List(item_field), &[1, 2, 3, 4]);
    assert!(matches!(
        result,
        Err(SchemaError::UnsupportedDataType { .. })
    ));
}

#[test]
fn test_datum_from_bytes_unsupported_struct() {
    let fields = vec![Field::new("a", 1, DataType::Int32, true)];
    let result = Datum::try_from_bytes(DataType::Struct(fields.into()), &[1, 2, 3, 4]);
    assert!(matches!(
        result,
        Err(SchemaError::UnsupportedDataType { .. })
    ));
}

#[test]
fn test_datum_from_bytes_invalid_size_u32() {
    // Too few bytes for u32
    let result = Datum::try_from_bytes(DataType::UInt32, &[1, 2, 3]);
    assert!(matches!(
        result,
        Err(SchemaError::NumberDeserializationError { .. })
    ));
}

#[test]
fn test_datum_from_bytes_invalid_utf8() {
    // Invalid UTF-8 sequence
    let bytes = vec![0x80, 0x81, 0x82];
    let result = Datum::try_from_bytes(DataType::Utf8, &bytes);
    assert!(matches!(
        result,
        Err(SchemaError::StringDeserializationError { .. })
    ));
}
