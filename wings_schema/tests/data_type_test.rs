//! Integration tests for DataType functionality

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType as ArrowDataType, TimeUnit as ArrowTimeUnit};

mod common;

use wings_schema::{DataType, Field, TimeUnit};

// ============================================
// DataType Primitive Tests
// ============================================

#[test]
fn test_datatype_null() {
    let dt = DataType::Null;
    assert!(dt.is_primitive_type());

    let arrow: ArrowDataType = dt.into();
    assert_eq!(arrow, ArrowDataType::Null);
}

#[test]
fn test_datatype_boolean() {
    let dt = DataType::Boolean;
    assert!(dt.is_primitive_type());

    let arrow: ArrowDataType = dt.into();
    assert_eq!(arrow, ArrowDataType::Boolean);
}

#[test]
fn test_datatype_integer_types() {
    let test_cases = vec![
        (DataType::Int8, ArrowDataType::Int8),
        (DataType::Int16, ArrowDataType::Int16),
        (DataType::Int32, ArrowDataType::Int32),
        (DataType::Int64, ArrowDataType::Int64),
        (DataType::UInt8, ArrowDataType::UInt8),
        (DataType::UInt16, ArrowDataType::UInt16),
        (DataType::UInt32, ArrowDataType::UInt32),
        (DataType::UInt64, ArrowDataType::UInt64),
    ];

    for (wings_dt, expected_arrow) in test_cases {
        assert!(wings_dt.is_primitive_type());
        let arrow: ArrowDataType = wings_dt.into();
        assert_eq!(arrow, expected_arrow);
    }
}

#[test]
fn test_datatype_float_types() {
    let test_cases = vec![
        (DataType::Float16, ArrowDataType::Float16),
        (DataType::Float32, ArrowDataType::Float32),
        (DataType::Float64, ArrowDataType::Float64),
    ];

    for (wings_dt, expected_arrow) in test_cases {
        assert!(wings_dt.is_primitive_type());
        let arrow: ArrowDataType = wings_dt.into();
        assert_eq!(arrow, expected_arrow);
    }
}

#[test]
fn test_datatype_binary() {
    let dt = DataType::Binary;
    assert!(dt.is_primitive_type());

    let arrow: ArrowDataType = dt.into();
    assert_eq!(arrow, ArrowDataType::Binary);
}

#[test]
fn test_datatype_utf8() {
    let dt = DataType::Utf8;
    assert!(dt.is_primitive_type());

    let arrow: ArrowDataType = dt.into();
    assert_eq!(arrow, ArrowDataType::Utf8);
}

// ============================================
// TimeUnit Tests
// ============================================

#[test]
fn test_timeunit_conversions() {
    let test_cases = vec![
        (TimeUnit::Second, ArrowTimeUnit::Second),
        (TimeUnit::Millisecond, ArrowTimeUnit::Millisecond),
        (TimeUnit::Microsecond, ArrowTimeUnit::Microsecond),
        (TimeUnit::Nanosecond, ArrowTimeUnit::Nanosecond),
    ];

    for (wings_tu, expected_arrow) in test_cases {
        let arrow: ArrowTimeUnit = wings_tu.into();
        assert_eq!(arrow, expected_arrow);
    }
}

// ============================================
// Timestamp Tests
// ============================================

#[test]
fn test_datatype_timestamp_no_timezone() {
    let dt = DataType::Timestamp(TimeUnit::Second, None);
    assert!(dt.is_primitive_type());

    let arrow: ArrowDataType = dt.clone().into();
    assert_eq!(arrow, ArrowDataType::Timestamp(ArrowTimeUnit::Second, None));
}

#[test]
fn test_datatype_timestamp_with_timezone() {
    let tz: Option<Arc<str>> = Some("UTC".into());
    let dt = DataType::Timestamp(TimeUnit::Millisecond, tz.clone());
    assert!(dt.is_primitive_type());

    let arrow: ArrowDataType = dt.clone().into();
    assert_eq!(
        arrow,
        ArrowDataType::Timestamp(ArrowTimeUnit::Millisecond, tz)
    );
}

#[test]
fn test_datatype_timestamp_all_units() {
    let units = vec![
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ];

    for unit in units {
        let dt = DataType::Timestamp(unit, None);
        let arrow: ArrowDataType = dt.into();

        let expected = match unit {
            TimeUnit::Second => ArrowTimeUnit::Second,
            TimeUnit::Millisecond => ArrowTimeUnit::Millisecond,
            TimeUnit::Microsecond => ArrowTimeUnit::Microsecond,
            TimeUnit::Nanosecond => ArrowTimeUnit::Nanosecond,
        };

        assert_eq!(arrow, ArrowDataType::Timestamp(expected, None));
    }
}

// ============================================
// Date Tests
// ============================================

#[test]
fn test_datatype_date32() {
    let dt = DataType::Date32;
    assert!(dt.is_primitive_type());

    let arrow: ArrowDataType = dt.into();
    assert_eq!(arrow, ArrowDataType::Date32);
}

#[test]
fn test_datatype_date64() {
    let dt = DataType::Date64;
    assert!(dt.is_primitive_type());

    let arrow: ArrowDataType = dt.into();
    assert_eq!(arrow, ArrowDataType::Date64);
}

// ============================================
// Duration Tests
// ============================================

#[test]
fn test_datatype_duration() {
    let dt = DataType::Duration(TimeUnit::Millisecond);
    assert!(dt.is_primitive_type());

    let arrow: ArrowDataType = dt.into();
    assert_eq!(arrow, ArrowDataType::Duration(ArrowTimeUnit::Millisecond));
}

#[test]
fn test_datatype_duration_all_units() {
    let units = vec![
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ];

    for unit in units {
        let dt = DataType::Duration(unit);
        let arrow: ArrowDataType = dt.into();

        let expected = match unit {
            TimeUnit::Second => ArrowTimeUnit::Second,
            TimeUnit::Millisecond => ArrowTimeUnit::Millisecond,
            TimeUnit::Microsecond => ArrowTimeUnit::Microsecond,
            TimeUnit::Nanosecond => ArrowTimeUnit::Nanosecond,
        };

        assert_eq!(arrow, ArrowDataType::Duration(expected));
    }
}

// ============================================
// List Tests
// ============================================

#[test]
fn test_datatype_list() {
    let item_field = Arc::new(Field::new("item", 1, DataType::Int32, true));
    let dt = DataType::List(item_field);

    assert!(!dt.is_primitive_type());

    let arrow: ArrowDataType = dt.clone().into();
    match arrow {
        ArrowDataType::List(field) => {
            assert_eq!(field.name(), "item");
            assert_eq!(field.data_type(), &ArrowDataType::Int32);
        }
        _ => panic!("Expected List type"),
    }
}

#[test]
fn test_datatype_list_nested() {
    let inner_item = Arc::new(Field::new("item", 2, DataType::Utf8, true));
    let middle_field = Arc::new(Field::new("item", 1, DataType::List(inner_item), true));
    let dt = DataType::List(middle_field);

    assert!(!dt.is_primitive_type());

    let arrow: ArrowDataType = dt.into();
    match arrow {
        ArrowDataType::List(field) => {
            assert_eq!(field.name(), "item");
            match field.data_type() {
                ArrowDataType::List(inner) => {
                    assert_eq!(inner.data_type(), &ArrowDataType::Utf8);
                }
                _ => panic!("Expected nested List type"),
            }
        }
        _ => panic!("Expected List type"),
    }
}

// ============================================
// Struct Tests
// ============================================

#[test]
fn test_datatype_struct() {
    let fields = vec![
        Field::new("name", 1, DataType::Utf8, false),
        Field::new("age", 2, DataType::Int32, true),
    ];
    let dt = DataType::Struct(fields.into());

    assert!(!dt.is_primitive_type());

    let arrow: ArrowDataType = dt.into();
    match arrow {
        ArrowDataType::Struct(arrow_fields) => {
            assert_eq!(arrow_fields.len(), 2);
            assert_eq!(arrow_fields[0].name(), "name");
            assert_eq!(arrow_fields[1].name(), "age");
        }
        _ => panic!("Expected Struct type"),
    }
}

#[test]
fn test_datatype_struct_empty() {
    let dt: DataType = DataType::Struct(Vec::<Field>::new().into());

    assert!(!dt.is_primitive_type());

    let arrow: ArrowDataType = dt.into();
    match arrow {
        ArrowDataType::Struct(arrow_fields) => {
            assert!(arrow_fields.is_empty());
        }
        _ => panic!("Expected Struct type"),
    }
}

#[test]
fn test_datatype_struct_nested() {
    let inner_fields = vec![
        Field::new("street", 10, DataType::Utf8, false),
        Field::new("city", 11, DataType::Utf8, false),
    ];
    let address_field = Field::new("address", 2, DataType::Struct(inner_fields.into()), true);

    let fields = vec![Field::new("name", 1, DataType::Utf8, false), address_field];
    let dt = DataType::Struct(fields.into());

    assert!(!dt.is_primitive_type());
}
