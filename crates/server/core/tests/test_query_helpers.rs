use std::ops::RangeInclusive;

use datafusion::{
    prelude::{Expr, col},
    scalar::ScalarValue,
};
use wings_resources::OFFSET_COLUMN_NAME;
use wings_server_core::query::helpers::{find_partition_column_value, validate_offset_filters};

#[test]
fn test_validate_offset_filters_with_eq() {
    // Test single equality filter
    let filters = vec![Expr::eq(
        col(OFFSET_COLUMN_NAME),
        Expr::Literal(ScalarValue::UInt64(Some(5)), None),
    )];
    let result = validate_offset_filters(&filters).expect("should parse eq filter");
    assert_eq!(result, RangeInclusive::new(5, 5));
}

#[test]
fn test_validate_offset_filters_with_between() {
    // Test lower and upper bounds
    let filters = vec![
        Expr::gt_eq(
            col(OFFSET_COLUMN_NAME),
            Expr::Literal(ScalarValue::UInt64(Some(10)), None),
        ),
        Expr::lt_eq(
            col(OFFSET_COLUMN_NAME),
            Expr::Literal(ScalarValue::UInt64(Some(100)), None),
        ),
    ];
    let result = validate_offset_filters(&filters).expect("should parse between filter");
    assert_eq!(result, RangeInclusive::new(10, 100));
}

#[test]
fn test_validate_offset_filters_with_gt_and_lt() {
    // Test using > and < operators (should adjust bounds)
    let filters = vec![
        Expr::gt(
            col(OFFSET_COLUMN_NAME),
            Expr::Literal(ScalarValue::UInt64(Some(9)), None),
        ),
        Expr::lt(
            col(OFFSET_COLUMN_NAME),
            Expr::Literal(ScalarValue::UInt64(Some(101)), None),
        ),
    ];
    let result = validate_offset_filters(&filters).expect("should parse gt/lt filter");
    assert_eq!(result, RangeInclusive::new(10, 100));
}

#[test]
fn test_validate_offset_filters_reversed_bounds() {
    // Test when upper bound comes before lower bound in filters vec
    let filters = vec![
        Expr::lt_eq(
            col(OFFSET_COLUMN_NAME),
            Expr::Literal(ScalarValue::UInt64(Some(100)), None),
        ),
        Expr::gt_eq(
            col(OFFSET_COLUMN_NAME),
            Expr::Literal(ScalarValue::UInt64(Some(10)), None),
        ),
    ];
    let result = validate_offset_filters(&filters).expect("should parse reversed bounds filter");
    assert_eq!(result, RangeInclusive::new(10, 100));
}

#[test]
fn test_validate_offset_filters_no_filter() {
    let filters: Vec<Expr> = vec![];
    let err = validate_offset_filters(&filters).unwrap_err();
    insta::assert_debug_snapshot!(err, @r#"
    Plan(
        "No __offset__ filter provided. You must provide a lower and upper bound for __offset__.",
    )
    "#);
}

#[test]
fn test_validate_offset_filters_invalid_multiple_bounds() {
    // Test with three bounds (invalid)
    let filters = vec![
        Expr::gt_eq(
            col(OFFSET_COLUMN_NAME),
            Expr::Literal(ScalarValue::UInt64(Some(10)), None),
        ),
        Expr::lt_eq(
            col(OFFSET_COLUMN_NAME),
            Expr::Literal(ScalarValue::UInt64(Some(100)), None),
        ),
        Expr::eq(
            col(OFFSET_COLUMN_NAME),
            Expr::Literal(ScalarValue::UInt64(Some(50)), None),
        ),
    ];
    let err = validate_offset_filters(&filters).unwrap_err();
    insta::assert_debug_snapshot!(err, @r#"
    Plan(
        "Invalid __offset__ filter provided. You must provide a lower and upper bound for __offset__.",
    )
    "#);
}

#[test]
fn test_validate_offset_filters_duplicate_lower() {
    // Test with duplicate lower bounds (invalid)
    let filters = vec![
        Expr::gt_eq(
            col(OFFSET_COLUMN_NAME),
            Expr::Literal(ScalarValue::UInt64(Some(10)), None),
        ),
        Expr::gt(
            col(OFFSET_COLUMN_NAME),
            Expr::Literal(ScalarValue::UInt64(Some(5)), None),
        ),
    ];
    let err = validate_offset_filters(&filters).unwrap_err();
    insta::assert_debug_snapshot!(err, @r#"
    Plan(
        "Invalid __offset__ filter provided. You must provide a lower and upper bound for __offset__.",
    )
    "#);
}

#[test]
fn test_find_partition_column_value_success() {
    let column_name = "region_id";
    let filters = vec![Expr::eq(
        col(column_name),
        Expr::Literal(ScalarValue::Int64(Some(100)), None),
    )];
    let result = find_partition_column_value(column_name, &filters).expect("should find value");
    assert_eq!(result, ScalarValue::Int64(Some(100)));
}

#[test]
fn test_find_partition_column_value_not_found() {
    let column_name = "region_id";
    let filters: Vec<Expr> = vec![];
    let err = find_partition_column_value(column_name, &filters).unwrap_err();
    insta::assert_debug_snapshot!(err, @r#"
    Plan(
        "No region_id filter provided. You must provide a value for the partition column.",
    )
    "#);
}

#[test]
fn test_find_partition_column_value_multiple_values() {
    let column_name = "region_id";
    let filters = vec![
        Expr::eq(
            col(column_name),
            Expr::Literal(ScalarValue::Int64(Some(100)), None),
        ),
        Expr::eq(
            col(column_name),
            Expr::Literal(ScalarValue::Int64(Some(200)), None),
        ),
    ];
    let err = find_partition_column_value(column_name, &filters).unwrap_err();
    insta::assert_debug_snapshot!(err, @r#"
    Plan(
        "Multiple values found for partition column 'region_id'.",
    )
    "#);
}

#[test]
fn test_find_partition_column_value_wrong_column() {
    let column_name = "region_id";
    let filters = vec![Expr::eq(
        col("wrong_column"),
        Expr::Literal(ScalarValue::Int64(Some(100)), None),
    )];
    let err = find_partition_column_value(column_name, &filters).unwrap_err();
    insta::assert_debug_snapshot!(err, @r#"
    Plan(
        "No region_id filter provided. You must provide a value for the partition column.",
    )
    "#);
}

#[test]
fn test_find_partition_column_value_various_types() {
    // Test with string value
    let filters = vec![Expr::eq(
        col("name"),
        Expr::Literal(ScalarValue::Utf8(Some("test".to_string())), None),
    )];
    let result = find_partition_column_value("name", &filters).expect("should find string value");
    assert_eq!(result, ScalarValue::Utf8(Some("test".to_string())));

    // Test with boolean value
    let filters = vec![Expr::eq(
        col("active"),
        Expr::Literal(ScalarValue::Boolean(Some(true)), None),
    )];
    let result = find_partition_column_value("active", &filters).expect("should find bool value");
    assert_eq!(result, ScalarValue::Boolean(Some(true)));
}
