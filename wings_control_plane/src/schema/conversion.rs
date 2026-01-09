#[cfg(test)]
pub mod tests {
    use crate::schema::{Field, NestedField};
    use datafusion::arrow::datatypes::{DataType, TimeUnit, UnionFields, UnionMode};
    use std::sync::Arc;

    fn round_trip_field(field: &Field) -> Field {
        let pb_field: crate::schema::pb::Field = field.try_into().unwrap();
        (&pb_field).try_into().unwrap()
    }

    #[test]
    fn test_null() {
        let field = Field::new("test_field", 1, DataType::Null, false);
        let result = round_trip_field(&field);
        assert_eq!(field, result);
    }

    #[test]
    fn test_boolean() {
        let field = Field::new("test_field", 1, DataType::Boolean, false);
        let result = round_trip_field(&field);
        assert_eq!(field, result);
    }

    #[test]
    fn test_integers() {
        let integer_types = vec![
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
        ];

        for data_type in integer_types {
            let field = Field::new("test_field", 1, data_type.clone(), false);
            let result = round_trip_field(&field);
            assert_eq!(field, result);
        }
    }

    #[test]
    fn test_floats() {
        let float_types = vec![DataType::Float16, DataType::Float32, DataType::Float64];

        for data_type in float_types {
            let field = Field::new("test_field", 1, data_type.clone(), false);
            let result = round_trip_field(&field);
            assert_eq!(field, result);
        }
    }

    #[test]
    fn test_string_types() {
        let string_types = vec![DataType::Utf8, DataType::Utf8View, DataType::LargeUtf8];

        for data_type in string_types {
            let field = Field::new("test_field", 1, data_type.clone(), false);
            let result = round_trip_field(&field);
            assert_eq!(field, result);
        }
    }

    #[test]
    fn test_binary_types() {
        let binary_types = vec![
            DataType::Binary,
            DataType::BinaryView,
            DataType::LargeBinary,
        ];

        for data_type in binary_types {
            let field = Field::new("test_field", 1, data_type.clone(), false);
            let result = round_trip_field(&field);
            assert_eq!(field, result);
        }
    }

    #[test]
    fn test_fixed_size_binary() {
        let sizes = vec![16, 32, 64];

        for size in sizes {
            let data_type = DataType::FixedSizeBinary(size);
            let field = Field::new("test_field", 1, data_type, false);
            let result = round_trip_field(&field);
            assert_eq!(field, result);
        }
    }

    #[test]
    fn test_date_types() {
        let date_types = vec![DataType::Date32, DataType::Date64];

        for data_type in date_types {
            let field = Field::new("test_field", 1, data_type.clone(), false);
            let result = round_trip_field(&field);
            assert_eq!(field, result);
        }
    }

    #[test]
    fn test_time32() {
        let time_units = vec![
            TimeUnit::Second,
            TimeUnit::Millisecond,
            TimeUnit::Microsecond,
            TimeUnit::Nanosecond,
        ];

        for time_unit in time_units {
            let data_type = DataType::Time32(time_unit);
            let field = Field::new("test_field", 1, data_type, false);
            let result = round_trip_field(&field);
            assert_eq!(field, result);
        }
    }

    #[test]
    fn test_time64() {
        let time_units = vec![
            TimeUnit::Second,
            TimeUnit::Millisecond,
            TimeUnit::Microsecond,
            TimeUnit::Nanosecond,
        ];

        for time_unit in time_units {
            let data_type = DataType::Time64(time_unit);
            let field = Field::new("test_field", 1, data_type, false);
            let result = round_trip_field(&field);
            assert_eq!(field, result);
        }
    }

    #[test]
    fn test_timestamp() {
        let time_units = vec![
            TimeUnit::Second,
            TimeUnit::Millisecond,
            TimeUnit::Microsecond,
            TimeUnit::Nanosecond,
        ];
        let timezones = vec![None, Some("UTC"), Some("America/Los_Angeles")];

        for time_unit in time_units {
            for timezone in &timezones {
                let data_type = DataType::Timestamp(time_unit, timezone.map(|s| s.into()));
                let field = Field::new("test_field", 1, data_type, false);
                let result = round_trip_field(&field);
                assert_eq!(field, result);
            }
        }
    }

    #[test]
    fn test_duration() {
        let time_units = vec![
            TimeUnit::Second,
            TimeUnit::Millisecond,
            TimeUnit::Microsecond,
            TimeUnit::Nanosecond,
        ];

        for time_unit in time_units {
            let data_type = DataType::Duration(time_unit);
            let field = Field::new("test_field", 1, data_type, false);
            let result = round_trip_field(&field);
            assert_eq!(field, result);
        }
    }

    #[test]
    fn test_interval() {
        let interval_units = vec![
            arrow::datatypes::IntervalUnit::YearMonth,
            arrow::datatypes::IntervalUnit::DayTime,
            arrow::datatypes::IntervalUnit::MonthDayNano,
        ];

        for interval_unit in interval_units {
            let data_type = DataType::Interval(interval_unit);
            let field = Field::new("test_field", 1, data_type, false);
            let result = round_trip_field(&field);
            assert_eq!(field, result);
        }
    }

    #[test]
    fn test_decimal32() {
        let test_cases = vec![(5, 2), (9, 0), (9, -2)];

        for (precision, scale) in test_cases {
            let data_type = DataType::Decimal32(precision, scale);
            let field = Field::new("test_field", 1, data_type, false);
            let result = round_trip_field(&field);
            assert_eq!(field, result);
        }
    }

    #[test]
    fn test_decimal64() {
        let test_cases = vec![(10, 2), (18, 0), (18, -2)];

        for (precision, scale) in test_cases {
            let data_type = DataType::Decimal64(precision, scale);
            let field = Field::new("test_field", 1, data_type, false);
            let result = round_trip_field(&field);
            assert_eq!(field, result);
        }
    }

    #[test]
    fn test_decimal128() {
        let test_cases = vec![(10, 2), (20, 0), (28, -2)];

        for (precision, scale) in test_cases {
            let data_type = DataType::Decimal128(precision, scale);
            let field = Field::new("test_field", 1, data_type, false);
            let result = round_trip_field(&field);
            assert_eq!(field, result);
        }
    }

    #[test]
    fn test_decimal256() {
        let test_cases = vec![(10, 2), (40, 0), (76, -2)];

        for (precision, scale) in test_cases {
            let data_type = DataType::Decimal256(precision, scale);
            let field = Field::new("test_field", 1, data_type, false);
            let result = round_trip_field(&field);
            assert_eq!(field, result);
        }
    }

    #[test]
    fn test_list() {
        let item_field = NestedField::new("item", DataType::Int32, false);
        let data_type = DataType::List(Arc::new(item_field));
        let field = Field::new("test_field", 1, data_type, false);
        let result = round_trip_field(&field);
        assert_eq!(field, result);
    }

    #[test]
    fn test_large_list() {
        let item_field = NestedField::new("item", DataType::Int32, false);
        let data_type = DataType::LargeList(Arc::new(item_field));
        let field = Field::new("test_field", 1, data_type, false);
        let result = round_trip_field(&field);
        assert_eq!(field, result);
    }

    #[test]
    fn test_fixed_size_list() {
        let sizes = vec![3, 10, 100];

        for size in sizes {
            let item_field = NestedField::new("item", DataType::Int32, false);
            let data_type = DataType::FixedSizeList(Arc::new(item_field), size);
            let field = Field::new("test_field", 1, data_type, false);
            let result = round_trip_field(&field);
            assert_eq!(field, result);
        }
    }

    #[test]
    fn test_struct() {
        let fields = vec![
            NestedField::new("a", DataType::Int32, false),
            NestedField::new("b", DataType::Utf8, true),
            NestedField::new("c", DataType::Float64, false),
        ];
        let data_type = DataType::Struct(fields.into());
        let field = Field::new("test_field", 1, data_type, false);
        let result = round_trip_field(&field);
        assert_eq!(field, result);
    }

    #[test]
    fn test_union_sparse() {
        let fields = vec![
            NestedField::new("a", DataType::Int32, false),
            NestedField::new("b", DataType::Utf8, true),
        ];
        let type_ids = vec![0i8, 1i8];
        let union_fields = UnionFields::new(type_ids.clone(), fields);
        let data_type = DataType::Union(union_fields, UnionMode::Sparse);
        let field = Field::new("test_field", 1, data_type, false);
        let result = round_trip_field(&field);
        assert_eq!(field, result);
    }

    #[test]
    fn test_union_dense() {
        let fields = vec![
            NestedField::new("a", DataType::Int32, false),
            NestedField::new("b", DataType::Utf8, true),
        ];
        let type_ids = vec![0i8, 1i8];
        let union_fields = UnionFields::new(type_ids.clone(), fields);
        let data_type = DataType::Union(union_fields, UnionMode::Dense);
        let field = Field::new("test_field", 1, data_type, false);
        let result = round_trip_field(&field);
        assert_eq!(field, result);
    }

    #[test]
    fn test_dictionary() {
        let key_type = DataType::Int32;
        let value_type = DataType::Utf8;
        let data_type = DataType::Dictionary(Box::new(key_type), Box::new(value_type));
        let field = Field::new("test_field", 1, data_type, false);
        let result = round_trip_field(&field);
        assert_eq!(field, result);
    }

    #[test]
    fn test_map() {
        let map_field = NestedField::new(
            "entries",
            DataType::Struct(
                vec![
                    NestedField::new("key", DataType::Utf8, false),
                    NestedField::new("value", DataType::Int32, true),
                ]
                .into(),
            ),
            false,
        );

        for keys_sorted in [true, false] {
            let data_type = DataType::Map(Arc::new(map_field.clone()), keys_sorted);
            let field = Field::new("test_field", 1, data_type, false);
            let result = round_trip_field(&field);
            assert_eq!(field, result);
        }
    }

    #[test]
    fn test_field_metadata() {
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("key1".to_string(), "value1".to_string());
        metadata.insert("key2".to_string(), "value2".to_string());

        let field =
            Field::new("test_field", 1, DataType::Int32, false).with_metadata(metadata.clone());
        let result = round_trip_field(&field);
        assert_eq!(field, result);
    }

    #[test]
    fn test_nullable() {
        let nullable_field = Field::new("test_field", 1, DataType::Int32, true);
        let non_nullable_field = Field::new("test_field", 1, DataType::Int32, false);

        let nullable_result = round_trip_field(&nullable_field);
        let non_nullable_result = round_trip_field(&non_nullable_field);

        assert_eq!(nullable_field, nullable_result);
        assert_eq!(non_nullable_field, non_nullable_result);
    }
}
