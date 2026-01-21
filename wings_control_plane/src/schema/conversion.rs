#[cfg(test)]
pub mod tests {
    use crate::schema::{DataType, Field, TimeUnit};
    use std::sync::Arc;

    fn round_trip_field(field: &Field) -> Field {
        let pb_field: crate::schema::pb::Field = field.try_into().expect("to proto");
        (&pb_field).try_into().expect("from proto")
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
        let string_types = vec![DataType::Utf8];

        for data_type in string_types {
            let field = Field::new("test_field", 1, data_type.clone(), false);
            let result = round_trip_field(&field);
            assert_eq!(field, result);
        }
    }

    #[test]
    fn test_binary_types() {
        let binary_types = vec![DataType::Binary];

        for data_type in binary_types {
            let field = Field::new("test_field", 1, data_type.clone(), false);
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
    fn test_list() {
        let item_field = Field::new("item", 2, DataType::Int32, false);
        let data_type = DataType::List(Arc::new(item_field.into()));
        let field = Field::new("test_field", 1, data_type, false);
        let result = round_trip_field(&field);
        assert_eq!(field, result);
    }

    #[test]
    fn test_struct() {
        let fields: Vec<Arc<Field>> = vec![
            Field::new("a", 4, DataType::Int32, false).into(),
            Field::new("b", 6, DataType::Utf8, true).into(),
            Field::new("c", 5, DataType::Float64, false).into(),
        ];
        let data_type = DataType::Struct(fields.into());
        let field = Field::new("test_field", 1, data_type, false);
        let result = round_trip_field(&field);
        assert_eq!(field, result);
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
