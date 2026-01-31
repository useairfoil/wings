// This code originally comes from the DataFusion project.
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
use std::sync::Arc;

use wings_schema::{
    DataType, Datum, Field, FieldRef, Schema, SchemaBuilder, SchemaError, TimeUnit, error::Result,
};

use crate::pb;

impl From<&Schema> for pb::Schema {
    fn from(value: &Schema) -> Self {
        let fields = value
            .fields
            .iter()
            .map(|f| f.as_ref().into())
            .collect::<Vec<_>>();

        pb::Schema {
            fields,
            metadata: value.metadata.clone(),
        }
    }
}

impl TryFrom<&pb::Schema> for Schema {
    type Error = SchemaError;

    fn try_from(schema: &pb::Schema) -> Result<Self> {
        let fields = schema
            .fields
            .iter()
            .map(|f| f.try_into().map(Arc::new))
            .collect::<Result<Vec<_>>>()?;

        SchemaBuilder::new(fields)
            .with_metadata(schema.metadata.clone())
            .build()
    }
}

impl From<&Field> for pb::Field {
    fn from(value: &Field) -> Self {
        let arrow_type: pb::ArrowType = (&value.data_type).into();
        pb::Field {
            name: value.name.clone(),
            id: value.id,
            arrow_type: Some(Box::new(arrow_type)),
            nullable: value.nullable,
            metadata: value.metadata.clone(),
        }
    }
}

impl TryFrom<&pb::Field> for Field {
    type Error = SchemaError;

    fn try_from(field: &pb::Field) -> Result<Self> {
        let datatype: &pb::ArrowType = field.arrow_type.as_ref().required("arrow_type")?;
        let datatype = datatype.try_into()?;
        let field = Self::new(field.name.as_str(), field.id, datatype, field.nullable)
            .with_metadata(field.metadata.clone());
        Ok(field)
    }
}

impl From<DataType> for pb::ArrowType {
    fn from(value: DataType) -> Self {
        (&value).into()
    }
}

impl From<&DataType> for pb::ArrowType {
    fn from(value: &DataType) -> Self {
        let arrow_type_enum = value.into();
        pb::ArrowType {
            arrow_type_enum: Some(arrow_type_enum),
        }
    }
}

impl From<&Datum> for pb::Datum {
    fn from(datum: &Datum) -> Self {
        pb::Datum {
            r#type: Some(datum.data_type().into()),
            content: datum.to_bytes(),
        }
    }
}

impl From<&DataType> for pb::arrow_type::ArrowTypeEnum {
    fn from(value: &DataType) -> Self {
        use pb::arrow_type::ArrowTypeEnum;

        match value {
            DataType::Null => ArrowTypeEnum::None(pb::EmptyMessage {}),
            DataType::Boolean => ArrowTypeEnum::Bool(pb::EmptyMessage {}),
            DataType::Int8 => ArrowTypeEnum::Int8(pb::EmptyMessage {}),
            DataType::Int16 => ArrowTypeEnum::Int16(pb::EmptyMessage {}),
            DataType::Int32 => ArrowTypeEnum::Int32(pb::EmptyMessage {}),
            DataType::Int64 => ArrowTypeEnum::Int64(pb::EmptyMessage {}),
            DataType::UInt8 => ArrowTypeEnum::Uint8(pb::EmptyMessage {}),
            DataType::UInt16 => ArrowTypeEnum::Uint16(pb::EmptyMessage {}),
            DataType::UInt32 => ArrowTypeEnum::Uint32(pb::EmptyMessage {}),
            DataType::UInt64 => ArrowTypeEnum::Uint64(pb::EmptyMessage {}),
            DataType::Float16 => ArrowTypeEnum::Float16(pb::EmptyMessage {}),
            DataType::Float32 => ArrowTypeEnum::Float32(pb::EmptyMessage {}),
            DataType::Float64 => ArrowTypeEnum::Float64(pb::EmptyMessage {}),
            DataType::Timestamp(time_unit, timezone) => {
                let pb_time_unit = match time_unit {
                    TimeUnit::Second => pb::TimeUnit::Second,
                    TimeUnit::Millisecond => pb::TimeUnit::Millisecond,
                    TimeUnit::Microsecond => pb::TimeUnit::Microsecond,
                    TimeUnit::Nanosecond => pb::TimeUnit::Nanosecond,
                };
                ArrowTypeEnum::Timestamp(pb::Timestamp {
                    time_unit: pb_time_unit as i32,
                    timezone: timezone.as_deref().unwrap_or("").to_string(),
                })
            }
            DataType::Date32 => ArrowTypeEnum::Date32(pb::EmptyMessage {}),
            DataType::Date64 => ArrowTypeEnum::Date64(pb::EmptyMessage {}),
            DataType::Duration(time_unit) => {
                let pb_time_unit = match time_unit {
                    TimeUnit::Second => pb::TimeUnit::Second,
                    TimeUnit::Millisecond => pb::TimeUnit::Millisecond,
                    TimeUnit::Microsecond => pb::TimeUnit::Microsecond,
                    TimeUnit::Nanosecond => pb::TimeUnit::Nanosecond,
                };
                ArrowTypeEnum::Duration(pb_time_unit as i32)
            }
            DataType::Binary => ArrowTypeEnum::Binary(pb::EmptyMessage {}),
            DataType::Utf8 => ArrowTypeEnum::Utf8(pb::EmptyMessage {}),
            DataType::List(item_type) => ArrowTypeEnum::List(Box::new(pb::List {
                field_type: Some(Box::new(item_type.as_ref().into())),
            })),
            DataType::Struct(struct_fields) => ArrowTypeEnum::Struct(pb::Struct {
                sub_field_types: arc_fields_to_proto_fields(struct_fields),
            }),
        }
    }
}

impl TryFrom<&pb::Datum> for Datum {
    type Error = SchemaError;

    fn try_from(datum: &pb::Datum) -> Result<Self> {
        let data_type: DataType = datum.r#type.as_ref().required("type")?.try_into()?;
        Datum::try_from_bytes(data_type, &datum.content)
    }
}

impl TryFrom<&pb::ArrowType> for DataType {
    type Error = SchemaError;

    fn try_from(arrow_type: &pb::ArrowType) -> Result<Self> {
        let arrow_type_enum = arrow_type
            .arrow_type_enum
            .as_ref()
            .required("arrow_type_enum")?;
        arrow_type_enum.try_into()
    }
}

impl TryFrom<&pb::arrow_type::ArrowTypeEnum> for DataType {
    type Error = SchemaError;

    fn try_from(arrow_type_enum: &pb::arrow_type::ArrowTypeEnum) -> Result<Self> {
        use pb::arrow_type::ArrowTypeEnum;

        Ok(match arrow_type_enum {
            ArrowTypeEnum::None(_) => DataType::Null,
            ArrowTypeEnum::Bool(_) => DataType::Boolean,
            ArrowTypeEnum::Uint8(_) => DataType::UInt8,
            ArrowTypeEnum::Int8(_) => DataType::Int8,
            ArrowTypeEnum::Uint16(_) => DataType::UInt16,
            ArrowTypeEnum::Int16(_) => DataType::Int16,
            ArrowTypeEnum::Uint32(_) => DataType::UInt32,
            ArrowTypeEnum::Int32(_) => DataType::Int32,
            ArrowTypeEnum::Uint64(_) => DataType::UInt64,
            ArrowTypeEnum::Int64(_) => DataType::Int64,
            ArrowTypeEnum::Float16(_) => DataType::Float16,
            ArrowTypeEnum::Float32(_) => DataType::Float32,
            ArrowTypeEnum::Float64(_) => DataType::Float64,
            ArrowTypeEnum::Utf8(_) => DataType::Utf8,
            ArrowTypeEnum::Binary(_) => DataType::Binary,
            ArrowTypeEnum::Date32(_) => DataType::Date32,
            ArrowTypeEnum::Date64(_) => DataType::Date64,
            ArrowTypeEnum::Duration(time_unit) => {
                DataType::Duration(parse_i32_to_time_unit(*time_unit)?)
            }
            ArrowTypeEnum::Timestamp(ts) => {
                let timezone = match ts.timezone.len() {
                    0 => None,
                    _ => Some(ts.timezone.as_str().into()),
                };
                DataType::Timestamp(parse_i32_to_time_unit(ts.time_unit)?, timezone)
            }
            ArrowTypeEnum::List(list) => {
                let list_type: &pb::Field = list.field_type.as_deref().required("field_type")?;
                let field: Field = list_type.try_into()?;
                DataType::List(Arc::new(field))
            }
            ArrowTypeEnum::Struct(strct) => {
                let fields = parse_proto_fields_to_fields(&strct.sub_field_types)?;
                DataType::Struct(fields.into())
            }
        })
    }
}

pub trait FromOptionalField<T> {
    fn required(self, field: &str) -> Result<T>;
}

impl<T> FromOptionalField<T> for Option<T> {
    fn required(self, field: &str) -> Result<T> {
        self.ok_or_else(|| SchemaError::ConversionError {
            message: format!("Missing required field: {}", field),
        })
    }
}

pub fn parse_i32_to_time_unit(value: i32) -> Result<TimeUnit> {
    use pb::TimeUnit as ProtoTimeUnit;

    match ProtoTimeUnit::try_from(value) {
        Err(_) | Ok(ProtoTimeUnit::Unspecified) => Err(SchemaError::ConversionError {
            message: format!("unspecified time unit: {value}"),
        }),
        Ok(ProtoTimeUnit::Second) => Ok(TimeUnit::Second),
        Ok(ProtoTimeUnit::Millisecond) => Ok(TimeUnit::Millisecond),
        Ok(ProtoTimeUnit::Microsecond) => Ok(TimeUnit::Microsecond),
        Ok(ProtoTimeUnit::Nanosecond) => Ok(TimeUnit::Nanosecond),
    }
}

pub fn parse_proto_fields_to_fields(fields: &[pb::Field]) -> Result<Vec<FieldRef>> {
    fields
        .iter()
        .map(|field| {
            let f: Field = field.try_into()?;
            Ok(f.into())
        })
        .collect()
}

pub fn arc_fields_to_proto_fields<'a, I>(fields: I) -> Vec<pb::Field>
where
    I: IntoIterator<Item = &'a Arc<Field>>,
{
    fields
        .into_iter()
        .map(|field| field.as_ref().into())
        .collect()
}

#[cfg(test)]
pub mod tests {
    use std::sync::Arc;

    use wings_schema::{DataType, Field, TimeUnit};

    fn round_trip_field(field: &Field) -> Field {
        let pb_field: crate::pb::Field = field.try_into().expect("to proto");
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
