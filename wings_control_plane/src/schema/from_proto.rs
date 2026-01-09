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

use datafusion::arrow::datatypes::{
    DataType, IntervalUnit as ArrowIntervalUnit, TimeUnit as ArrowTimeUnit, UnionFields,
    UnionMode as ArrowUnionMode,
};

use crate::schema::{
    Field, Schema,
    error::{Result, SchemaError},
};

impl TryFrom<&crate::schema::pb::Schema> for Schema {
    type Error = SchemaError;

    fn try_from(schema: &crate::schema::pb::Schema) -> Result<Self> {
        let fields = schema
            .fields
            .iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            fields,
            schema_id: schema.schema_id,
            metadata: schema.metadata.clone(),
        })
    }
}

impl TryFrom<&crate::schema::pb::Field> for Field {
    type Error = SchemaError;

    fn try_from(field: &crate::schema::pb::Field) -> Result<Self> {
        let datatype: &crate::schema::pb::ArrowType =
            field.arrow_type.as_ref().required("arrow_type")?;
        let datatype = datatype.try_into()?;
        let field = Self::new(field.name.as_str(), field.id, datatype, field.nullable)
            .with_metadata(field.metadata.clone());
        Ok(field)
    }
}

impl TryFrom<&crate::schema::pb::ArrowType> for DataType {
    type Error = SchemaError;

    fn try_from(arrow_type: &crate::schema::pb::ArrowType) -> Result<Self> {
        let arrow_type_enum = arrow_type
            .arrow_type_enum
            .as_ref()
            .required("arrow_type_enum")?;
        arrow_type_enum.try_into()
    }
}

impl TryFrom<&crate::schema::pb::arrow_type::ArrowTypeEnum> for DataType {
    type Error = SchemaError;

    fn try_from(arrow_type_enum: &crate::schema::pb::arrow_type::ArrowTypeEnum) -> Result<Self> {
        use crate::schema::pb::arrow_type::ArrowTypeEnum;

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
            ArrowTypeEnum::Utf8View(_) => DataType::Utf8View,
            ArrowTypeEnum::LargeUtf8(_) => DataType::LargeUtf8,
            ArrowTypeEnum::Binary(_) => DataType::Binary,
            ArrowTypeEnum::BinaryView(_) => DataType::BinaryView,
            ArrowTypeEnum::FixedSizeBinary(size) => DataType::FixedSizeBinary(*size),
            ArrowTypeEnum::LargeBinary(_) => DataType::LargeBinary,
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
            ArrowTypeEnum::Time32(time_unit) => {
                DataType::Time32(parse_i32_to_time_unit(*time_unit)?)
            }
            ArrowTypeEnum::Time64(time_unit) => {
                DataType::Time64(parse_i32_to_time_unit(*time_unit)?)
            }
            ArrowTypeEnum::Interval(interval_unit) => {
                DataType::Interval(parse_i32_to_interval_unit(*interval_unit)?)
            }
            ArrowTypeEnum::Decimal32(decimal) => {
                DataType::Decimal32(decimal.precision as u8, decimal.scale as i8)
            }
            ArrowTypeEnum::Decimal64(decimal) => {
                DataType::Decimal64(decimal.precision as u8, decimal.scale as i8)
            }
            ArrowTypeEnum::Decimal128(decimal) => {
                DataType::Decimal128(decimal.precision as u8, decimal.scale as i8)
            }
            ArrowTypeEnum::Decimal256(decimal) => {
                DataType::Decimal256(decimal.precision as u8, decimal.scale as i8)
            }
            ArrowTypeEnum::List(list) => {
                let list_type: &crate::schema::pb::NestedField =
                    list.field_type.as_deref().required("field_type")?;
                DataType::List(Arc::new(list_type.try_into()?))
            }
            ArrowTypeEnum::LargeList(list) => {
                let list_type: &crate::schema::pb::NestedField =
                    list.field_type.as_deref().required("field_type")?;
                DataType::LargeList(Arc::new(list_type.try_into()?))
            }
            ArrowTypeEnum::FixedSizeList(list) => {
                let list_type: &crate::schema::pb::NestedField =
                    list.field_type.as_deref().required("field_type")?;
                DataType::FixedSizeList(Arc::new(list_type.try_into()?), list.list_size)
            }
            ArrowTypeEnum::Struct(strct) => {
                DataType::Struct(parse_proto_fields_to_fields(&strct.sub_field_types)?.into())
            }
            ArrowTypeEnum::Union(union) => {
                let union_mode = match union.union_mode {
                    0 => ArrowUnionMode::Sparse,
                    1 => ArrowUnionMode::Dense,
                    _ => {
                        return Err(SchemaError::ConversionError {
                            message: format!("Invalid union mode: {}", union.union_mode),
                        });
                    }
                };
                let union_fields = parse_proto_fields_to_fields(&union.union_types)?;

                let type_ids: Vec<_> = match union.type_ids.is_empty() {
                    true => (0..union_fields.len() as i8).collect(),
                    false => union.type_ids.iter().map(|i| *i as i8).collect(),
                };

                DataType::Union(UnionFields::new(type_ids, union_fields), union_mode)
            }
            ArrowTypeEnum::Dictionary(dict) => {
                let key_datatype: &crate::schema::pb::ArrowType =
                    dict.key.as_deref().required("key")?;
                let value_datatype: &crate::schema::pb::ArrowType =
                    dict.value.as_deref().required("value")?;
                DataType::Dictionary(
                    Box::new(key_datatype.try_into()?),
                    Box::new(value_datatype.try_into()?),
                )
            }
            ArrowTypeEnum::Map(map) => {
                let field: &crate::schema::pb::NestedField =
                    map.field_type.as_deref().required("field_type")?;
                DataType::Map(Arc::new(field.try_into()?), map.keys_sorted)
            }
        })
    }
}

impl TryFrom<&crate::schema::pb::NestedField> for arrow::datatypes::Field {
    type Error = SchemaError;

    fn try_from(field: &crate::schema::pb::NestedField) -> Result<Self> {
        let datatype: &crate::schema::pb::ArrowType =
            field.arrow_type.as_deref().required("arrow_type")?;
        Ok(Self::new(
            field.name.as_str(),
            datatype.try_into()?,
            field.nullable,
        ))
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

#[allow(clippy::ptr_arg)]
pub fn parse_i32_to_time_unit(value: i32) -> Result<ArrowTimeUnit> {
    Ok(match value {
        0 => ArrowTimeUnit::Second,
        1 => ArrowTimeUnit::Millisecond,
        2 => ArrowTimeUnit::Microsecond,
        3 => ArrowTimeUnit::Nanosecond,
        _ => {
            return Err(SchemaError::ConversionError {
                message: format!("Invalid time unit: {}", value),
            });
        }
    })
}

#[allow(clippy::ptr_arg)]
pub fn parse_i32_to_interval_unit(value: i32) -> Result<ArrowIntervalUnit> {
    Ok(match value {
        0 => ArrowIntervalUnit::YearMonth,
        1 => ArrowIntervalUnit::DayTime,
        2 => ArrowIntervalUnit::MonthDayNano,
        _ => {
            return Err(SchemaError::ConversionError {
                message: format!("Invalid interval unit: {}", value),
            });
        }
    })
}

pub fn parse_proto_fields_to_fields(
    fields: &[crate::schema::pb::NestedField],
) -> Result<Vec<arrow::datatypes::Field>> {
    fields.iter().map(|field| field.try_into()).collect()
}
