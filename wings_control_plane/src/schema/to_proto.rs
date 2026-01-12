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

use datafusion::arrow::datatypes::{DataType, UnionMode};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::schema::{
    Field, Schema,
    error::{Result, SchemaError},
};

impl TryFrom<&Schema> for crate::schema::pb::Schema {
    type Error = SchemaError;

    fn try_from(value: &Schema) -> Result<Self> {
        let fields = value
            .fields
            .iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>>>()?;

        Ok(crate::schema::pb::Schema {
            fields,
            metadata: value.metadata.clone(),
        })
    }
}

impl TryFrom<&Field> for crate::schema::pb::Field {
    type Error = SchemaError;

    fn try_from(value: &Field) -> Result<Self> {
        let arrow_type: crate::schema::pb::ArrowType = (&value.data_type).try_into()?;
        Ok(crate::schema::pb::Field {
            name: value.name.clone(),
            id: value.id,
            arrow_type: Some(Box::new(arrow_type)),
            nullable: value.nullable,
            metadata: value.metadata.clone(),
        })
    }
}

impl TryFrom<DataType> for crate::schema::pb::ArrowType {
    type Error = SchemaError;

    fn try_from(value: DataType) -> std::result::Result<Self, Self::Error> {
        (&value).try_into()
    }
}

impl TryFrom<&DataType> for crate::schema::pb::ArrowType {
    type Error = SchemaError;

    fn try_from(value: &DataType) -> Result<Self> {
        let arrow_type_enum = value.try_into()?;
        Ok(crate::schema::pb::ArrowType {
            arrow_type_enum: Some(arrow_type_enum),
        })
    }
}

impl TryFrom<&DataType> for crate::schema::pb::arrow_type::ArrowTypeEnum {
    type Error = SchemaError;

    fn try_from(value: &DataType) -> Result<Self> {
        use crate::schema::pb::{self as pb, arrow_type::ArrowTypeEnum};

        let res = match value {
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
                    arrow::datatypes::TimeUnit::Second => pb::TimeUnit::Second,
                    arrow::datatypes::TimeUnit::Millisecond => pb::TimeUnit::Millisecond,
                    arrow::datatypes::TimeUnit::Microsecond => pb::TimeUnit::Microsecond,
                    arrow::datatypes::TimeUnit::Nanosecond => pb::TimeUnit::Nanosecond,
                };
                ArrowTypeEnum::Timestamp(pb::Timestamp {
                    time_unit: pb_time_unit as i32,
                    timezone: timezone.as_deref().unwrap_or("").to_string(),
                })
            }
            DataType::Date32 => ArrowTypeEnum::Date32(pb::EmptyMessage {}),
            DataType::Date64 => ArrowTypeEnum::Date64(pb::EmptyMessage {}),
            DataType::Time32(time_unit) => {
                let pb_time_unit = match time_unit {
                    arrow::datatypes::TimeUnit::Second => pb::TimeUnit::Second,
                    arrow::datatypes::TimeUnit::Millisecond => pb::TimeUnit::Millisecond,
                    arrow::datatypes::TimeUnit::Microsecond => pb::TimeUnit::Microsecond,
                    arrow::datatypes::TimeUnit::Nanosecond => pb::TimeUnit::Nanosecond,
                };
                ArrowTypeEnum::Time32(pb_time_unit as i32)
            }
            DataType::Time64(time_unit) => {
                let pb_time_unit = match time_unit {
                    arrow::datatypes::TimeUnit::Second => pb::TimeUnit::Second,
                    arrow::datatypes::TimeUnit::Millisecond => pb::TimeUnit::Millisecond,
                    arrow::datatypes::TimeUnit::Microsecond => pb::TimeUnit::Microsecond,
                    arrow::datatypes::TimeUnit::Nanosecond => pb::TimeUnit::Nanosecond,
                };
                ArrowTypeEnum::Time64(pb_time_unit as i32)
            }
            DataType::Duration(time_unit) => {
                let pb_time_unit = match time_unit {
                    arrow::datatypes::TimeUnit::Second => pb::TimeUnit::Second,
                    arrow::datatypes::TimeUnit::Millisecond => pb::TimeUnit::Millisecond,
                    arrow::datatypes::TimeUnit::Microsecond => pb::TimeUnit::Microsecond,
                    arrow::datatypes::TimeUnit::Nanosecond => pb::TimeUnit::Nanosecond,
                };
                ArrowTypeEnum::Duration(pb_time_unit as i32)
            }
            DataType::Interval(interval_unit) => {
                let pb_interval_unit = match interval_unit {
                    arrow::datatypes::IntervalUnit::YearMonth => pb::IntervalUnit::YearMonth,
                    arrow::datatypes::IntervalUnit::DayTime => pb::IntervalUnit::DayTime,
                    arrow::datatypes::IntervalUnit::MonthDayNano => pb::IntervalUnit::MonthDayNano,
                };
                ArrowTypeEnum::Interval(pb_interval_unit as i32)
            }
            DataType::Binary => ArrowTypeEnum::Binary(pb::EmptyMessage {}),
            DataType::BinaryView => ArrowTypeEnum::BinaryView(pb::EmptyMessage {}),
            DataType::FixedSizeBinary(size) => ArrowTypeEnum::FixedSizeBinary(*size),
            DataType::LargeBinary => ArrowTypeEnum::LargeBinary(pb::EmptyMessage {}),
            DataType::Utf8 => ArrowTypeEnum::Utf8(pb::EmptyMessage {}),
            DataType::Utf8View => ArrowTypeEnum::Utf8View(pb::EmptyMessage {}),
            DataType::LargeUtf8 => ArrowTypeEnum::LargeUtf8(pb::EmptyMessage {}),
            DataType::List(item_type) => ArrowTypeEnum::List(Box::new(pb::List {
                field_type: Some(Box::new(arrow_field_to_proto_field(item_type.as_ref())?)),
            })),
            DataType::FixedSizeList(item_type, size) => {
                ArrowTypeEnum::FixedSizeList(Box::new(pb::FixedSizeList {
                    field_type: Some(Box::new(arrow_field_to_proto_field(item_type.as_ref())?)),
                    list_size: *size,
                }))
            }
            DataType::LargeList(item_type) => ArrowTypeEnum::LargeList(Box::new(pb::List {
                field_type: Some(Box::new(arrow_field_to_proto_field(item_type.as_ref())?)),
            })),
            DataType::Struct(struct_fields) => ArrowTypeEnum::Struct(pb::Struct {
                sub_field_types: convert_arc_fields_to_proto_fields(struct_fields)?,
            }),
            DataType::Union(fields, union_mode) => {
                let union_mode = match union_mode {
                    UnionMode::Sparse => pb::UnionMode::Sparse,
                    UnionMode::Dense => pb::UnionMode::Dense,
                };
                ArrowTypeEnum::Union(pb::Union {
                    union_types: convert_arc_fields_to_proto_fields(
                        fields.iter().map(|(_, item)| item),
                    )?,
                    union_mode: union_mode.into(),
                    type_ids: fields.iter().map(|(x, _)| x as i32).collect(),
                })
            }
            DataType::Dictionary(key_type, value_type) => {
                let key_arrow_type =
                    <&DataType as TryInto<crate::schema::pb::ArrowType>>::try_into(
                        key_type.as_ref(),
                    )?;
                let value_arrow_type =
                    <&DataType as TryInto<crate::schema::pb::ArrowType>>::try_into(
                        value_type.as_ref(),
                    )?;
                ArrowTypeEnum::Dictionary(Box::new(pb::Dictionary {
                    key: Some(Box::new(key_arrow_type)),
                    value: Some(Box::new(value_arrow_type)),
                }))
            }
            DataType::Decimal32(precision, scale) => ArrowTypeEnum::Decimal32(pb::Decimal32Type {
                precision: *precision as u32,
                scale: *scale as i32,
            }),
            DataType::Decimal64(precision, scale) => ArrowTypeEnum::Decimal64(pb::Decimal64Type {
                precision: *precision as u32,
                scale: *scale as i32,
            }),
            DataType::Decimal128(precision, scale) => {
                ArrowTypeEnum::Decimal128(pb::Decimal128Type {
                    precision: *precision as u32,
                    scale: *scale as i32,
                })
            }
            DataType::Decimal256(precision, scale) => {
                ArrowTypeEnum::Decimal256(pb::Decimal256Type {
                    precision: *precision as u32,
                    scale: *scale as i32,
                })
            }
            DataType::Map(field, sorted) => ArrowTypeEnum::Map(Box::new(pb::Map {
                field_type: Some(Box::new(arrow_field_to_proto_field(field.as_ref())?)),
                keys_sorted: *sorted,
            })),
            DataType::RunEndEncoded(_, _) => {
                return Err(SchemaError::ConversionError {
                    message: "RunEndEncoded data type is not yet supported".to_string(),
                });
            }
            DataType::ListView(_) | DataType::LargeListView(_) => {
                return Err(SchemaError::ConversionError {
                    message: format!("{} data type not yet supported", value),
                });
            }
        };

        Ok(res)
    }
}

fn arrow_field_to_proto_field(field: &arrow::datatypes::Field) -> Result<crate::schema::pb::Field> {
    let arrow_type =
        <&DataType as TryInto<crate::schema::pb::ArrowType>>::try_into(field.data_type())?;
    let id = field
        .metadata()
        .get(PARQUET_FIELD_ID_META_KEY)
        .and_then(|s| s.parse::<u64>().ok())
        .ok_or_else(|| SchemaError::ConversionError {
            message: format!(
                "Missing or invalid field ID metadata for field '{}'",
                field.name()
            ),
        })?;
    Ok(crate::schema::pb::Field {
        name: field.name().to_owned(),
        id,
        arrow_type: Some(Box::new(arrow_type)),
        nullable: field.is_nullable(),
        metadata: field.metadata().clone(),
    })
}

pub fn convert_arc_fields_to_proto_fields<'a, I>(fields: I) -> Result<Vec<crate::schema::pb::Field>>
where
    I: IntoIterator<Item = &'a Arc<arrow::datatypes::Field>>,
{
    fields
        .into_iter()
        .map(|field| arrow_field_to_proto_field(field.as_ref()))
        .collect()
}
