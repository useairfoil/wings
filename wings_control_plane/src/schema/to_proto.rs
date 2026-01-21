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

use crate::schema::{
    DataType, Field, Schema, TimeUnit,
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
                field_type: Some(Box::new(field_to_proto_field(item_type.as_ref())?)),
            })),
            DataType::Struct(struct_fields) => ArrowTypeEnum::Struct(pb::Struct {
                sub_field_types: arc_fields_to_proto_fields(struct_fields.into_iter())?,
            }),
        };

        Ok(res)
    }
}

fn field_to_proto_field(field: &Field) -> Result<crate::schema::pb::Field> {
    field.try_into()
}

pub fn arc_fields_to_proto_fields<'a, I>(fields: I) -> Result<Vec<crate::schema::pb::Field>>
where
    I: IntoIterator<Item = &'a Arc<Field>>,
{
    fields
        .into_iter()
        .map(|field| field_to_proto_field(field.as_ref()))
        .collect()
}
