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

use crate::schema::{DataType, Datum, Field, Schema, TimeUnit, pb};

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
        use crate::schema::pb::{self as pb, arrow_type::ArrowTypeEnum};

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

pub fn arc_fields_to_proto_fields<'a, I>(fields: I) -> Vec<pb::Field>
where
    I: IntoIterator<Item = &'a Arc<Field>>,
{
    fields
        .into_iter()
        .map(|field| field.as_ref().into())
        .collect()
}
