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
    DataType, Field, FieldRef, Schema, SchemaBuilder, TimeUnit,
    error::{Result, SchemaError},
};

impl TryFrom<&crate::schema::pb::Schema> for Schema {
    type Error = SchemaError;

    fn try_from(schema: &crate::schema::pb::Schema) -> Result<Self> {
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
                let list_type: &crate::schema::pb::Field =
                    list.field_type.as_deref().required("field_type")?;
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
    use crate::schema::pb::TimeUnit as ProtoTimeUnit;
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

pub fn parse_proto_fields_to_fields(fields: &[crate::schema::pb::Field]) -> Result<Vec<FieldRef>> {
    fields
        .iter()
        .map(|field| {
            let f: Field = field.try_into()?;
            Ok(f.into())
        })
        .collect()
}
