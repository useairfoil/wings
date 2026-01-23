// This code originally comes from the Apache Iceberg Rust project.
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
use crate::schema::{DataType, SchemaError, TimeUnit, error::Result};

#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub enum Datum {
    Null,
    Boolean(bool),
    UInt8(u8),
    Int8(i8),
    UInt16(u16),
    Int16(i16),
    UInt32(u32),
    Int32(i32),
    UInt64(u64),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Utf8(String),
    Binary(Vec<u8>),
    Date32(i32),
    Date64(i64),
    Timestamp(TimeUnit, i64),
    Duration(TimeUnit, i64),
}

impl Datum {
    pub fn bool(val: bool) -> Self {
        Self::Boolean(val)
    }

    pub fn u8(val: u8) -> Self {
        Self::UInt8(val)
    }

    pub fn i8(val: i8) -> Self {
        Self::Int8(val)
    }

    pub fn u16(val: u16) -> Self {
        Self::UInt16(val)
    }

    pub fn i16(val: i16) -> Self {
        Self::Int16(val)
    }

    pub fn u32(val: u32) -> Self {
        Self::UInt32(val)
    }

    pub fn i32(val: i32) -> Self {
        Self::Int32(val)
    }

    pub fn u64(val: u64) -> Self {
        Self::UInt64(val)
    }

    pub fn i64(val: i64) -> Self {
        Self::Int64(val)
    }

    pub fn f32(val: f32) -> Self {
        Self::Float32(val)
    }

    pub fn f64(val: f64) -> Self {
        Self::Float64(val)
    }

    pub fn utf8(val: impl Into<String>) -> Self {
        Self::Utf8(val.into())
    }

    pub fn binary(val: Vec<u8>) -> Self {
        Self::Binary(val)
    }

    pub fn date32(val: i32) -> Self {
        Self::Date32(val)
    }

    pub fn date64(val: i64) -> Self {
        Self::Date64(val)
    }

    pub fn timestamp(unit: TimeUnit, val: i64) -> Self {
        Self::Timestamp(unit, val)
    }

    pub fn duration(unit: TimeUnit, val: i64) -> Self {
        Self::Duration(unit, val)
    }

    pub fn data_type(&self) -> DataType {
        match self {
            Self::Null => DataType::Null,
            Self::Boolean(_) => DataType::Boolean,
            Self::UInt8(_) => DataType::UInt8,
            Self::Int8(_) => DataType::Int8,
            Self::UInt16(_) => DataType::UInt16,
            Self::Int16(_) => DataType::Int16,
            Self::UInt32(_) => DataType::UInt32,
            Self::Int32(_) => DataType::Int32,
            Self::UInt64(_) => DataType::UInt64,
            Self::Int64(_) => DataType::Int64,
            Self::Float32(_) => DataType::Float32,
            Self::Float64(_) => DataType::Float64,
            Self::Utf8(_) => DataType::Utf8,
            Self::Binary(_) => DataType::Binary,
            Self::Date32(_) => DataType::Date32,
            Self::Date64(_) => DataType::Date64,
            Self::Timestamp(unit, _) => DataType::Timestamp(*unit, None),
            Self::Duration(unit, _) => DataType::Duration(*unit),
        }
    }

    /// Create iceberg value from bytes.
    ///
    /// See [the Iceberg spec](https://iceberg.apache.org/spec/#binary-single-value-serialization) for reference.
    pub fn try_from_bytes(data_type: DataType, bytes: &[u8]) -> Result<Self> {
        let out = match data_type {
            DataType::Null => Self::Null,
            DataType::Boolean => {
                if bytes.len() == 1 && bytes[0] == 0 {
                    Self::bool(false)
                } else {
                    Self::bool(true)
                }
            }
            DataType::UInt8 => Self::u8(u8::from_le_bytes(bytes.try_into()?)),
            DataType::Int8 => Self::i8(i8::from_le_bytes(bytes.try_into()?)),
            DataType::UInt16 => Self::u16(u16::from_le_bytes(bytes.try_into()?)),
            DataType::Int16 => Self::i16(i16::from_le_bytes(bytes.try_into()?)),
            DataType::UInt32 => Self::u32(u32::from_le_bytes(bytes.try_into()?)),
            DataType::Int32 => Self::i32(i32::from_le_bytes(bytes.try_into()?)),
            DataType::UInt64 => Self::u64(u64::from_le_bytes(bytes.try_into()?)),
            DataType::Int64 => Self::i64(i64::from_le_bytes(bytes.try_into()?)),
            DataType::Float16 => return Err(SchemaError::UnsupportedDataType { data_type }),
            DataType::Float32 => Self::f32(f32::from_le_bytes(bytes.try_into()?)),
            DataType::Float64 => Self::f64(f64::from_le_bytes(bytes.try_into()?)),
            DataType::Utf8 => Self::utf8(String::from_utf8(bytes.to_vec())?),
            DataType::Binary => Self::binary(bytes.to_vec()),
            DataType::Date32 => Self::date32(i32::from_le_bytes(bytes.try_into()?)),
            DataType::Date64 => Self::date64(i64::from_le_bytes(bytes.try_into()?)),
            DataType::Timestamp(unit, _) => {
                Self::timestamp(unit, i64::from_le_bytes(bytes.try_into()?))
            }
            DataType::Duration(unit) => Self::duration(unit, i64::from_le_bytes(bytes.try_into()?)),
            DataType::List(_) | DataType::Struct(_) => {
                return Err(SchemaError::UnsupportedDataType { data_type });
            }
        };

        Ok(out)
    }

    /// Convert the value to bytes
    ///
    /// See [the Iceberg spec](https://iceberg.apache.org/spec/#binary-single-value-serialization) for reference.
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Self::Null => Vec::default(),
            Self::Boolean(v) => {
                if *v {
                    vec![1u8]
                } else {
                    vec![0u8]
                }
            }
            Self::UInt8(v) => v.to_le_bytes().to_vec(),
            Self::Int8(v) => v.to_le_bytes().to_vec(),
            Self::UInt16(v) => v.to_le_bytes().to_vec(),
            Self::Int16(v) => v.to_le_bytes().to_vec(),
            Self::UInt32(v) => v.to_le_bytes().to_vec(),
            Self::Int32(v) => v.to_le_bytes().to_vec(),
            Self::Int64(v) => v.to_le_bytes().to_vec(),
            Self::UInt64(v) => v.to_le_bytes().to_vec(),
            Self::Float32(v) => v.to_le_bytes().to_vec(),
            Self::Float64(v) => v.to_le_bytes().to_vec(),
            Self::Utf8(v) => v.as_bytes().to_vec(),
            Self::Binary(v) => v.clone(),
            Self::Date32(v) => v.to_le_bytes().to_vec(),
            Self::Date64(v) => v.to_le_bytes().to_vec(),
            Self::Timestamp(_unit, v) => v.to_le_bytes().to_vec(),
            Self::Duration(_unit, v) => v.to_le_bytes().to_vec(),
        }
    }
}
