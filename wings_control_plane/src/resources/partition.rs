//! Partition value.
//!
//! This module provides a restricted set of types that can be used as partition keys.
//! Unlike DataFusion's ScalarValue, PartitionValue only allows a limited set of types
//! that are suitable for partitioning data. Non-null data types are not nullable.

use std::convert::TryFrom;

use base64::{Engine, prelude::BASE64_STANDARD};
use datafusion::scalar::ScalarValue;
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};

use crate::schema::DataType;

/// Errors that can occur when converting partition values.
#[derive(Debug, Clone, PartialEq, Eq, Snafu)]
pub enum PartitionValueError {
    #[snafu(display("unsupported scalar value type for partitioning: {scalar_type}"))]
    UnsupportedScalarType { scalar_type: String },
    #[snafu(display("unexpected null value"))]
    NullValue,
}

#[derive(Debug, Clone, PartialEq, Snafu)]
pub enum PartitionValueParseError {
    #[snafu(display("failed to parse {data_type} integer value"))]
    InvalidIntValue {
        data_type: DataType,
        source: std::num::ParseIntError,
    },
    #[snafu(display("failed to parse {data_type} base64 value"))]
    InvalidBase64Value {
        data_type: DataType,
        source: base64::DecodeError,
    },
    #[snafu(display("unsupported data type for partitioning: {data_type}"))]
    UnsupportedDataType { data_type: DataType },
    #[snafu(display("expected value, got null"))]
    UnexpectedNull,
    #[snafu(display("expected null, got value"))]
    UnexpectedValue,
}

/// A restricted set of values that can be used for partitioning.
///
/// This enum only allows types that are suitable for partitioning data,
/// providing a more constrained alternative to DataFusion's ScalarValue.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum PartitionValue {
    /// Null value
    Null,

    // Signed integers
    /// 8-bit signed integer
    Int8(i8),
    /// 16-bit signed integer
    Int16(i16),
    /// 32-bit signed integer
    Int32(i32),
    /// 64-bit signed integer
    Int64(i64),

    // Unsigned integers
    /// 8-bit unsigned integer
    UInt8(u8),
    /// 16-bit unsigned integer
    UInt16(u16),
    /// 32-bit unsigned integer
    UInt32(u32),
    /// 64-bit unsigned integer
    UInt64(u64),

    /// UTF-8 string
    String(String),
    /// Binary data
    Bytes(Vec<u8>),
    /// Boolean value
    Boolean(bool),
}

impl PartitionValue {
    pub fn parse_with_datatype_option(
        data_type: Option<&DataType>,
        value: Option<&str>,
    ) -> Result<Option<Self>, PartitionValueParseError> {
        match (data_type, value) {
            (None, None) => Ok(None),
            (Some(data_type), Some(value)) => Self::parse_with_datatype(data_type, value).map(Some),
            (None, Some(_)) => Err(PartitionValueParseError::UnexpectedValue),
            (Some(_), None) => Err(PartitionValueParseError::UnexpectedNull),
        }
    }

    pub fn parse_with_datatype(
        data_type: &DataType,
        value: &str,
    ) -> Result<Self, PartitionValueParseError> {
        match data_type {
            DataType::UInt8 => {
                let parsed = value.parse::<u8>().context(InvalidIntValueSnafu {
                    data_type: DataType::UInt8,
                })?;
                Ok(PartitionValue::UInt8(parsed))
            }
            DataType::UInt16 => {
                let parsed = value.parse::<u16>().context(InvalidIntValueSnafu {
                    data_type: DataType::UInt16,
                })?;
                Ok(PartitionValue::UInt16(parsed))
            }
            DataType::UInt32 => {
                let parsed = value.parse::<u32>().context(InvalidIntValueSnafu {
                    data_type: DataType::UInt32,
                })?;
                Ok(PartitionValue::UInt32(parsed))
            }
            DataType::UInt64 => {
                let parsed = value.parse::<u64>().context(InvalidIntValueSnafu {
                    data_type: DataType::UInt64,
                })?;
                Ok(PartitionValue::UInt64(parsed))
            }
            DataType::Int8 => {
                let parsed = value.parse::<i8>().context(InvalidIntValueSnafu {
                    data_type: DataType::Int8,
                })?;
                Ok(PartitionValue::Int8(parsed))
            }
            DataType::Int16 => {
                let parsed = value.parse::<i16>().context(InvalidIntValueSnafu {
                    data_type: DataType::Int16,
                })?;
                Ok(PartitionValue::Int16(parsed))
            }
            DataType::Int32 => {
                let parsed = value.parse::<i32>().context(InvalidIntValueSnafu {
                    data_type: DataType::Int32,
                })?;
                Ok(PartitionValue::Int32(parsed))
            }
            DataType::Int64 => {
                let parsed = value.parse::<i64>().context(InvalidIntValueSnafu {
                    data_type: DataType::Int64,
                })?;
                Ok(PartitionValue::Int64(parsed))
            }
            DataType::Utf8 => Ok(PartitionValue::String(value.to_string())),
            DataType::Binary => {
                let bytes = BASE64_STANDARD
                    .decode(value)
                    .context(InvalidBase64ValueSnafu {
                        data_type: data_type.clone(),
                    })?;
                Ok(PartitionValue::Bytes(bytes))
            }
            _ => Err(PartitionValueParseError::UnsupportedDataType {
                data_type: data_type.clone(),
            }),
        }
    }

    /// Returns true if the value is null.
    pub fn is_null(&self) -> bool {
        matches!(self, PartitionValue::Null)
    }

    /// Returns the type name of the partition value.
    pub fn type_name(&self) -> &'static str {
        match self {
            PartitionValue::Null => "null",
            PartitionValue::Int8(_) => "int8",
            PartitionValue::Int16(_) => "int16",
            PartitionValue::Int32(_) => "int32",
            PartitionValue::Int64(_) => "int64",
            PartitionValue::UInt8(_) => "uint8",
            PartitionValue::UInt16(_) => "uint16",
            PartitionValue::UInt32(_) => "uint32",
            PartitionValue::UInt64(_) => "uint64",
            PartitionValue::String(_) => "string",
            PartitionValue::Bytes(_) => "bytes",
            PartitionValue::Boolean(_) => "boolean",
        }
    }

    pub fn data_type(&self) -> DataType {
        match self {
            PartitionValue::Null => DataType::Null,
            PartitionValue::Int8(_) => DataType::Int8,
            PartitionValue::Int16(_) => DataType::Int16,
            PartitionValue::Int32(_) => DataType::Int32,
            PartitionValue::Int64(_) => DataType::Int64,
            PartitionValue::UInt8(_) => DataType::UInt8,
            PartitionValue::UInt16(_) => DataType::UInt16,
            PartitionValue::UInt32(_) => DataType::UInt32,
            PartitionValue::UInt64(_) => DataType::UInt64,
            PartitionValue::String(_) => DataType::Utf8,
            PartitionValue::Bytes(_) => DataType::Binary,
            PartitionValue::Boolean(_) => DataType::Boolean,
        }
    }
}

impl std::fmt::Display for PartitionValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PartitionValue::Null => write!(f, "null"),
            PartitionValue::Int8(v) => write!(f, "{}", v),
            PartitionValue::Int16(v) => write!(f, "{}", v),
            PartitionValue::Int32(v) => write!(f, "{}", v),
            PartitionValue::Int64(v) => write!(f, "{}", v),
            PartitionValue::UInt8(v) => write!(f, "{}", v),
            PartitionValue::UInt16(v) => write!(f, "{}", v),
            PartitionValue::UInt32(v) => write!(f, "{}", v),
            PartitionValue::UInt64(v) => write!(f, "{}", v),
            PartitionValue::String(v) => write!(f, "{}", v),
            PartitionValue::Bytes(v) => write!(f, "{}", BASE64_STANDARD.encode(v)),
            PartitionValue::Boolean(v) => write!(f, "{}", v),
        }
    }
}

impl TryFrom<ScalarValue> for PartitionValue {
    type Error = PartitionValueError;

    fn try_from(value: ScalarValue) -> Result<Self, Self::Error> {
        match value {
            ScalarValue::Null => Ok(PartitionValue::Null),

            // Signed integers
            ScalarValue::Int8(Some(v)) => Ok(PartitionValue::Int8(v)),
            ScalarValue::Int16(Some(v)) => Ok(PartitionValue::Int16(v)),
            ScalarValue::Int32(Some(v)) => Ok(PartitionValue::Int32(v)),
            ScalarValue::Int64(Some(v)) => Ok(PartitionValue::Int64(v)),

            // Unsigned integers
            ScalarValue::UInt8(Some(v)) => Ok(PartitionValue::UInt8(v)),
            ScalarValue::UInt16(Some(v)) => Ok(PartitionValue::UInt16(v)),
            ScalarValue::UInt32(Some(v)) => Ok(PartitionValue::UInt32(v)),
            ScalarValue::UInt64(Some(v)) => Ok(PartitionValue::UInt64(v)),

            // String types
            ScalarValue::Utf8(Some(v)) => Ok(PartitionValue::String(v)),
            ScalarValue::LargeUtf8(Some(v)) => Ok(PartitionValue::String(v)),

            // Binary types
            ScalarValue::Binary(Some(v)) => Ok(PartitionValue::Bytes(v)),
            ScalarValue::LargeBinary(Some(v)) => Ok(PartitionValue::Bytes(v)),

            // Boolean
            ScalarValue::Boolean(Some(v)) => Ok(PartitionValue::Boolean(v)),

            // Null variants of supported types
            ScalarValue::Int8(None)
            | ScalarValue::Int16(None)
            | ScalarValue::Int32(None)
            | ScalarValue::Int64(None)
            | ScalarValue::UInt8(None)
            | ScalarValue::UInt16(None)
            | ScalarValue::UInt32(None)
            | ScalarValue::UInt64(None)
            | ScalarValue::Utf8(None)
            | ScalarValue::LargeUtf8(None)
            | ScalarValue::Binary(None)
            | ScalarValue::LargeBinary(None)
            | ScalarValue::Boolean(None) => Err(PartitionValueError::NullValue),

            // Unsupported types
            _ => Err(PartitionValueError::UnsupportedScalarType {
                scalar_type: format!("{:?}", value),
            }),
        }
    }
}

impl From<PartitionValue> for ScalarValue {
    fn from(value: PartitionValue) -> Self {
        match value {
            PartitionValue::Null => ScalarValue::Null,

            // Signed integers
            PartitionValue::Int8(v) => ScalarValue::Int8(Some(v)),
            PartitionValue::Int16(v) => ScalarValue::Int16(Some(v)),
            PartitionValue::Int32(v) => ScalarValue::Int32(Some(v)),
            PartitionValue::Int64(v) => ScalarValue::Int64(Some(v)),

            // Unsigned integers
            PartitionValue::UInt8(v) => ScalarValue::UInt8(Some(v)),
            PartitionValue::UInt16(v) => ScalarValue::UInt16(Some(v)),
            PartitionValue::UInt32(v) => ScalarValue::UInt32(Some(v)),
            PartitionValue::UInt64(v) => ScalarValue::UInt64(Some(v)),

            // String
            PartitionValue::String(v) => ScalarValue::Utf8(Some(v)),

            // Binary
            PartitionValue::Bytes(v) => ScalarValue::Binary(Some(v)),

            // Boolean
            PartitionValue::Boolean(v) => ScalarValue::Boolean(Some(v)),
        }
    }
}
