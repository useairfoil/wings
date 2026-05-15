//! Partition value.
//!
//! This module provides a restricted set of types that can be used as partition keys.
//! Unlike DataFusion's ScalarValue, PartitionValue only allows a limited set of types
//! that are suitable for partitioning data. Non-null data types are not nullable.

use std::convert::TryFrom;

use base64::{Engine, prelude::BASE64_STANDARD};
use datafusion::{prelude::Expr, scalar::ScalarValue};
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use wings_schema::DataType;

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

    pub fn into_lit(self) -> Expr {
        let scalar: ScalarValue = self.into();
        Expr::Literal(scalar, None)
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

#[cfg(test)]
mod tests {
    use datafusion::scalar::ScalarValue;

    use super::*;

    #[test]
    fn test_partition_value_null() {
        let pv = PartitionValue::Null;
        assert!(pv.is_null());
        assert_eq!(pv.type_name(), "null");
        assert_eq!(pv.data_type(), DataType::Null);
        assert_eq!(pv.to_string(), "null");
    }

    #[test]
    fn test_partition_value_int8() {
        let pv = PartitionValue::Int8(42);
        assert!(!pv.is_null());
        assert_eq!(pv.type_name(), "int8");
        assert_eq!(pv.data_type(), DataType::Int8);
        assert_eq!(pv.to_string(), "42");

        let pv = PartitionValue::Int8(-128);
        assert_eq!(pv.to_string(), "-128");
    }

    #[test]
    fn test_partition_value_int16() {
        let pv = PartitionValue::Int16(1000);
        assert!(!pv.is_null());
        assert_eq!(pv.type_name(), "int16");
        assert_eq!(pv.data_type(), DataType::Int16);
        assert_eq!(pv.to_string(), "1000");
    }

    #[test]
    fn test_partition_value_int32() {
        let pv = PartitionValue::Int32(100000);
        assert!(!pv.is_null());
        assert_eq!(pv.type_name(), "int32");
        assert_eq!(pv.data_type(), DataType::Int32);
        assert_eq!(pv.to_string(), "100000");
    }

    #[test]
    fn test_partition_value_int64() {
        let pv = PartitionValue::Int64(10000000000);
        assert!(!pv.is_null());
        assert_eq!(pv.type_name(), "int64");
        assert_eq!(pv.data_type(), DataType::Int64);
        assert_eq!(pv.to_string(), "10000000000");
    }

    #[test]
    fn test_partition_value_uint8() {
        let pv = PartitionValue::UInt8(255);
        assert!(!pv.is_null());
        assert_eq!(pv.type_name(), "uint8");
        assert_eq!(pv.data_type(), DataType::UInt8);
        assert_eq!(pv.to_string(), "255");
    }

    #[test]
    fn test_partition_value_uint16() {
        let pv = PartitionValue::UInt16(65535);
        assert!(!pv.is_null());
        assert_eq!(pv.type_name(), "uint16");
        assert_eq!(pv.data_type(), DataType::UInt16);
        assert_eq!(pv.to_string(), "65535");
    }

    #[test]
    fn test_partition_value_uint32() {
        let pv = PartitionValue::UInt32(4294967295);
        assert!(!pv.is_null());
        assert_eq!(pv.type_name(), "uint32");
        assert_eq!(pv.data_type(), DataType::UInt32);
        assert_eq!(pv.to_string(), "4294967295");
    }

    #[test]
    fn test_partition_value_uint64() {
        let pv = PartitionValue::UInt64(18446744073709551615);
        assert!(!pv.is_null());
        assert_eq!(pv.type_name(), "uint64");
        assert_eq!(pv.data_type(), DataType::UInt64);
        assert_eq!(pv.to_string(), "18446744073709551615");
    }

    #[test]
    fn test_partition_value_string() {
        let pv = PartitionValue::String("hello".to_string());
        assert!(!pv.is_null());
        assert_eq!(pv.type_name(), "string");
        assert_eq!(pv.data_type(), DataType::Utf8);
        assert_eq!(pv.to_string(), "hello");
    }

    #[test]
    fn test_partition_value_bytes() {
        let bytes = vec![0x00, 0x01, 0x02, 0x03];
        let pv = PartitionValue::Bytes(bytes.clone());
        assert!(!pv.is_null());
        assert_eq!(pv.type_name(), "bytes");
        assert_eq!(pv.data_type(), DataType::Binary);
        assert_eq!(pv.to_string(), BASE64_STANDARD.encode(&bytes));
    }

    #[test]
    fn test_partition_value_boolean() {
        let pv_true = PartitionValue::Boolean(true);
        assert!(!pv_true.is_null());
        assert_eq!(pv_true.type_name(), "boolean");
        assert_eq!(pv_true.data_type(), DataType::Boolean);
        assert_eq!(pv_true.to_string(), "true");

        let pv_false = PartitionValue::Boolean(false);
        assert_eq!(pv_false.to_string(), "false");
    }

    #[test]
    fn test_parse_with_datatype_uint8() {
        let pv = PartitionValue::parse_with_datatype(&DataType::UInt8, "255").unwrap();
        assert_eq!(pv, PartitionValue::UInt8(255));

        let result = PartitionValue::parse_with_datatype(&DataType::UInt8, "256");
        assert!(matches!(
            result,
            Err(PartitionValueParseError::InvalidIntValue { .. })
        ));

        let result = PartitionValue::parse_with_datatype(&DataType::UInt8, "-1");
        assert!(matches!(
            result,
            Err(PartitionValueParseError::InvalidIntValue { .. })
        ));
    }

    #[test]
    fn test_parse_with_datatype_uint16() {
        let pv = PartitionValue::parse_with_datatype(&DataType::UInt16, "65535").unwrap();
        assert_eq!(pv, PartitionValue::UInt16(65535));
    }

    #[test]
    fn test_parse_with_datatype_uint32() {
        let pv = PartitionValue::parse_with_datatype(&DataType::UInt32, "4294967295").unwrap();
        assert_eq!(pv, PartitionValue::UInt32(4294967295));
    }

    #[test]
    fn test_parse_with_datatype_uint64() {
        let pv =
            PartitionValue::parse_with_datatype(&DataType::UInt64, "18446744073709551615").unwrap();
        assert_eq!(pv, PartitionValue::UInt64(18446744073709551615));
    }

    #[test]
    fn test_parse_with_datatype_int8() {
        let pv = PartitionValue::parse_with_datatype(&DataType::Int8, "127").unwrap();
        assert_eq!(pv, PartitionValue::Int8(127));

        let pv = PartitionValue::parse_with_datatype(&DataType::Int8, "-128").unwrap();
        assert_eq!(pv, PartitionValue::Int8(-128));
    }

    #[test]
    fn test_parse_with_datatype_int16() {
        let pv = PartitionValue::parse_with_datatype(&DataType::Int16, "32767").unwrap();
        assert_eq!(pv, PartitionValue::Int16(32767));
    }

    #[test]
    fn test_parse_with_datatype_int32() {
        let pv = PartitionValue::parse_with_datatype(&DataType::Int32, "2147483647").unwrap();
        assert_eq!(pv, PartitionValue::Int32(2147483647));
    }

    #[test]
    fn test_parse_with_datatype_int64() {
        let pv =
            PartitionValue::parse_with_datatype(&DataType::Int64, "9223372036854775807").unwrap();
        assert_eq!(pv, PartitionValue::Int64(9223372036854775807));
    }

    #[test]
    fn test_parse_with_datatype_utf8() {
        let pv = PartitionValue::parse_with_datatype(&DataType::Utf8, "hello world").unwrap();
        assert_eq!(pv, PartitionValue::String("hello world".to_string()));

        let pv = PartitionValue::parse_with_datatype(&DataType::Utf8, "").unwrap();
        assert_eq!(pv, PartitionValue::String("".to_string()));
    }

    #[test]
    fn test_parse_with_datatype_binary() {
        let encoded = BASE64_STANDARD.encode("hello");
        let pv = PartitionValue::parse_with_datatype(&DataType::Binary, &encoded).unwrap();
        assert_eq!(
            pv,
            PartitionValue::Bytes(vec![0x68, 0x65, 0x6c, 0x6c, 0x6f])
        );

        // Test invalid base64
        let result = PartitionValue::parse_with_datatype(&DataType::Binary, "not-valid-base64!!!");
        assert!(matches!(
            result,
            Err(PartitionValueParseError::InvalidBase64Value { .. })
        ));
    }

    #[test]
    fn test_parse_with_datatype_unsupported() {
        let result = PartitionValue::parse_with_datatype(&DataType::Float32, "1.0");
        assert!(matches!(
            result,
            Err(PartitionValueParseError::UnsupportedDataType { .. })
        ));

        let result = PartitionValue::parse_with_datatype(&DataType::Float64, "1.0");
        assert!(matches!(
            result,
            Err(PartitionValueParseError::UnsupportedDataType { .. })
        ));

        let result = PartitionValue::parse_with_datatype(&DataType::Boolean, "true");
        assert!(matches!(
            result,
            Err(PartitionValueParseError::UnsupportedDataType { .. })
        ));
    }

    #[test]
    fn test_parse_with_datatype_option() {
        // Both None
        let result = PartitionValue::parse_with_datatype_option(None, None).unwrap();
        assert_eq!(result, None);

        // Both Some
        let result =
            PartitionValue::parse_with_datatype_option(Some(&DataType::UInt8), Some("42")).unwrap();
        assert_eq!(result, Some(PartitionValue::UInt8(42)));

        // Some datatype, None value -> UnexpectedNull
        let result = PartitionValue::parse_with_datatype_option(Some(&DataType::UInt8), None);
        assert!(matches!(
            result,
            Err(PartitionValueParseError::UnexpectedNull)
        ));

        // None datatype, Some value -> UnexpectedValue
        let result = PartitionValue::parse_with_datatype_option(None, Some("42"));
        assert!(matches!(
            result,
            Err(PartitionValueParseError::UnexpectedValue)
        ));
    }

    #[test]
    fn test_try_from_scalar_value_null() {
        let scalar = ScalarValue::Null;
        let pv = PartitionValue::try_from(scalar).unwrap();
        assert_eq!(pv, PartitionValue::Null);
    }

    #[test]
    fn test_try_from_scalar_value_integers() {
        // Int8
        let scalar = ScalarValue::Int8(Some(42));
        let pv = PartitionValue::try_from(scalar).unwrap();
        assert_eq!(pv, PartitionValue::Int8(42));

        // Int16
        let scalar = ScalarValue::Int16(Some(1000));
        let pv = PartitionValue::try_from(scalar).unwrap();
        assert_eq!(pv, PartitionValue::Int16(1000));

        // Int32
        let scalar = ScalarValue::Int32(Some(100000));
        let pv = PartitionValue::try_from(scalar).unwrap();
        assert_eq!(pv, PartitionValue::Int32(100000));

        // Int64
        let scalar = ScalarValue::Int64(Some(10000000000));
        let pv = PartitionValue::try_from(scalar).unwrap();
        assert_eq!(pv, PartitionValue::Int64(10000000000));

        // UInt8
        let scalar = ScalarValue::UInt8(Some(255));
        let pv = PartitionValue::try_from(scalar).unwrap();
        assert_eq!(pv, PartitionValue::UInt8(255));

        // UInt16
        let scalar = ScalarValue::UInt16(Some(65535));
        let pv = PartitionValue::try_from(scalar).unwrap();
        assert_eq!(pv, PartitionValue::UInt16(65535));

        // UInt32
        let scalar = ScalarValue::UInt32(Some(4294967295));
        let pv = PartitionValue::try_from(scalar).unwrap();
        assert_eq!(pv, PartitionValue::UInt32(4294967295));

        // UInt64
        let scalar = ScalarValue::UInt64(Some(18446744073709551615));
        let pv = PartitionValue::try_from(scalar).unwrap();
        assert_eq!(pv, PartitionValue::UInt64(18446744073709551615));
    }

    #[test]
    fn test_try_from_scalar_value_strings() {
        // Utf8
        let scalar = ScalarValue::Utf8(Some("hello".to_string()));
        let pv = PartitionValue::try_from(scalar).unwrap();
        assert_eq!(pv, PartitionValue::String("hello".to_string()));

        // LargeUtf8
        let scalar = ScalarValue::LargeUtf8(Some("large hello".to_string()));
        let pv = PartitionValue::try_from(scalar).unwrap();
        assert_eq!(pv, PartitionValue::String("large hello".to_string()));
    }

    #[test]
    fn test_try_from_scalar_value_binary() {
        let bytes = vec![0x00, 0x01, 0x02, 0x03];

        // Binary
        let scalar = ScalarValue::Binary(Some(bytes.clone()));
        let pv = PartitionValue::try_from(scalar).unwrap();
        assert_eq!(pv, PartitionValue::Bytes(bytes.clone()));

        // LargeBinary
        let scalar = ScalarValue::LargeBinary(Some(bytes.clone()));
        let pv = PartitionValue::try_from(scalar).unwrap();
        assert_eq!(pv, PartitionValue::Bytes(bytes.clone()));
    }

    #[test]
    fn test_try_from_scalar_value_boolean() {
        let scalar = ScalarValue::Boolean(Some(true));
        let pv = PartitionValue::try_from(scalar).unwrap();
        assert_eq!(pv, PartitionValue::Boolean(true));

        let scalar = ScalarValue::Boolean(Some(false));
        let pv = PartitionValue::try_from(scalar).unwrap();
        assert_eq!(pv, PartitionValue::Boolean(false));
    }

    #[test]
    fn test_try_from_scalar_value_null_variants() {
        // Test that null variants of supported types return NullValue error
        let scalar = ScalarValue::Int8(None);
        let result = PartitionValue::try_from(scalar);
        assert!(matches!(result, Err(PartitionValueError::NullValue)));

        let scalar = ScalarValue::Int16(None);
        let result = PartitionValue::try_from(scalar);
        assert!(matches!(result, Err(PartitionValueError::NullValue)));

        let scalar = ScalarValue::Int32(None);
        let result = PartitionValue::try_from(scalar);
        assert!(matches!(result, Err(PartitionValueError::NullValue)));

        let scalar = ScalarValue::Int64(None);
        let result = PartitionValue::try_from(scalar);
        assert!(matches!(result, Err(PartitionValueError::NullValue)));

        let scalar = ScalarValue::UInt8(None);
        let result = PartitionValue::try_from(scalar);
        assert!(matches!(result, Err(PartitionValueError::NullValue)));

        let scalar = ScalarValue::UInt16(None);
        let result = PartitionValue::try_from(scalar);
        assert!(matches!(result, Err(PartitionValueError::NullValue)));

        let scalar = ScalarValue::UInt32(None);
        let result = PartitionValue::try_from(scalar);
        assert!(matches!(result, Err(PartitionValueError::NullValue)));

        let scalar = ScalarValue::UInt64(None);
        let result = PartitionValue::try_from(scalar);
        assert!(matches!(result, Err(PartitionValueError::NullValue)));

        let scalar = ScalarValue::Utf8(None);
        let result = PartitionValue::try_from(scalar);
        assert!(matches!(result, Err(PartitionValueError::NullValue)));

        let scalar = ScalarValue::LargeUtf8(None);
        let result = PartitionValue::try_from(scalar);
        assert!(matches!(result, Err(PartitionValueError::NullValue)));

        let scalar = ScalarValue::Binary(None);
        let result = PartitionValue::try_from(scalar);
        assert!(matches!(result, Err(PartitionValueError::NullValue)));

        let scalar = ScalarValue::LargeBinary(None);
        let result = PartitionValue::try_from(scalar);
        assert!(matches!(result, Err(PartitionValueError::NullValue)));

        let scalar = ScalarValue::Boolean(None);
        let result = PartitionValue::try_from(scalar);
        assert!(matches!(result, Err(PartitionValueError::NullValue)));
    }

    #[test]
    fn test_try_from_scalar_value_unsupported() {
        let scalar = ScalarValue::Float32(Some(1.0));
        let result = PartitionValue::try_from(scalar);
        assert!(matches!(
            result,
            Err(PartitionValueError::UnsupportedScalarType { .. })
        ));

        let scalar = ScalarValue::Float64(Some(1.0));
        let result = PartitionValue::try_from(scalar);
        assert!(matches!(
            result,
            Err(PartitionValueError::UnsupportedScalarType { .. })
        ));

        let scalar = ScalarValue::Decimal128(Some(1), 10, 2);
        let result = PartitionValue::try_from(scalar);
        assert!(matches!(
            result,
            Err(PartitionValueError::UnsupportedScalarType { .. })
        ));
    }

    #[test]
    fn test_from_partition_value_to_scalar() {
        // Null
        let pv = PartitionValue::Null;
        let scalar: ScalarValue = pv.into();
        assert_eq!(scalar, ScalarValue::Null);

        // Int8
        let pv = PartitionValue::Int8(42);
        let scalar: ScalarValue = pv.into();
        assert_eq!(scalar, ScalarValue::Int8(Some(42)));

        // Int16
        let pv = PartitionValue::Int16(1000);
        let scalar: ScalarValue = pv.into();
        assert_eq!(scalar, ScalarValue::Int16(Some(1000)));

        // Int32
        let pv = PartitionValue::Int32(100000);
        let scalar: ScalarValue = pv.into();
        assert_eq!(scalar, ScalarValue::Int32(Some(100000)));

        // Int64
        let pv = PartitionValue::Int64(10000000000);
        let scalar: ScalarValue = pv.into();
        assert_eq!(scalar, ScalarValue::Int64(Some(10000000000)));

        // UInt8
        let pv = PartitionValue::UInt8(255);
        let scalar: ScalarValue = pv.into();
        assert_eq!(scalar, ScalarValue::UInt8(Some(255)));

        // UInt16
        let pv = PartitionValue::UInt16(65535);
        let scalar: ScalarValue = pv.into();
        assert_eq!(scalar, ScalarValue::UInt16(Some(65535)));

        // UInt32
        let pv = PartitionValue::UInt32(4294967295);
        let scalar: ScalarValue = pv.into();
        assert_eq!(scalar, ScalarValue::UInt32(Some(4294967295)));

        // UInt64
        let pv = PartitionValue::UInt64(18446744073709551615);
        let scalar: ScalarValue = pv.into();
        assert_eq!(scalar, ScalarValue::UInt64(Some(18446744073709551615)));

        // String
        let pv = PartitionValue::String("hello".to_string());
        let scalar: ScalarValue = pv.into();
        assert_eq!(scalar, ScalarValue::Utf8(Some("hello".to_string())));

        // Bytes
        let bytes = vec![0x00, 0x01, 0x02, 0x03];
        let pv = PartitionValue::Bytes(bytes.clone());
        let scalar: ScalarValue = pv.into();
        assert_eq!(scalar, ScalarValue::Binary(Some(bytes)));

        // Boolean
        let pv = PartitionValue::Boolean(true);
        let scalar: ScalarValue = pv.into();
        assert_eq!(scalar, ScalarValue::Boolean(Some(true)));
    }

    #[test]
    fn test_partition_value_into_lit() {
        let pv = PartitionValue::Int32(42);
        let expr = pv.into_lit();
        // We can't easily compare Expr, but we can at least verify it doesn't panic
        assert!(matches!(expr, Expr::Literal(_, None)));
    }
}
