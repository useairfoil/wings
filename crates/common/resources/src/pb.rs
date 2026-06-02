use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum WireError {
    #[snafu(display("missing field: {field_name}"))]
    MissingField { field_name: String },
    #[snafu(display("{data_type} value {value} is out of range"))]
    OutOfRange {
        data_type: &'static str,
        value: String,
    },
}

type Result<T, E = WireError> = std::result::Result<T, E>;

#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct Empty {}

#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct PartitionValue {
    #[prost(
        oneof = "partition_value::Value",
        tags = "1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12"
    )]
    pub value: Option<partition_value::Value>,
}

pub mod partition_value {
    #[derive(Clone, PartialEq, Eq, ::prost::Oneof)]
    pub enum Value {
        #[prost(message, tag = "1")]
        Null(super::Empty),
        #[prost(int32, tag = "2")]
        Int8(i32),
        #[prost(int32, tag = "3")]
        Int16(i32),
        #[prost(int32, tag = "4")]
        Int32(i32),
        #[prost(int64, tag = "5")]
        Int64(i64),
        #[prost(uint32, tag = "6")]
        UInt8(u32),
        #[prost(uint32, tag = "7")]
        UInt16(u32),
        #[prost(uint32, tag = "8")]
        UInt32(u32),
        #[prost(uint64, tag = "9")]
        UInt64(u64),
        #[prost(string, tag = "10")]
        String(String),
        #[prost(bytes = "vec", tag = "11")]
        Bytes(Vec<u8>),
        #[prost(bool, tag = "12")]
        Boolean(bool),
    }
}

impl From<&crate::PartitionValue> for PartitionValue {
    fn from(value: &crate::PartitionValue) -> Self {
        use partition_value::Value;

        let value = match value {
            crate::PartitionValue::Null => Value::Null(Empty {}),
            crate::PartitionValue::Int8(value) => Value::Int8(i32::from(*value)),
            crate::PartitionValue::Int16(value) => Value::Int16(i32::from(*value)),
            crate::PartitionValue::Int32(value) => Value::Int32(*value),
            crate::PartitionValue::Int64(value) => Value::Int64(*value),
            crate::PartitionValue::UInt8(value) => Value::UInt8(u32::from(*value)),
            crate::PartitionValue::UInt16(value) => Value::UInt16(u32::from(*value)),
            crate::PartitionValue::UInt32(value) => Value::UInt32(*value),
            crate::PartitionValue::UInt64(value) => Value::UInt64(*value),
            crate::PartitionValue::String(value) => Value::String(value.clone()),
            crate::PartitionValue::Bytes(value) => Value::Bytes(value.clone()),
            crate::PartitionValue::Boolean(value) => Value::Boolean(*value),
        };

        Self { value: Some(value) }
    }
}

impl From<crate::PartitionValue> for PartitionValue {
    fn from(value: crate::PartitionValue) -> Self {
        (&value).into()
    }
}

impl TryFrom<&PartitionValue> for crate::PartitionValue {
    type Error = WireError;

    fn try_from(value: &PartitionValue) -> Result<Self> {
        use partition_value::Value;

        let value = value.value.as_ref().required("value")?;
        match value {
            Value::Null(_) => Ok(crate::PartitionValue::Null),
            Value::Int8(value) => Ok(crate::PartitionValue::Int8((*value).try_into().map_err(
                |_| WireError::OutOfRange {
                    data_type: "int8",
                    value: value.to_string(),
                },
            )?)),
            Value::Int16(value) => Ok(crate::PartitionValue::Int16((*value).try_into().map_err(
                |_| WireError::OutOfRange {
                    data_type: "int16",
                    value: value.to_string(),
                },
            )?)),
            Value::Int32(value) => Ok(crate::PartitionValue::Int32(*value)),
            Value::Int64(value) => Ok(crate::PartitionValue::Int64(*value)),
            Value::UInt8(value) => Ok(crate::PartitionValue::UInt8((*value).try_into().map_err(
                |_| WireError::OutOfRange {
                    data_type: "uint8",
                    value: value.to_string(),
                },
            )?)),
            Value::UInt16(value) => Ok(crate::PartitionValue::UInt16(
                (*value).try_into().map_err(|_| WireError::OutOfRange {
                    data_type: "uint16",
                    value: value.to_string(),
                })?,
            )),
            Value::UInt32(value) => Ok(crate::PartitionValue::UInt32(*value)),
            Value::UInt64(value) => Ok(crate::PartitionValue::UInt64(*value)),
            Value::String(value) => Ok(crate::PartitionValue::String(value.clone())),
            Value::Bytes(value) => Ok(crate::PartitionValue::Bytes(value.clone())),
            Value::Boolean(value) => Ok(crate::PartitionValue::Boolean(*value)),
        }
    }
}

impl TryFrom<PartitionValue> for crate::PartitionValue {
    type Error = WireError;

    fn try_from(value: PartitionValue) -> Result<Self> {
        (&value).try_into()
    }
}

trait Required<T> {
    fn required(self, field_name: impl Into<String>) -> Result<T>;
}

impl<T> Required<T> for Option<T> {
    fn required(self, field_name: impl Into<String>) -> Result<T> {
        self.ok_or_else(|| WireError::MissingField {
            field_name: field_name.into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{PartitionValue as WirePartitionValue, partition_value};

    #[test]
    fn partition_value_proto_roundtrip() {
        let values = vec![
            crate::PartitionValue::Null,
            crate::PartitionValue::Int8(-8),
            crate::PartitionValue::Int16(-16),
            crate::PartitionValue::Int32(-32),
            crate::PartitionValue::Int64(-64),
            crate::PartitionValue::UInt8(8),
            crate::PartitionValue::UInt16(16),
            crate::PartitionValue::UInt32(32),
            crate::PartitionValue::UInt64(64),
            crate::PartitionValue::String("partition-a".to_string()),
            crate::PartitionValue::Bytes(vec![0, 1, 2]),
            crate::PartitionValue::Boolean(true),
        ];

        for value in values {
            let wire = WirePartitionValue::from(&value);
            let decoded = crate::PartitionValue::try_from(wire).unwrap();
            assert_eq!(decoded, value);
        }
    }

    #[test]
    fn partition_value_proto_rejects_out_of_range_int8() {
        let wire = WirePartitionValue {
            value: Some(partition_value::Value::Int8(i32::from(i8::MAX) + 1)),
        };

        assert!(crate::PartitionValue::try_from(wire).is_err());
    }
}
