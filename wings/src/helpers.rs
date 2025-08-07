use arrow_schema::DataType;
use wings_metadata_core::partition::PartitionValue;

use crate::error::{CliError, Result};

/// Convert a string partition value to the appropriate type based on the Arrow DataType
pub fn convert_partition_value(value: &str, data_type: &DataType) -> Result<PartitionValue> {
    match data_type {
        DataType::UInt8 => {
            let parsed = value
                .parse::<u8>()
                .map_err(|_| CliError::InvalidPartitionValue)?;
            Ok(PartitionValue::UInt8(parsed))
        }
        DataType::UInt16 => {
            let parsed = value
                .parse::<u16>()
                .map_err(|_| CliError::InvalidPartitionValue)?;
            Ok(PartitionValue::UInt16(parsed))
        }
        DataType::UInt32 => {
            let parsed = value
                .parse::<u32>()
                .map_err(|_| CliError::InvalidPartitionValue)?;
            Ok(PartitionValue::UInt32(parsed))
        }
        DataType::UInt64 => {
            let parsed = value
                .parse::<u64>()
                .map_err(|_| CliError::InvalidPartitionValue)?;
            Ok(PartitionValue::UInt64(parsed))
        }
        DataType::Int8 => {
            let parsed = value
                .parse::<i8>()
                .map_err(|_| CliError::InvalidPartitionValue)?;
            Ok(PartitionValue::Int8(parsed))
        }
        DataType::Int16 => {
            let parsed = value
                .parse::<i16>()
                .map_err(|_| CliError::InvalidPartitionValue)?;
            Ok(PartitionValue::Int16(parsed))
        }
        DataType::Int32 => {
            let parsed = value
                .parse::<i32>()
                .map_err(|_| CliError::InvalidPartitionValue)?;
            Ok(PartitionValue::Int32(parsed))
        }
        DataType::Int64 => {
            let parsed = value
                .parse::<i64>()
                .map_err(|_| CliError::InvalidPartitionValue)?;
            Ok(PartitionValue::Int64(parsed))
        }
        DataType::Utf8 | DataType::LargeUtf8 => Ok(PartitionValue::String(value.to_string())),
        DataType::Binary | DataType::LargeBinary => {
            // Handle hex encoding for binary data
            if let Some(stripped) = value.strip_prefix("0x") {
                let bytes = hex::decode(stripped).map_err(|_| CliError::InvalidPartitionValue)?;
                Ok(PartitionValue::Bytes(bytes))
            } else {
                let bytes = hex::decode(value).map_err(|_| CliError::InvalidPartitionValue)?;
                Ok(PartitionValue::Bytes(bytes))
            }
        }
        _ => Err(CliError::InvalidPartitionValue),
    }
}
