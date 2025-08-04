use arrow_schema::DataType;
use error_stack::ResultExt;
use wings_metadata_core::partition::PartitionValue;

use crate::error::{CliError, CliResult};

/// Convert a string partition value to the appropriate type based on the Arrow DataType
pub fn convert_partition_value(value: &str, data_type: &DataType) -> CliResult<PartitionValue> {
    match data_type {
        DataType::UInt8 => {
            let parsed = value
                .parse::<u8>()
                .change_context(CliError::InvalidArguments)
                .attach_printable_lazy(|| format!("Invalid u8 partition value: {value}"))?;
            Ok(PartitionValue::UInt8(parsed))
        }
        DataType::UInt16 => {
            let parsed = value
                .parse::<u16>()
                .change_context(CliError::InvalidArguments)
                .attach_printable_lazy(|| format!("Invalid u16 partition value: {value}"))?;
            Ok(PartitionValue::UInt16(parsed))
        }
        DataType::UInt32 => {
            let parsed = value
                .parse::<u32>()
                .change_context(CliError::InvalidArguments)
                .attach_printable_lazy(|| format!("Invalid u32 partition value: {value}"))?;
            Ok(PartitionValue::UInt32(parsed))
        }
        DataType::UInt64 => {
            let parsed = value
                .parse::<u64>()
                .change_context(CliError::InvalidArguments)
                .attach_printable_lazy(|| format!("Invalid u64 partition value: {value}"))?;
            Ok(PartitionValue::UInt64(parsed))
        }
        DataType::Int8 => {
            let parsed = value
                .parse::<i8>()
                .change_context(CliError::InvalidArguments)
                .attach_printable_lazy(|| format!("Invalid i8 partition value: {value}"))?;
            Ok(PartitionValue::Int8(parsed))
        }
        DataType::Int16 => {
            let parsed = value
                .parse::<i16>()
                .change_context(CliError::InvalidArguments)
                .attach_printable_lazy(|| format!("Invalid i16 partition value: {value}"))?;
            Ok(PartitionValue::Int16(parsed))
        }
        DataType::Int32 => {
            let parsed = value
                .parse::<i32>()
                .change_context(CliError::InvalidArguments)
                .attach_printable_lazy(|| format!("Invalid i32 partition value: {value}"))?;
            Ok(PartitionValue::Int32(parsed))
        }
        DataType::Int64 => {
            let parsed = value
                .parse::<i64>()
                .change_context(CliError::InvalidArguments)
                .attach_printable_lazy(|| format!("Invalid i64 partition value: {value}"))?;
            Ok(PartitionValue::Int64(parsed))
        }
        DataType::Utf8 | DataType::LargeUtf8 => Ok(PartitionValue::String(value.to_string())),
        DataType::Binary | DataType::LargeBinary => {
            // Handle hex encoding for binary data
            if let Some(stripped) = value.strip_prefix("0x") {
                let bytes = hex::decode(stripped)
                    .change_context(CliError::InvalidArguments)
                    .attach_printable_lazy(|| format!("Invalid hex partition value: {value}"))?;
                Ok(PartitionValue::Bytes(bytes))
            } else {
                let bytes = hex::decode(value)
                    .change_context(CliError::InvalidArguments)
                    .attach_printable_lazy(|| format!("Invalid hex partition value: {value}"))?;
                Ok(PartitionValue::Bytes(bytes))
            }
        }
        _ => Err(CliError::InvalidArguments)
            .attach_printable_lazy(|| format!("Unsupported partition key type: {data_type:?}")),
    }
}
