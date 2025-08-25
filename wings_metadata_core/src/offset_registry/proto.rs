//! Conversions between offset registry domain types and protobuf types.

use std::time::{Duration, SystemTime};

use snafu::{ResultExt, ensure};

use crate::admin::TopicName;
use crate::offset_registry::types::*;
use crate::partition::PartitionValue;
use crate::protocol::wings::v1::{self as pb};

use super::error::{InvalidArgumentSnafu, InvalidResourceNameSnafu, OffsetRegistryError};

impl From<Option<OffsetLocation>> for pb::OffsetLocationResponse {
    fn from(location: Option<OffsetLocation>) -> Self {
        let inner = location.map(|location| match location {
            OffsetLocation::Folio(folio) => {
                pb::offset_location_response::Location::FolioLocation(folio.into())
            }
        });

        pb::OffsetLocationResponse { location: inner }
    }
}

impl From<pb::OffsetLocationResponse> for Option<OffsetLocation> {
    fn from(response: pb::OffsetLocationResponse) -> Self {
        use pb::offset_location_response::Location as ProtoLocation;

        let location = response.location?;

        match location {
            ProtoLocation::FolioLocation(folio) => OffsetLocation::Folio(folio.into()).into(),
        }
    }
}

impl From<ListTopicPartitionStatesResponse> for pb::ListTopicPartitionStatesResponse {
    fn from(value: ListTopicPartitionStatesResponse) -> Self {
        pb::ListTopicPartitionStatesResponse {
            states: value.states.into_iter().map(Into::into).collect(),
            next_page_token: value.next_page_token,
        }
    }
}

impl From<FolioLocation> for pb::FolioLocation {
    fn from(location: FolioLocation) -> Self {
        pb::FolioLocation {
            file_ref: location.file_ref,
            offset_bytes: location.offset_bytes,
            size_bytes: location.size_bytes,
            start_offset: location.start_offset,
            end_offset: location.end_offset,
        }
    }
}

impl From<pb::FolioLocation> for FolioLocation {
    fn from(location: pb::FolioLocation) -> Self {
        Self {
            file_ref: location.file_ref,
            offset_bytes: location.offset_bytes,
            size_bytes: location.size_bytes,
            start_offset: location.start_offset,
            end_offset: location.end_offset,
        }
    }
}

impl From<PartitionValueState> for pb::PartitionValueState {
    fn from(state: PartitionValueState) -> Self {
        pb::PartitionValueState {
            value: state.partition_value.as_ref().map(Into::into),
            next_offset: state.next_offset,
        }
    }
}

impl TryFrom<pb::PartitionValueState> for PartitionValueState {
    type Error = OffsetRegistryError;

    fn try_from(state: pb::PartitionValueState) -> Result<Self, Self::Error> {
        let value = state.value.map(TryFrom::try_from).transpose()?;

        Ok(Self {
            partition_value: value,
            next_offset: state.next_offset,
        })
    }
}

impl TryFrom<pb::BatchToCommit> for BatchToCommit {
    type Error = OffsetRegistryError;

    fn try_from(batch: pb::BatchToCommit) -> Result<Self, Self::Error> {
        let topic_name =
            TopicName::parse(&batch.topic).map_err(|_| OffsetRegistryError::InvalidArgument {
                message: "invalid topic name format".to_string(),
            })?;

        let partition_value = batch.partition.map(TryFrom::try_from).transpose()?;
        let metadata = batch
            .metadata
            .into_iter()
            .map(TryFrom::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            topic_name,
            partition_value,
            metadata,
            num_messages: batch.num_messages,
            offset_bytes: batch.offset_bytes,
            batch_size_bytes: batch.batch_size_bytes,
        })
    }
}

impl TryFrom<&BatchToCommit> for pb::BatchToCommit {
    type Error = OffsetRegistryError;

    fn try_from(batch: &BatchToCommit) -> Result<Self, Self::Error> {
        let metadata = batch
            .metadata
            .iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(pb::BatchToCommit {
            topic: batch.topic_name.to_string(),
            partition: batch.partition_value.as_ref().map(Into::into),
            metadata,
            num_messages: batch.num_messages,
            offset_bytes: batch.offset_bytes,
            batch_size_bytes: batch.batch_size_bytes,
        })
    }
}

impl TryFrom<pb::CommitFolioResponse> for Vec<CommittedBatch> {
    type Error = OffsetRegistryError;

    fn try_from(response: pb::CommitFolioResponse) -> Result<Self, Self::Error> {
        response
            .batches
            .into_iter()
            .map(TryFrom::try_from)
            .collect()
    }
}

impl From<CommittedBatch> for pb::CommittedBatch {
    fn from(batch: CommittedBatch) -> Self {
        Self {
            topic: batch.topic_name.to_string(),
            partition: batch.partition_value.as_ref().map(Into::into),
            start_offset: batch.start_offset,
            end_offset: batch.end_offset,
        }
    }
}

impl TryFrom<pb::CommittedBatch> for CommittedBatch {
    type Error = OffsetRegistryError;

    fn try_from(batch: pb::CommittedBatch) -> Result<Self, Self::Error> {
        let topic_name = TopicName::parse(&batch.topic)
            .context(InvalidResourceNameSnafu { resource: "topic" })?;

        let partition_value = batch.partition.map(TryFrom::try_from).transpose()?;

        Ok(Self {
            topic_name,
            partition_value,
            start_offset: batch.start_offset,
            end_offset: batch.end_offset,
        })
    }
}

impl TryFrom<pb::PartitionValue> for PartitionValue {
    type Error = OffsetRegistryError;

    fn try_from(value: pb::PartitionValue) -> Result<Self, Self::Error> {
        use pb::partition_value::Value;

        match value.value {
            Some(Value::NullValue(_)) => Ok(PartitionValue::Null),
            Some(Value::Int8Value(v)) => {
                ensure!(
                    v >= i8::MIN as i32 && v <= i8::MAX as i32,
                    InvalidArgumentSnafu {
                        message: format!("Int8 value out of range: {v}")
                    }
                );

                Ok(PartitionValue::Int8(v as i8))
            }
            Some(Value::Int16Value(v)) => {
                ensure!(
                    v >= i16::MIN as i32 && v <= i16::MAX as i32,
                    InvalidArgumentSnafu {
                        message: format!("Int16 value out of range: {v}")
                    }
                );

                Ok(PartitionValue::Int16(v as i16))
            }
            Some(Value::Int32Value(v)) => Ok(PartitionValue::Int32(v)),
            Some(Value::Int64Value(v)) => Ok(PartitionValue::Int64(v)),
            Some(Value::Uint8Value(v)) => {
                ensure!(
                    v <= u8::MAX as u32,
                    InvalidArgumentSnafu {
                        message: format!("UInt8 value out of range: {v}")
                    }
                );

                Ok(PartitionValue::UInt8(v as u8))
            }
            Some(Value::Uint16Value(v)) => {
                ensure!(
                    v <= u16::MAX as u32,
                    InvalidArgumentSnafu {
                        message: format!("UInt16 value out of range: {v}")
                    }
                );

                Ok(PartitionValue::UInt16(v as u16))
            }
            Some(Value::Uint32Value(v)) => Ok(PartitionValue::UInt32(v)),
            Some(Value::Uint64Value(v)) => Ok(PartitionValue::UInt64(v)),
            Some(Value::StringValue(v)) => Ok(PartitionValue::String(v)),
            Some(Value::BytesValue(v)) => Ok(PartitionValue::Bytes(v)),
            Some(Value::BoolValue(v)) => Ok(PartitionValue::Boolean(v)),
            None => InvalidArgumentSnafu {
                message: "Missing partition value".to_string(),
            }
            .fail(),
        }
    }
}

impl From<&PartitionValue> for pb::PartitionValue {
    fn from(value: &PartitionValue) -> Self {
        use pb::partition_value::Value;

        let value = match value {
            PartitionValue::Null => Value::NullValue(()),
            PartitionValue::Int8(v) => Value::Int8Value(*v as i32),
            PartitionValue::Int16(v) => Value::Int16Value(*v as i32),
            PartitionValue::Int32(v) => Value::Int32Value(*v),
            PartitionValue::Int64(v) => Value::Int64Value(*v),
            PartitionValue::UInt8(v) => Value::Uint8Value(*v as u32),
            PartitionValue::UInt16(v) => Value::Uint16Value(*v as u32),
            PartitionValue::UInt32(v) => Value::Uint32Value(*v),
            PartitionValue::UInt64(v) => Value::Uint64Value(*v),
            PartitionValue::String(v) => Value::StringValue(v.to_string()),
            PartitionValue::Bytes(v) => Value::BytesValue(v.clone()),
            PartitionValue::Boolean(v) => Value::BoolValue(*v),
        };

        pb::PartitionValue { value: Some(value) }
    }
}

impl TryFrom<pb::RecordBatchMetadata> for RecordBatchMetadata {
    type Error = OffsetRegistryError;

    fn try_from(value: pb::RecordBatchMetadata) -> Result<Self, Self::Error> {
        let Some(timestamp) = value.timestamp else {
            return Ok(RecordBatchMetadata { timestamp: None });
        };

        assert!(timestamp.seconds >= 0);
        assert!(timestamp.nanos >= 0);

        let millis_since_epoch =
            (timestamp.seconds as u64) * 1000 + (timestamp.nanos as u64) / 1_000_000;
        let duration_since_epoch = Duration::from_millis(millis_since_epoch);
        let timestamp = SystemTime::UNIX_EPOCH
            .checked_add(duration_since_epoch)
            .ok_or_else(|| OffsetRegistryError::Internal {
                message: "failed to create RecordBatchMetadata.timestamp from Protobuf".to_string(),
            })?;

        Ok(RecordBatchMetadata {
            timestamp: Some(timestamp),
        })
    }
}

impl TryFrom<&RecordBatchMetadata> for pb::RecordBatchMetadata {
    type Error = OffsetRegistryError;

    fn try_from(meta: &RecordBatchMetadata) -> Result<Self, Self::Error> {
        let timestamp = meta
            .timestamp
            .map(|ts| {
                let duration_since_epoch =
                    ts.duration_since(SystemTime::UNIX_EPOCH).map_err(|_| {
                        OffsetRegistryError::Internal {
                            message: "failed to convert RecordBatchMetadata.timestamp to Protobuf"
                                .to_string(),
                        }
                    })?;
                let seconds = duration_since_epoch.as_secs();
                let nanos = duration_since_epoch.subsec_nanos();
                Ok(prost_types::Timestamp {
                    seconds: seconds as _,
                    nanos: nanos as _,
                })
            })
            .transpose()?;

        Ok(pb::RecordBatchMetadata { timestamp })
    }
}
