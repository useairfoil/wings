//! Conversions between offset registry domain types and protobuf types.

use std::sync::Arc;

use snafu::{ResultExt, ensure};

use crate::admin::TopicName;
use crate::offset_registry::error::InvalidTimestampSnafu;
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

impl TryFrom<pb::OffsetLocationResponse> for Option<OffsetLocation> {
    type Error = OffsetRegistryError;

    fn try_from(response: pb::OffsetLocationResponse) -> Result<Self, Self::Error> {
        use pb::offset_location_response::Location as ProtoLocation;

        let Some(location) = response.location else {
            return Ok(None);
        };

        match location {
            ProtoLocation::FolioLocation(folio) => {
                let inner = folio.try_into()?;
                Ok(OffsetLocation::Folio(inner).into())
            }
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
            batches: location.batches.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<pb::FolioLocation> for FolioLocation {
    type Error = OffsetRegistryError;

    fn try_from(location: pb::FolioLocation) -> Result<Self, Self::Error> {
        let batches = location
            .batches
            .into_iter()
            .map(TryFrom::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            file_ref: location.file_ref,
            offset_bytes: location.offset_bytes,
            size_bytes: location.size_bytes,
            batches,
        })
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

impl TryFrom<pb::CommitPageRequest> for CommitPageRequest {
    type Error = OffsetRegistryError;

    fn try_from(request: pb::CommitPageRequest) -> Result<Self, Self::Error> {
        let topic_name =
            TopicName::parse(&request.topic).map_err(|_| OffsetRegistryError::InvalidArgument {
                message: "invalid topic name format".to_string(),
            })?;

        let partition_value = request.partition.map(TryFrom::try_from).transpose()?;

        let batches = request
            .batches
            .into_iter()
            .map(TryFrom::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            topic_name,
            partition_value,
            batches,
            num_messages: request.num_messages,
            offset_bytes: request.offset_bytes,
            batch_size_bytes: request.batch_size_bytes,
        })
    }
}

impl TryFrom<&CommitPageRequest> for pb::CommitPageRequest {
    type Error = OffsetRegistryError;

    fn try_from(request: &CommitPageRequest) -> Result<Self, Self::Error> {
        let batches = request
            .batches
            .iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(pb::CommitPageRequest {
            topic: request.topic_name.to_string(),
            partition: request.partition_value.as_ref().map(Into::into),
            num_messages: request.num_messages,
            offset_bytes: request.offset_bytes,
            batch_size_bytes: request.batch_size_bytes,
            batches,
        })
    }
}

impl TryFrom<pb::CommitFolioResponse> for Vec<CommitPageResponse> {
    type Error = OffsetRegistryError;

    fn try_from(response: pb::CommitFolioResponse) -> Result<Self, Self::Error> {
        response.pages.into_iter().map(TryFrom::try_from).collect()
    }
}

impl From<CommitPageResponse> for pb::CommitPageResponse {
    fn from(response: CommitPageResponse) -> Self {
        let batches = response.batches.into_iter().map(Into::into).collect();
        Self {
            topic: response.topic_name.to_string(),
            partition: response.partition_value.as_ref().map(Into::into),
            batches,
        }
    }
}

impl TryFrom<pb::CommitPageResponse> for CommitPageResponse {
    type Error = OffsetRegistryError;

    fn try_from(response: pb::CommitPageResponse) -> Result<Self, Self::Error> {
        let topic_name = TopicName::parse(&response.topic)
            .context(InvalidResourceNameSnafu { resource: "topic" })?;

        let partition_value = response.partition.map(TryFrom::try_from).transpose()?;

        let batches = response
            .batches
            .into_iter()
            .map(TryFrom::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            topic_name,
            partition_value,
            batches,
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

impl TryFrom<pb::CommitBatchRequest> for CommitBatchRequest {
    type Error = OffsetRegistryError;

    fn try_from(meta: pb::CommitBatchRequest) -> Result<Self, Self::Error> {
        let Some(timestamp) = meta.timestamp else {
            return Ok(CommitBatchRequest {
                timestamp: None,
                num_messages: meta.num_messages,
            });
        };

        assert!(timestamp.seconds >= 0);
        assert!(timestamp.nanos >= 0);

        let timestamp = timestamp
            .try_into()
            .map_err(Arc::new)
            .context(InvalidTimestampSnafu {})?;

        Ok(CommitBatchRequest {
            timestamp: Some(timestamp),
            num_messages: meta.num_messages,
        })
    }
}

impl TryFrom<&CommitBatchRequest> for pb::CommitBatchRequest {
    type Error = OffsetRegistryError;

    fn try_from(meta: &CommitBatchRequest) -> Result<Self, Self::Error> {
        let timestamp = meta.timestamp.map(Into::into);

        Ok(pb::CommitBatchRequest {
            timestamp,
            num_messages: meta.num_messages,
        })
    }
}

impl TryFrom<pb::CommittedBatch> for CommittedBatch {
    type Error = OffsetRegistryError;

    fn try_from(meta: pb::CommittedBatch) -> Result<Self, OffsetRegistryError> {
        use pb::committed_batch::*;

        let result = meta.result.ok_or_else(|| OffsetRegistryError::Internal {
            message: "failed to convert CommittedWrite.result to CommittedWrite".to_string(),
        })?;

        match result {
            Result::Accepted(info) => {
                let timestamp = info
                    .timestamp
                    .ok_or_else(|| OffsetRegistryError::Internal {
                        message: "missing timestamp in committed write".to_string(),
                    })?
                    .try_into()
                    .map_err(Arc::new)
                    .context(InvalidTimestampSnafu {})?;
                let accepted = AcceptedBatchInfo {
                    start_offset: info.start_offset,
                    end_offset: info.end_offset,
                    timestamp,
                };
                Ok(CommittedBatch::Accepted(accepted))
            }
            Result::Rejected(rejected) => Ok(CommittedBatch::Rejected {
                num_messages: rejected.num_messages,
            }),
        }
    }
}

impl From<CommittedBatch> for pb::CommittedBatch {
    fn from(write: CommittedBatch) -> Self {
        use pb::committed_batch::*;

        match write {
            CommittedBatch::Accepted(info) => pb::CommittedBatch {
                result: Some(Result::Accepted(Accepted {
                    start_offset: info.start_offset,
                    end_offset: info.end_offset,
                    timestamp: Some((info.timestamp).into()),
                })),
            },
            CommittedBatch::Rejected { num_messages } => pb::CommittedBatch {
                result: Some(Result::Rejected(Rejected { num_messages })),
            },
        }
    }
}
