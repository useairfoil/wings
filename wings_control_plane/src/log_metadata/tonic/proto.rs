//! Conversions between log metadata domain types and protobuf types.

use std::sync::Arc;

use snafu::{ResultExt, ensure};

use crate::{
    log_metadata::{
        AcceptedBatchInfo, CommitBatchRequest, CommitPageRequest, CommitPageResponse,
        CommittedBatch, FolioLocation, GetLogLocationRequest, ListPartitionsRequest,
        ListPartitionsResponse, LogLocation, LogLocationRequest, LogMetadataError, LogOffset,
        PartitionMetadata, RejectedBatchInfo,
        error::{InvalidArgumentSnafu, InvalidResourceNameSnafu, InvalidTimestampSnafu},
    },
    resources::{PartitionValue, TopicName},
};

use super::pb;

impl TryFrom<pb::LogOffset> for LogOffset {
    type Error = LogMetadataError;

    fn try_from(offset: pb::LogOffset) -> Result<Self, Self::Error> {
        let timestamp = offset
            .timestamp
            .unwrap_or_default()
            .try_into()
            .map_err(Arc::new)
            .context(InvalidTimestampSnafu {})?;

        Ok(Self {
            offset: offset.offset,
            timestamp,
        })
    }
}

impl From<LogOffset> for pb::LogOffset {
    fn from(offset: LogOffset) -> Self {
        Self {
            offset: offset.offset,
            timestamp: Some(offset.timestamp.into()),
        }
    }
}

/*
 *     █████████     ███████    ██████   ██████ ██████   ██████ █████ ███████████
 *    ███░░░░░███  ███░░░░░███ ░░██████ ██████ ░░██████ ██████ ░░███ ░█░░░███░░░█
 *   ███     ░░░  ███     ░░███ ░███░█████░███  ░███░█████░███  ░███ ░   ░███  ░
 *  ░███         ░███      ░███ ░███░░███ ░███  ░███░░███ ░███  ░███     ░███
 *  ░███         ░███      ░███ ░███ ░░░  ░███  ░███ ░░░  ░███  ░███     ░███
 *  ░░███     ███░░███     ███  ░███      ░███  ░███      ░███  ░███     ░███
 *   ░░█████████  ░░░███████░   █████     █████ █████     █████ █████    █████
 *    ░░░░░░░░░     ░░░░░░░    ░░░░░     ░░░░░ ░░░░░     ░░░░░ ░░░░░    ░░░░░
 */

impl TryFrom<pb::CommitPageRequest> for CommitPageRequest {
    type Error = LogMetadataError;

    fn try_from(request: pb::CommitPageRequest) -> Result<Self, Self::Error> {
        let topic_name =
            TopicName::parse(&request.topic).map_err(|_| LogMetadataError::InvalidArgument {
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

impl From<&CommitPageRequest> for pb::CommitPageRequest {
    fn from(request: &CommitPageRequest) -> Self {
        let batches = request.batches.iter().map(Into::into).collect::<Vec<_>>();

        pb::CommitPageRequest {
            topic: request.topic_name.to_string(),
            partition: request.partition_value.as_ref().map(Into::into),
            num_messages: request.num_messages,
            offset_bytes: request.offset_bytes,
            batch_size_bytes: request.batch_size_bytes,
            batches,
        }
    }
}

impl TryFrom<pb::CommitBatchRequest> for CommitBatchRequest {
    type Error = LogMetadataError;

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

impl From<&CommitBatchRequest> for pb::CommitBatchRequest {
    fn from(meta: &CommitBatchRequest) -> Self {
        let timestamp = meta.timestamp.map(Into::into);

        pb::CommitBatchRequest {
            timestamp,
            num_messages: meta.num_messages,
        }
    }
}

impl TryFrom<pb::CommitFolioResponse> for Vec<CommitPageResponse> {
    type Error = LogMetadataError;

    fn try_from(response: pb::CommitFolioResponse) -> Result<Self, Self::Error> {
        response.pages.into_iter().map(TryFrom::try_from).collect()
    }
}

impl TryFrom<pb::CommitPageResponse> for CommitPageResponse {
    type Error = LogMetadataError;

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

impl TryFrom<pb::CommittedBatch> for CommittedBatch {
    type Error = LogMetadataError;

    fn try_from(meta: pb::CommittedBatch) -> Result<Self, LogMetadataError> {
        use pb::committed_batch::*;

        let result = meta.result.ok_or_else(|| LogMetadataError::Internal {
            message: "missing inner result in CommittedBatch proto".to_string(),
        })?;

        match result {
            Result::Accepted(info) => {
                let accepted = info.try_into()?;
                Ok(CommittedBatch::Accepted(accepted))
            }
            Result::Rejected(rejected) => Ok(CommittedBatch::Rejected(rejected.into())),
        }
    }
}

impl From<CommittedBatch> for pb::CommittedBatch {
    fn from(write: CommittedBatch) -> Self {
        use pb::committed_batch::*;

        match write {
            CommittedBatch::Accepted(info) => pb::CommittedBatch {
                result: Some(Result::Accepted(info.into())),
            },
            CommittedBatch::Rejected(rejected) => pb::CommittedBatch {
                result: Some(Result::Rejected(rejected.into())),
            },
        }
    }
}

impl TryFrom<pb::committed_batch::Accepted> for AcceptedBatchInfo {
    type Error = LogMetadataError;

    fn try_from(info: pb::committed_batch::Accepted) -> Result<Self, Self::Error> {
        let timestamp = info
            .timestamp
            .ok_or_else(|| LogMetadataError::Internal {
                message: "missing timestamp in CommittedBatch proto".to_string(),
            })?
            .try_into()
            .map_err(Arc::new)
            .context(InvalidTimestampSnafu {})?;
        Ok(AcceptedBatchInfo {
            start_offset: info.start_offset,
            end_offset: info.end_offset,
            timestamp,
        })
    }
}

impl From<AcceptedBatchInfo> for pb::committed_batch::Accepted {
    fn from(info: AcceptedBatchInfo) -> Self {
        pb::committed_batch::Accepted {
            start_offset: info.start_offset,
            end_offset: info.end_offset,
            timestamp: Some(info.timestamp.into()),
        }
    }
}

impl From<pb::committed_batch::Rejected> for RejectedBatchInfo {
    fn from(info: pb::committed_batch::Rejected) -> Self {
        RejectedBatchInfo {
            num_messages: info.num_messages,
        }
    }
}

impl From<RejectedBatchInfo> for pb::committed_batch::Rejected {
    fn from(info: RejectedBatchInfo) -> Self {
        pb::committed_batch::Rejected {
            num_messages: info.num_messages,
        }
    }
}

/*
 *  █████          ███████      █████████    █████████   ███████████ █████    ███████    ██████   █████
 * ░░███         ███░░░░░███   ███░░░░░███  ███░░░░░███ ░█░░░███░░░█░░███   ███░░░░░███ ░░██████ ░░███
 *  ░███        ███     ░░███ ███     ░░░  ░███    ░███ ░   ░███  ░  ░███  ███     ░░███ ░███░███ ░███
 *  ░███       ░███      ░███░███          ░███████████     ░███     ░███ ░███      ░███ ░███░░███░███
 *  ░███       ░███      ░███░███          ░███░░░░░███     ░███     ░███ ░███      ░███ ░███ ░░██████
 *  ░███      █░░███     ███ ░░███     ███ ░███    ░███     ░███     ░███ ░░███     ███  ░███  ░░█████
 *  ███████████ ░░░███████░   ░░█████████  █████   █████    █████    █████ ░░░███████░   █████  ░░█████
 * ░░░░░░░░░░░    ░░░░░░░      ░░░░░░░░░  ░░░░░   ░░░░░    ░░░░░    ░░░░░    ░░░░░░░    ░░░░░    ░░░░░
 */

impl TryFrom<pb::GetLogLocationRequest> for GetLogLocationRequest {
    type Error = LogMetadataError;

    fn try_from(request: pb::GetLogLocationRequest) -> Result<Self, Self::Error> {
        let topic_name = TopicName::parse(&request.topic)
            .context(InvalidResourceNameSnafu { resource: "topic" })?;

        let partition_value = request.partition.map(TryFrom::try_from).transpose()?;

        let deadline = request
            .deadline
            .map(TryInto::try_into)
            .transpose()
            .map_err(Arc::new)
            .context(InvalidTimestampSnafu {})?;

        let location = request
            .location
            .ok_or_else(|| LogMetadataError::Internal {
                message: "missing location in GetLocationRequest proto".to_string(),
            })?
            .into();

        Ok(GetLogLocationRequest {
            topic_name,
            partition_value,
            deadline,
            location,
        })
    }
}

impl From<GetLogLocationRequest> for pb::GetLogLocationRequest {
    fn from(request: GetLogLocationRequest) -> Self {
        pb::GetLogLocationRequest {
            topic: request.topic_name.to_string(),
            partition: request.partition_value.as_ref().map(Into::into),
            deadline: request.deadline.map(Into::into),
            location: Some(request.location.into()),
        }
    }
}

impl From<pb::get_log_location_request::Location> for LogLocationRequest {
    fn from(value: pb::get_log_location_request::Location) -> Self {
        use pb::get_log_location_request::Location;

        match value {
            Location::Offset(offset) => LogLocationRequest::Offset(offset),
        }
    }
}

impl From<LogLocationRequest> for pb::get_log_location_request::Location {
    fn from(value: LogLocationRequest) -> Self {
        use pb::get_log_location_request::Location;

        match value {
            LogLocationRequest::Offset(offset) => Location::Offset(offset),
        }
    }
}

impl TryFrom<pb::GetLogLocationResponse> for Option<LogLocation> {
    type Error = LogMetadataError;

    fn try_from(response: pb::GetLogLocationResponse) -> Result<Self, Self::Error> {
        use pb::get_log_location_response::Location as ProtoLocation;

        let Some(location) = response.location else {
            return Ok(None);
        };

        match location {
            ProtoLocation::FolioLocation(folio) => {
                let inner = folio.try_into()?;
                Ok(LogLocation::Folio(inner).into())
            }
        }
    }
}

impl From<Option<LogLocation>> for pb::GetLogLocationResponse {
    fn from(location: Option<LogLocation>) -> Self {
        use pb::get_log_location_response::Location;

        let inner = location.map(|location| match location {
            LogLocation::Folio(folio) => Location::FolioLocation(folio.into()),
        });

        pb::GetLogLocationResponse { location: inner }
    }
}

impl TryFrom<pb::FolioLocation> for FolioLocation {
    type Error = LogMetadataError;

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

/*
 *  ███████████    █████████   ███████████   ███████████ █████ ███████████ █████    ███████    ██████   █████  █████████
 * ░░███░░░░░███  ███░░░░░███ ░░███░░░░░███ ░█░░░███░░░█░░███ ░█░░░███░░░█░░███   ███░░░░░███ ░░██████ ░░███  ███░░░░░███
 *  ░███    ░███ ░███    ░███  ░███    ░███ ░   ░███  ░  ░███ ░   ░███  ░  ░███  ███     ░░███ ░███░███ ░███ ░███    ░░░
 *  ░██████████  ░███████████  ░██████████      ░███     ░███     ░███     ░███ ░███      ░███ ░███░░███░███ ░░█████████
 *  ░███░░░░░░   ░███░░░░░███  ░███░░░░░███     ░███     ░███     ░███     ░███ ░███      ░███ ░███ ░░██████  ░░░░░░░░███
 *  ░███         ░███    ░███  ░███    ░███     ░███     ░███     ░███     ░███ ░░███     ███  ░███  ░░█████  ███    ░███
 *  █████        █████   █████ █████   █████    █████    █████    █████    █████ ░░░███████░   █████  ░░█████░░█████████
 * ░░░░░        ░░░░░   ░░░░░ ░░░░░   ░░░░░    ░░░░░    ░░░░░    ░░░░░    ░░░░░    ░░░░░░░    ░░░░░    ░░░░░  ░░░░░░░░░
 */

impl TryFrom<pb::ListPartitionsRequest> for ListPartitionsRequest {
    type Error = LogMetadataError;

    fn try_from(request: pb::ListPartitionsRequest) -> Result<Self, Self::Error> {
        let topic_name = TopicName::parse(&request.topic)
            .context(InvalidResourceNameSnafu { resource: "topic" })?;

        Ok(ListPartitionsRequest {
            topic_name,
            page_size: request.page_size.map(|size| size as usize),
            page_token: request.page_token,
        })
    }
}

impl From<ListPartitionsRequest> for pb::ListPartitionsRequest {
    fn from(request: ListPartitionsRequest) -> Self {
        pb::ListPartitionsRequest {
            topic: request.topic_name.to_string(),
            page_size: request.page_size.map(|v| v as i32),
            page_token: request.page_token,
        }
    }
}

impl TryFrom<pb::ListPartitionsResponse> for ListPartitionsResponse {
    type Error = LogMetadataError;

    fn try_from(response: pb::ListPartitionsResponse) -> Result<Self, Self::Error> {
        let partitions = response
            .partitions
            .into_iter()
            .map(|value| value.try_into())
            .collect::<Result<Vec<_>, _>>()?;

        Ok(ListPartitionsResponse {
            partitions,
            next_page_token: response.next_page_token,
        })
    }
}

impl From<ListPartitionsResponse> for pb::ListPartitionsResponse {
    fn from(response: ListPartitionsResponse) -> Self {
        pb::ListPartitionsResponse {
            partitions: response.partitions.into_iter().map(Into::into).collect(),
            next_page_token: response.next_page_token,
        }
    }
}

impl TryFrom<pb::PartitionMetadata> for PartitionMetadata {
    type Error = LogMetadataError;

    fn try_from(metadata: pb::PartitionMetadata) -> Result<Self, Self::Error> {
        let partition_value = metadata.value.map(TryInto::try_into).transpose()?;
        let end_offset = metadata
            .end_offset
            .ok_or_else(|| LogMetadataError::Internal {
                message: "missing end_offset in PartitionMetadata proto".to_string(),
            })?
            .try_into()?;
        Ok(PartitionMetadata {
            partition_value,
            end_offset,
        })
    }
}

impl From<PartitionMetadata> for pb::PartitionMetadata {
    fn from(metadata: PartitionMetadata) -> Self {
        pb::PartitionMetadata {
            value: metadata.partition_value.as_ref().map(Into::into),
            end_offset: Some(metadata.end_offset.into()),
        }
    }
}

/*
 *  ███████████  █████   █████
 * ░░███░░░░░███░░███   ░░███
 *  ░███    ░███ ░███    ░███
 *  ░██████████  ░███    ░███
 *  ░███░░░░░░   ░░███   ███
 *  ░███          ░░░█████░
 *  █████           ░░███
 * ░░░░░             ░░░
 */

impl TryFrom<pb::PartitionValue> for PartitionValue {
    type Error = LogMetadataError;

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
