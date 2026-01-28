//! Conversions between log metadata domain types and protobuf types.

use std::{collections::HashMap, sync::Arc};

use bytesize::ByteSize;
use snafu::{ResultExt, ensure};

use crate::{
    log_metadata::{
        AcceptedBatchInfo, CommitBatchRequest, CommitPageRequest, CommitPageResponse, CommitResult,
        CommitTask, CommittedBatch, CompactionOperation, CompactionResult, CompactionTask,
        CompleteTaskRequest, CompleteTaskResponse, CreateTableResult, CreateTableTask, FileInfo,
        FolioLocation, GetLogLocationOptions, GetLogLocationRequest, ListPartitionsRequest,
        ListPartitionsResponse, LogLocation, LogMetadataError, LogOffset, PartitionMetadata,
        RejectedBatchInfo, RequestTaskRequest, RequestTaskResponse, Task, TaskCompletionResult,
        TaskMetadata, TaskResult, TaskStatus,
        error::{
            InvalidArgumentSnafu, InvalidDurationSnafu, InvalidResourceNameSnafu,
            InvalidTimestampSnafu,
        },
    },
    parquet::FileMetadata,
    resources::{PartitionValue, TopicName},
    schema::Datum,
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
            reason: info.reason,
        }
    }
}

impl From<RejectedBatchInfo> for pb::committed_batch::Rejected {
    fn from(info: RejectedBatchInfo) -> Self {
        pb::committed_batch::Rejected {
            num_messages: info.num_messages,
            reason: info.reason,
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

        let options = request
            .options
            .ok_or_else(|| LogMetadataError::Internal {
                message: "missing options in GetLocationRequest proto".to_string(),
            })?
            .try_into()?;

        Ok(GetLogLocationRequest {
            topic_name,
            partition_value,
            options,
            offset: request.offset,
        })
    }
}

impl TryFrom<pb::GetLogLocationOptions> for GetLogLocationOptions {
    type Error = LogMetadataError;

    fn try_from(options: pb::GetLogLocationOptions) -> Result<Self, Self::Error> {
        let deadline = options
            .deadline
            .ok_or_else(|| LogMetadataError::Internal {
                message: "missing deadline in GetLogLocationOptions proto".to_string(),
            })?
            .try_into()
            .map_err(Arc::new)
            .context(InvalidDurationSnafu {})?;

        Ok(GetLogLocationOptions {
            deadline,
            min_rows: options.min_rows as _,
            max_rows: options.max_rows as _,
        })
    }
}

impl TryFrom<GetLogLocationRequest> for pb::GetLogLocationRequest {
    type Error = LogMetadataError;

    fn try_from(request: GetLogLocationRequest) -> Result<Self, Self::Error> {
        let options = request.options.try_into()?;
        Ok(pb::GetLogLocationRequest {
            topic: request.topic_name.to_string(),
            partition: request.partition_value.as_ref().map(Into::into),
            offset: request.offset,
            options: Some(options),
        })
    }
}

impl TryFrom<GetLogLocationOptions> for pb::GetLogLocationOptions {
    type Error = LogMetadataError;

    fn try_from(options: GetLogLocationOptions) -> Result<Self, Self::Error> {
        let deadline = options
            .deadline
            .try_into()
            .map_err(Arc::new)
            .context(InvalidDurationSnafu {})?;

        Ok(Self {
            deadline: Some(deadline),
            min_rows: options.min_rows as _,
            max_rows: options.max_rows as _,
        })
    }
}

impl TryFrom<pb::GetLogLocationResponse> for Vec<LogLocation> {
    type Error = LogMetadataError;

    fn try_from(response: pb::GetLogLocationResponse) -> Result<Self, Self::Error> {
        use pb::log_location::Location as ProtoLocation;

        response
            .locations
            .into_iter()
            .map(|location| match location.location {
                None => Err(LogMetadataError::Internal {
                    message: "missing location in LogLocation proto".to_string(),
                }),
                Some(ProtoLocation::FolioLocation(folio)) => {
                    let inner = folio.try_into()?;
                    Ok(LogLocation::Folio(inner))
                }
            })
            .collect::<Result<Vec<_>, _>>()
    }
}

impl From<Vec<LogLocation>> for pb::GetLogLocationResponse {
    fn from(locations: Vec<LogLocation>) -> Self {
        use pb::log_location::Location;

        let locations = locations
            .into_iter()
            .map(|location| {
                let inner = match location {
                    LogLocation::Folio(folio) => Location::FolioLocation(folio.into()),
                };

                pb::LogLocation {
                    location: Some(inner),
                }
            })
            .collect::<Vec<_>>();

        pb::GetLogLocationResponse { locations }
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

/*
 *  ███████████   █████████    █████████  █████   ████
 * ░█░░░███░░░█  ███░░░░░███  ███░░░░░███░░███   ███░
 * ░   ░███  ░  ░███    ░███ ░███    ░░░  ░███  ███
 *     ░███     ░███████████ ░░█████████  ░███████
 *     ░███     ░███░░░░░███  ░░░░░░░░███ ░███░░███
 *     ░███     ░███    ░███  ███    ░███ ░███ ░░███
 *     █████    █████   █████░░█████████  █████ ░░████
 */

impl TryFrom<pb::TaskStatus> for TaskStatus {
    type Error = LogMetadataError;

    fn try_from(status: pb::TaskStatus) -> Result<Self, Self::Error> {
        match status {
            pb::TaskStatus::Unspecified => Err(LogMetadataError::Internal {
                message: "unspecified task status received".to_string(),
            }),
            pb::TaskStatus::Pending => Ok(TaskStatus::Pending),
            pb::TaskStatus::InProgress => Ok(TaskStatus::InProgress),
            pb::TaskStatus::Completed => Ok(TaskStatus::Completed),
            pb::TaskStatus::Failed => Ok(TaskStatus::Failed),
        }
    }
}

impl From<TaskStatus> for pb::TaskStatus {
    fn from(status: TaskStatus) -> Self {
        match status {
            TaskStatus::Pending => pb::TaskStatus::Pending,
            TaskStatus::InProgress => pb::TaskStatus::InProgress,
            TaskStatus::Completed => pb::TaskStatus::Completed,
            TaskStatus::Failed => pb::TaskStatus::Failed,
        }
    }
}

impl From<CompactionTask> for pb::CompactionTask {
    fn from(task: CompactionTask) -> Self {
        pb::CompactionTask {
            topic: task.topic_name.to_string(),
            partition: task.partition_value.as_ref().map(Into::into),
            start_offset: task.start_offset,
            end_offset: task.end_offset,
            target_file_size: task.target_file_size.as_u64(),
            operation: match task.operation {
                CompactionOperation::Append => pb::CompactionOperation::Append.into(),
                CompactionOperation::Replace => pb::CompactionOperation::Replace.into(),
            },
        }
    }
}

impl TryFrom<pb::CompactionTask> for CompactionTask {
    type Error = LogMetadataError;

    fn try_from(task: pb::CompactionTask) -> Result<Self, Self::Error> {
        let topic_name = TopicName::parse(&task.topic)
            .context(InvalidResourceNameSnafu { resource: "topic" })?;

        let partition_value = task.partition.clone().map(TryFrom::try_from).transpose()?;

        let operation = match task.operation() {
            pb::CompactionOperation::Unspecified => {
                return Err(LogMetadataError::InvalidArgument {
                    message: "CompactionOperation must be specified".to_string(),
                });
            }
            pb::CompactionOperation::Append => CompactionOperation::Append,
            pb::CompactionOperation::Replace => CompactionOperation::Replace,
        };

        Ok(CompactionTask {
            topic_name,
            partition_value,
            start_offset: task.start_offset,
            end_offset: task.end_offset,
            target_file_size: ByteSize::b(task.target_file_size),
            operation,
        })
    }
}

impl From<CreateTableTask> for pb::CreateTableTask {
    fn from(task: CreateTableTask) -> Self {
        pb::CreateTableTask {
            topic: task.topic_name.to_string(),
        }
    }
}

impl TryFrom<pb::CreateTableTask> for CreateTableTask {
    type Error = LogMetadataError;

    fn try_from(task: pb::CreateTableTask) -> Result<Self, Self::Error> {
        let topic_name = TopicName::parse(&task.topic)
            .context(InvalidResourceNameSnafu { resource: "topic" })?;

        Ok(CreateTableTask { topic_name })
    }
}

impl From<CommitTask> for pb::CommitTask {
    fn from(task: CommitTask) -> Self {
        let new_files = task
            .new_files
            .into_iter()
            .map(Into::into)
            .collect::<Vec<_>>();

        pb::CommitTask {
            topic: task.topic_name.to_string(),
            new_files,
        }
    }
}

impl TryFrom<pb::CommitTask> for CommitTask {
    type Error = LogMetadataError;

    fn try_from(task: pb::CommitTask) -> Result<Self, Self::Error> {
        let topic_name = TopicName::parse(&task.topic)
            .context(InvalidResourceNameSnafu { resource: "topic" })?;

        let new_files = task
            .new_files
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(CommitTask {
            topic_name,
            new_files,
        })
    }
}

impl TryFrom<pb::Task> for Task {
    type Error = LogMetadataError;

    fn try_from(task: pb::Task) -> Result<Self, Self::Error> {
        let metadata = {
            let task_id = task.task_id.clone();
            let status = task.status().try_into()?;

            let created_at = task
                .created_at
                .ok_or_else(|| LogMetadataError::Internal {
                    message: "missing created_at in Task proto".to_string(),
                })?
                .try_into()
                .map_err(Arc::new)
                .context(InvalidTimestampSnafu {})?;

            let updated_at = task
                .updated_at
                .ok_or_else(|| LogMetadataError::Internal {
                    message: "missing updated_at in Task proto".to_string(),
                })?
                .try_into()
                .map_err(Arc::new)
                .context(InvalidTimestampSnafu {})?;

            TaskMetadata {
                task_id,
                status,
                created_at,
                updated_at,
            }
        };

        let inner_task = task.task.ok_or_else(|| LogMetadataError::Internal {
            message: "missing task in Task proto".to_string(),
        })?;

        match inner_task {
            pb::task::Task::Compaction(compaction) => {
                let task = compaction.try_into()?;
                Ok(Task::Compaction { metadata, task })
            }
            pb::task::Task::CreateTable(create_table) => {
                let task = create_table.try_into()?;
                Ok(Task::CreateTable { metadata, task })
            }
            pb::task::Task::Commit(commit) => {
                let task = commit.try_into()?;
                Ok(Task::Commit { metadata, task })
            }
        }
    }
}

impl From<Task> for pb::Task {
    fn from(task: Task) -> Self {
        let meta = match task {
            Task::Compaction { ref metadata, .. } => metadata,
            Task::CreateTable { ref metadata, .. } => metadata,
            Task::Commit { ref metadata, .. } => metadata,
        };

        let status: pb::TaskStatus = meta.status.into();

        let task = match task {
            Task::Compaction { ref task, .. } => pb::task::Task::Compaction(task.clone().into()),
            Task::CreateTable { ref task, .. } => pb::task::Task::CreateTable(task.clone().into()),
            Task::Commit { ref task, .. } => pb::task::Task::Commit(task.clone().into()),
        };

        pb::Task {
            task_id: meta.task_id.clone(),
            status: status as i32,
            created_at: Some(meta.created_at.into()),
            updated_at: Some(meta.updated_at.into()),
            task: Some(task),
        }
    }
}

impl TryFrom<pb::RequestTaskRequest> for RequestTaskRequest {
    type Error = LogMetadataError;

    fn try_from(_request: pb::RequestTaskRequest) -> Result<Self, Self::Error> {
        Ok(RequestTaskRequest {})
    }
}

impl From<RequestTaskRequest> for pb::RequestTaskRequest {
    fn from(_request: RequestTaskRequest) -> Self {
        pb::RequestTaskRequest {}
    }
}

impl TryFrom<pb::RequestTaskResponse> for RequestTaskResponse {
    type Error = LogMetadataError;

    fn try_from(response: pb::RequestTaskResponse) -> Result<Self, Self::Error> {
        let task = response.task.map(TryFrom::try_from).transpose()?;
        Ok(RequestTaskResponse { task })
    }
}

impl From<RequestTaskResponse> for pb::RequestTaskResponse {
    fn from(response: RequestTaskResponse) -> Self {
        pb::RequestTaskResponse {
            task: response.task.map(Into::into),
        }
    }
}

impl From<FileMetadata> for pb::FileMetadata {
    fn from(meta: FileMetadata) -> Self {
        use crate::schema::pb::Datum as ProtoDatum;

        let lower_bounds = meta
            .lower_bounds
            .into_iter()
            .map(|(k, v)| (k, ProtoDatum::from(&v)))
            .collect();
        let upper_bounds = meta
            .upper_bounds
            .into_iter()
            .map(|(k, v)| (k, ProtoDatum::from(&v)))
            .collect();

        pb::FileMetadata {
            file_size_bytes: meta.file_size.as_u64(),
            num_rows: meta.num_rows as _,
            column_sizes: meta.column_sizes,
            value_counts: meta.value_counts,
            null_value_counts: meta.null_value_counts,
            lower_bounds,
            upper_bounds,
        }
    }
}

impl From<FileInfo> for pb::FileInfo {
    fn from(info: FileInfo) -> Self {
        pb::FileInfo {
            file_ref: info.file_ref,
            start_offset: info.start_offset,
            end_offset: info.end_offset,
            metadata: Some(info.metadata.into()),
        }
    }
}

impl TryFrom<pb::FileMetadata> for FileMetadata {
    type Error = LogMetadataError;

    fn try_from(meta: pb::FileMetadata) -> Result<Self, Self::Error> {
        let lower_bounds = {
            let mut m = HashMap::<u64, Datum>::with_capacity(meta.lower_bounds.len());
            for (k, v) in meta.lower_bounds.into_iter() {
                let v = Datum::try_from(&v).map_err(|err| LogMetadataError::Internal {
                    message: format!("invalid datum in lower bounds: {err}"),
                })?;
                m.insert(k, v);
            }
            m
        };

        let upper_bounds = {
            let mut m = HashMap::<u64, Datum>::with_capacity(meta.upper_bounds.len());
            for (k, v) in meta.upper_bounds.into_iter() {
                let v = Datum::try_from(&v).map_err(|err| LogMetadataError::Internal {
                    message: format!("invalid datum in upper bounds: {err}"),
                })?;
                m.insert(k, v);
            }
            m
        };

        Ok(FileMetadata {
            file_size: ByteSize::b(meta.file_size_bytes),
            num_rows: meta.num_rows as _,
            column_sizes: meta.column_sizes,
            value_counts: meta.value_counts,
            null_value_counts: meta.null_value_counts,
            lower_bounds,
            upper_bounds,
        })
    }
}

impl TryFrom<pb::FileInfo> for FileInfo {
    type Error = LogMetadataError;

    fn try_from(info: pb::FileInfo) -> Result<Self, Self::Error> {
        let metadata = info
            .metadata
            .ok_or_else(|| LogMetadataError::Internal {
                message: "missing result in CompleteTaskRequest proto".to_string(),
            })?
            .try_into()?;

        Ok(FileInfo {
            file_ref: info.file_ref,
            start_offset: info.start_offset,
            end_offset: info.end_offset,
            metadata,
        })
    }
}

impl From<CompactionResult> for pb::CompactionResult {
    fn from(result: CompactionResult) -> Self {
        let new_files = result
            .new_files
            .into_iter()
            .map(Into::into)
            .collect::<Vec<_>>();

        let operation = match result.operation {
            CompactionOperation::Append => pb::CompactionOperation::Append.into(),
            CompactionOperation::Replace => pb::CompactionOperation::Replace.into(),
        };

        pb::CompactionResult {
            new_files,
            operation,
        }
    }
}

impl TryFrom<pb::CompactionResult> for CompactionResult {
    type Error = LogMetadataError;

    fn try_from(result: pb::CompactionResult) -> Result<Self, Self::Error> {
        let operation = match result.operation() {
            pb::CompactionOperation::Unspecified => {
                return Err(LogMetadataError::InvalidArgument {
                    message: "CompactionOperation must be specified".to_string(),
                });
            }
            pb::CompactionOperation::Append => CompactionOperation::Append,
            pb::CompactionOperation::Replace => CompactionOperation::Replace,
        };

        let new_files = result
            .new_files
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(CompactionResult {
            new_files,
            operation,
        })
    }
}

impl From<CreateTableResult> for pb::CreateTableResult {
    fn from(result: CreateTableResult) -> Self {
        pb::CreateTableResult {
            table_id: result.table_id,
        }
    }
}

impl TryFrom<pb::CreateTableResult> for CreateTableResult {
    type Error = LogMetadataError;

    fn try_from(result: pb::CreateTableResult) -> Result<Self, Self::Error> {
        Ok(CreateTableResult {
            table_id: result.table_id,
        })
    }
}

impl From<CommitResult> for pb::CommitResult {
    fn from(result: CommitResult) -> Self {
        pb::CommitResult {
            table_version: result.table_version,
        }
    }
}

impl TryFrom<pb::CommitResult> for CommitResult {
    type Error = LogMetadataError;

    fn try_from(result: pb::CommitResult) -> Result<Self, Self::Error> {
        Ok(CommitResult {
            table_version: result.table_version,
        })
    }
}

impl From<TaskResult> for pb::TaskResult {
    fn from(result: TaskResult) -> Self {
        match result {
            TaskResult::Compaction(compaction) => pb::TaskResult {
                result: Some(pb::task_result::Result::Compaction(compaction.into())),
            },
            TaskResult::CreateTable(create_table) => pb::TaskResult {
                result: Some(pb::task_result::Result::CreateTable(create_table.into())),
            },
            TaskResult::Commit(commit) => pb::TaskResult {
                result: Some(pb::task_result::Result::Commit(commit.into())),
            },
        }
    }
}

impl TryFrom<pb::TaskResult> for TaskResult {
    type Error = LogMetadataError;

    fn try_from(result: pb::TaskResult) -> Result<Self, Self::Error> {
        match result.result {
            Some(pb::task_result::Result::Compaction(compaction)) => {
                Ok(TaskResult::Compaction(compaction.try_into()?))
            }
            Some(pb::task_result::Result::CreateTable(create_table)) => {
                Ok(TaskResult::CreateTable(create_table.try_into()?))
            }
            Some(pb::task_result::Result::Commit(commit)) => {
                Ok(TaskResult::Commit(commit.try_into()?))
            }
            None => Err(LogMetadataError::Internal {
                message: "missing result in TaskResult proto".to_string(),
            }),
        }
    }
}

impl TryFrom<pb::CompleteTaskRequest> for CompleteTaskRequest {
    type Error = LogMetadataError;

    fn try_from(request: pb::CompleteTaskRequest) -> Result<Self, Self::Error> {
        let result = match request.result {
            Some(pb::complete_task_request::Result::Success(task_result)) => {
                TaskCompletionResult::Success(task_result.try_into()?)
            }
            Some(pb::complete_task_request::Result::Failure(error_message)) => {
                TaskCompletionResult::Failure(error_message)
            }
            None => {
                return Err(LogMetadataError::Internal {
                    message: "missing result in CompleteTaskRequest proto".to_string(),
                });
            }
        };

        Ok(CompleteTaskRequest {
            task_id: request.task_id,
            result,
        })
    }
}

impl From<CompleteTaskRequest> for pb::CompleteTaskRequest {
    fn from(request: CompleteTaskRequest) -> Self {
        let result = match request.result {
            TaskCompletionResult::Success(task_result) => {
                pb::complete_task_request::Result::Success(task_result.into())
            }
            TaskCompletionResult::Failure(error_message) => {
                pb::complete_task_request::Result::Failure(error_message)
            }
        };

        pb::CompleteTaskRequest {
            task_id: request.task_id,
            result: Some(result),
        }
    }
}

impl TryFrom<pb::CompleteTaskResponse> for CompleteTaskResponse {
    type Error = LogMetadataError;

    fn try_from(response: pb::CompleteTaskResponse) -> Result<Self, Self::Error> {
        Ok(CompleteTaskResponse {
            success: response.success,
        })
    }
}

impl From<CompleteTaskResponse> for pb::CompleteTaskResponse {
    fn from(response: CompleteTaskResponse) -> Self {
        pb::CompleteTaskResponse {
            success: response.success,
        }
    }
}
