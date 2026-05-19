//! Conversions between log metadata domain types and protobuf types.

use std::collections::HashMap;

use bytesize::ByteSize;
use snafu::{ResultExt, ensure};
use wings_resources::{PartitionValue, TableName};
use wings_schema::Datum;

use crate::{
    table_metadata::{
        AcceptedBatchInfo, CommitBatchRequest, CommitResult, CommitTask, CommittedBatch,
        CompactionOperation, CompactionResult, CompactionTask, CompleteTaskRequest,
        CompleteTaskResponse, CreateTableResult, CreateTableTask, DataLakeLocation, FileInfo,
        FileMetadata, FolioLocation, GetTableLocationOptions, GetTableLocationRequest,
        ListPartitionsRequest, ListPartitionsResponse, TableLocation, SeqNum, PartitionMetadata,
        RejectedBatchInfo, RequestTaskRequest, RequestTaskResponse, Task, TaskCompletionResult,
        TaskMetadata, TaskResult, TaskStatus,
    },
    pb::{
        self,
        error::{ResourceSnafu, Result, UnspecifiedSnafu, ValueOutOfRangeSnafu, WireError},
        schema::FromOptionalField,
    },
};

impl TryFrom<pb::SeqNum> for SeqNum {
    type Error = WireError;

    fn try_from(seqnum: pb::SeqNum) -> Result<Self, Self::Error> {
        let timestamp = seqnum.timestamp.unwrap_or_default().try_into()?;

        Ok(Self {
            seqnum: seqnum.seqnum,
            timestamp,
        })
    }
}

impl From<SeqNum> for pb::SeqNum {
    fn from(seqnum: SeqNum) -> Self {
        Self {
            seqnum: seqnum.seqnum,
            timestamp: Some(seqnum.timestamp.into()),
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

impl TryFrom<pb::CommitBatchRequest> for CommitBatchRequest {
    type Error = WireError;

    fn try_from(meta: pb::CommitBatchRequest) -> Result<Self, Self::Error> {
        let table_name =
            TableName::parse(&meta.table).context(ResourceSnafu { resource: "table" })?;
        let partition_value = meta.partition.map(TryFrom::try_from).transpose()?;

        let Some(timestamp) = meta.timestamp else {
            return Ok(CommitBatchRequest {
                batch_id: meta.batch_id,
                table_name,
                partition_value,
                file_ref: meta.file_ref,
                page_offset_bytes: meta.page_offset_bytes,
                page_size_bytes: meta.page_size_bytes,
                timestamp: None,
                num_rows: meta.num_rows,
            });
        };

        assert!(timestamp.seconds >= 0);
        assert!(timestamp.nanos >= 0);

        let timestamp = timestamp.try_into()?;

        Ok(CommitBatchRequest {
            batch_id: meta.batch_id,
            table_name,
            partition_value,
            file_ref: meta.file_ref,
            page_offset_bytes: meta.page_offset_bytes,
            page_size_bytes: meta.page_size_bytes,
            timestamp: Some(timestamp),
            num_rows: meta.num_rows,
        })
    }
}

impl From<&CommitBatchRequest> for pb::CommitBatchRequest {
    fn from(meta: &CommitBatchRequest) -> Self {
        let timestamp = meta.timestamp.map(Into::into);

        pb::CommitBatchRequest {
            batch_id: meta.batch_id,
            table: meta.table_name.to_string(),
            partition: meta.partition_value.as_ref().map(Into::into),
            timestamp,
            num_rows: meta.num_rows,
            file_ref: meta.file_ref.clone(),
            page_offset_bytes: meta.page_offset_bytes,
            page_size_bytes: meta.page_size_bytes,
        }
    }
}

impl TryFrom<pb::CommitResponse> for Vec<CommittedBatch> {
    type Error = WireError;

    fn try_from(response: pb::CommitResponse) -> Result<Self, Self::Error> {
        response
            .batches
            .into_iter()
            .map(TryFrom::try_from)
            .collect()
    }
}

impl TryFrom<pb::CommittedBatch> for CommittedBatch {
    type Error = WireError;

    fn try_from(meta: pb::CommittedBatch) -> Result<Self> {
        use pb::committed_batch::*;

        let result = meta.result.required("inner")?;

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
            CommittedBatch::Rejected(info) => pb::CommittedBatch {
                result: Some(Result::Rejected(info.into())),
            },
        }
    }
}

impl TryFrom<pb::committed_batch::Accepted> for AcceptedBatchInfo {
    type Error = WireError;

    fn try_from(info: pb::committed_batch::Accepted) -> Result<Self, Self::Error> {
        let timestamp = info.timestamp.required("timestamp")?.try_into()?;

        Ok(AcceptedBatchInfo {
            batch_id: info.batch_id,
            start_seqnum: info.start_seqnum,
            end_seqnum: info.end_seqnum,
            timestamp,
        })
    }
}

impl From<AcceptedBatchInfo> for pb::committed_batch::Accepted {
    fn from(info: AcceptedBatchInfo) -> Self {
        pb::committed_batch::Accepted {
            batch_id: info.batch_id,
            start_seqnum: info.start_seqnum,
            end_seqnum: info.end_seqnum,
            timestamp: Some(info.timestamp.into()),
        }
    }
}

impl From<pb::committed_batch::Rejected> for RejectedBatchInfo {
    fn from(info: pb::committed_batch::Rejected) -> Self {
        RejectedBatchInfo {
            batch_id: info.batch_id,
            num_rows: info.num_rows,
            reason: info.reason,
        }
    }
}

impl From<RejectedBatchInfo> for pb::committed_batch::Rejected {
    fn from(info: RejectedBatchInfo) -> Self {
        pb::committed_batch::Rejected {
            batch_id: info.batch_id,
            num_rows: info.num_rows,
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

impl TryFrom<pb::GetTableLocationRequest> for GetTableLocationRequest {
    type Error = WireError;

    fn try_from(request: pb::GetTableLocationRequest) -> Result<Self, Self::Error> {
        let table_name =
            TableName::parse(&request.table).context(ResourceSnafu { resource: "table" })?;

        let partition_value = request.partition.map(TryFrom::try_from).transpose()?;

        let options = request.options.required("options")?.try_into()?;

        Ok(GetTableLocationRequest {
            table_name,
            partition_value,
            options,
            seqnum: request.seqnum,
        })
    }
}

impl TryFrom<pb::GetTableLocationOptions> for GetTableLocationOptions {
    type Error = WireError;

    fn try_from(options: pb::GetTableLocationOptions) -> Result<Self, Self::Error> {
        let deadline = options.deadline.required("deadline")?.try_into()?;

        Ok(GetTableLocationOptions {
            deadline,
            min_rows: options.min_rows as _,
            max_rows: options.max_rows as _,
        })
    }
}

impl TryFrom<GetTableLocationRequest> for pb::GetTableLocationRequest {
    type Error = WireError;

    fn try_from(request: GetTableLocationRequest) -> Result<Self, Self::Error> {
        let options = request.options.try_into()?;
        Ok(pb::GetTableLocationRequest {
            table: request.table_name.to_string(),
            partition: request.partition_value.as_ref().map(Into::into),
            seqnum: request.seqnum,
            options: Some(options),
        })
    }
}

impl TryFrom<GetTableLocationOptions> for pb::GetTableLocationOptions {
    type Error = WireError;

    fn try_from(options: GetTableLocationOptions) -> Result<Self, Self::Error> {
        let deadline = options.deadline.try_into()?;

        Ok(Self {
            deadline: Some(deadline),
            min_rows: options.min_rows as _,
            max_rows: options.max_rows as _,
        })
    }
}

impl TryFrom<pb::GetTableLocationResponse> for Vec<TableLocation> {
    type Error = WireError;

    fn try_from(response: pb::GetTableLocationResponse) -> Result<Self, Self::Error> {
        use pb::table_location::Location as ProtoLocation;

        response
            .locations
            .into_iter()
            .map(|location| match location.location {
                None => Err(WireError::MissingField {
                    field_name: "location".to_string(),
                }),
                Some(ProtoLocation::FolioLocation(folio)) => {
                    let inner = folio.try_into()?;
                    Ok(TableLocation::Folio(inner))
                }
                Some(ProtoLocation::DataLakeLocation(lake)) => {
                    let inner = lake.try_into()?;
                    Ok(TableLocation::DataLake(inner))
                }
            })
            .collect::<Result<Vec<_>, _>>()
    }
}

impl From<Vec<TableLocation>> for pb::GetTableLocationResponse {
    fn from(locations: Vec<TableLocation>) -> Self {
        use pb::table_location::Location;

        let locations = locations
            .into_iter()
            .map(|location| {
                let inner = match location {
                    TableLocation::Folio(folio) => Location::FolioLocation(folio.into()),
                    TableLocation::DataLake(lake) => Location::DataLakeLocation(lake.into()),
                };

                pb::TableLocation {
                    location: Some(inner),
                }
            })
            .collect::<Vec<_>>();

        pb::GetTableLocationResponse { locations }
    }
}

impl TryFrom<pb::FolioLocation> for FolioLocation {
    type Error = WireError;

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
            num_rows: location.num_rows as _,
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
            num_rows: location.num_rows as _,
            batches: location.batches.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<pb::DataLakeLocation> for DataLakeLocation {
    type Error = WireError;

    fn try_from(location: pb::DataLakeLocation) -> Result<Self, Self::Error> {
        let start_seqnum = location.start_seqnum.required("start_seqnum")?.try_into()?;
        let end_seqnum = location.end_seqnum.required("end_seqnum")?.try_into()?;

        Ok(Self {
            file_ref: location.file_ref,
            size_bytes: location.size_bytes,
            num_rows: location.num_rows as _,
            start_seqnum,
            end_seqnum,
        })
    }
}

impl From<DataLakeLocation> for pb::DataLakeLocation {
    fn from(location: DataLakeLocation) -> Self {
        pb::DataLakeLocation {
            file_ref: location.file_ref,
            size_bytes: location.size_bytes,
            num_rows: location.num_rows as _,
            start_seqnum: Some(location.start_seqnum.into()),
            end_seqnum: Some(location.end_seqnum.into()),
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
    type Error = WireError;

    fn try_from(request: pb::ListPartitionsRequest) -> Result<Self, Self::Error> {
        let table_name =
            TableName::parse(&request.table).context(ResourceSnafu { resource: "table" })?;

        Ok(ListPartitionsRequest {
            table_name,
            page_size: request.page_size.map(|size| size as usize),
            page_token: request.page_token,
        })
    }
}

impl From<ListPartitionsRequest> for pb::ListPartitionsRequest {
    fn from(request: ListPartitionsRequest) -> Self {
        pb::ListPartitionsRequest {
            table: request.table_name.to_string(),
            page_size: request.page_size.map(|v| v as i32),
            page_token: request.page_token,
        }
    }
}

impl TryFrom<pb::ListPartitionsResponse> for ListPartitionsResponse {
    type Error = WireError;

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
    type Error = WireError;

    fn try_from(metadata: pb::PartitionMetadata) -> Result<Self, Self::Error> {
        let partition_value = metadata.value.map(TryInto::try_into).transpose()?;
        let end_seqnum = metadata.end_seqnum.required("end_seqnum")?.try_into()?;

        Ok(PartitionMetadata {
            partition_value,
            end_seqnum,
        })
    }
}

impl From<PartitionMetadata> for pb::PartitionMetadata {
    fn from(metadata: PartitionMetadata) -> Self {
        pb::PartitionMetadata {
            value: metadata.partition_value.as_ref().map(Into::into),
            end_seqnum: Some(metadata.end_seqnum.into()),
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
    type Error = WireError;

    fn try_from(value: pb::PartitionValue) -> Result<Self, Self::Error> {
        use pb::partition_value::Value;

        match value.value.required("value")? {
            Value::NullValue(_) => Ok(PartitionValue::Null),
            Value::Int8Value(v) => {
                ensure!(
                    v >= i8::MIN as i32 && v <= i8::MAX as i32,
                    ValueOutOfRangeSnafu {
                        r#type: "Int8",
                        value: v.to_string(),
                    }
                );

                Ok(PartitionValue::Int8(v as i8))
            }
            Value::Int16Value(v) => {
                ensure!(
                    v >= i16::MIN as i32 && v <= i16::MAX as i32,
                    ValueOutOfRangeSnafu {
                        r#type: "Int16",
                        value: v.to_string(),
                    }
                );

                Ok(PartitionValue::Int16(v as i16))
            }
            Value::Int32Value(v) => Ok(PartitionValue::Int32(v)),
            Value::Int64Value(v) => Ok(PartitionValue::Int64(v)),
            Value::Uint8Value(v) => {
                ensure!(
                    v <= u8::MAX as u32,
                    ValueOutOfRangeSnafu {
                        r#type: "UInt8",
                        value: v.to_string(),
                    }
                );

                Ok(PartitionValue::UInt8(v as u8))
            }
            Value::Uint16Value(v) => {
                ensure!(
                    v <= u16::MAX as u32,
                    ValueOutOfRangeSnafu {
                        r#type: "UInt16",
                        value: v.to_string(),
                    }
                );

                Ok(PartitionValue::UInt16(v as u16))
            }
            Value::Uint32Value(v) => Ok(PartitionValue::UInt32(v)),
            Value::Uint64Value(v) => Ok(PartitionValue::UInt64(v)),
            Value::StringValue(v) => Ok(PartitionValue::String(v)),
            Value::BytesValue(v) => Ok(PartitionValue::Bytes(v)),
            Value::BoolValue(v) => Ok(PartitionValue::Boolean(v)),
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
    type Error = WireError;

    fn try_from(status: pb::TaskStatus) -> Result<Self, Self::Error> {
        match status {
            pb::TaskStatus::Unspecified => UnspecifiedSnafu {
                r#enum: "TaskStatus",
            }
            .fail(),
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
            table: task.table_name.to_string(),
            partition: task.partition_value.as_ref().map(Into::into),
            start_seqnum: task.start_seqnum,
            end_seqnum: task.end_seqnum,
            target_file_size: task.target_file_size.as_u64(),
            operation: match task.operation {
                CompactionOperation::Append => pb::CompactionOperation::Append.into(),
                CompactionOperation::Replace => pb::CompactionOperation::Replace.into(),
            },
        }
    }
}

impl TryFrom<pb::CompactionTask> for CompactionTask {
    type Error = WireError;

    fn try_from(task: pb::CompactionTask) -> Result<Self, Self::Error> {
        let table_name =
            TableName::parse(&task.table).context(ResourceSnafu { resource: "table" })?;

        let partition_value = task.partition.clone().map(TryFrom::try_from).transpose()?;

        let operation = match task.operation() {
            pb::CompactionOperation::Unspecified => {
                return UnspecifiedSnafu {
                    r#enum: "CompactionOperation",
                }
                .fail();
            }
            pb::CompactionOperation::Append => CompactionOperation::Append,
            pb::CompactionOperation::Replace => CompactionOperation::Replace,
        };

        Ok(CompactionTask {
            table_name,
            partition_value,
            start_seqnum: task.start_seqnum,
            end_seqnum: task.end_seqnum,
            target_file_size: ByteSize::b(task.target_file_size),
            operation,
        })
    }
}

impl From<CreateTableTask> for pb::CreateTableTask {
    fn from(task: CreateTableTask) -> Self {
        pb::CreateTableTask {
            table: task.table_name.to_string(),
        }
    }
}

impl TryFrom<pb::CreateTableTask> for CreateTableTask {
    type Error = WireError;

    fn try_from(task: pb::CreateTableTask) -> Result<Self, Self::Error> {
        let table_name =
            TableName::parse(&task.table).context(ResourceSnafu { resource: "table" })?;

        Ok(CreateTableTask { table_name })
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
            table: task.table_name.to_string(),
            new_files,
        }
    }
}

impl TryFrom<pb::CommitTask> for CommitTask {
    type Error = WireError;

    fn try_from(task: pb::CommitTask) -> Result<Self, Self::Error> {
        let table_name =
            TableName::parse(&task.table).context(ResourceSnafu { resource: "table" })?;

        let new_files = task
            .new_files
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(CommitTask {
            table_name,
            new_files,
        })
    }
}

impl TryFrom<pb::Task> for Task {
    type Error = WireError;

    fn try_from(task: pb::Task) -> Result<Self, Self::Error> {
        let metadata = {
            let task_id = task.task_id.clone();
            let status = task.status().try_into()?;

            let created_at = task.created_at.required("created_at")?.try_into()?;

            let updated_at = task.updated_at.required("updated_at")?.try_into()?;

            TaskMetadata {
                task_id,
                status,
                created_at,
                updated_at,
            }
        };

        match task.task.required("task")? {
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
    type Error = WireError;

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
    type Error = WireError;

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
        use crate::pb::Datum as ProtoDatum;

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
            partition_value: info.partition_value.as_ref().map(Into::into),
            start_seqnum: Some(info.start_seqnum.into()),
            end_seqnum: Some(info.end_seqnum.into()),
            metadata: Some(info.metadata.into()),
            modification_time: Some(info.modification_time.into()),
        }
    }
}

impl TryFrom<pb::FileMetadata> for FileMetadata {
    type Error = WireError;

    fn try_from(meta: pb::FileMetadata) -> Result<Self, Self::Error> {
        let lower_bounds = {
            let mut m = HashMap::<u64, Datum>::with_capacity(meta.lower_bounds.len());
            for (k, v) in meta.lower_bounds.into_iter() {
                let v = Datum::try_from(&v)?;
                m.insert(k, v);
            }
            m
        };

        let upper_bounds = {
            let mut m = HashMap::<u64, Datum>::with_capacity(meta.upper_bounds.len());
            for (k, v) in meta.upper_bounds.into_iter() {
                let v = Datum::try_from(&v)?;
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
    type Error = WireError;

    fn try_from(info: pb::FileInfo) -> Result<Self, Self::Error> {
        let partition_value = info
            .partition_value
            .clone()
            .map(TryFrom::try_from)
            .transpose()?;

        let start_seqnum = info.start_seqnum.required("start_seqnum")?.try_into()?;
        let end_seqnum = info.end_seqnum.required("end_seqnum")?.try_into()?;

        let metadata = info.metadata.required("metadata")?.try_into()?;
        let modification_time = info
            .modification_time
            .required("modification_time")?
            .try_into()?;

        Ok(FileInfo {
            file_ref: info.file_ref,
            partition_value,
            start_seqnum,
            end_seqnum,
            metadata,
            modification_time,
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
    type Error = WireError;

    fn try_from(result: pb::CompactionResult) -> Result<Self, Self::Error> {
        let operation = match result.operation() {
            pb::CompactionOperation::Unspecified => {
                return UnspecifiedSnafu {
                    r#enum: "CompactionOperation",
                }
                .fail();
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
    type Error = WireError;

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
    type Error = WireError;

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
    type Error = WireError;

    fn try_from(result: pb::TaskResult) -> Result<Self, Self::Error> {
        match result.result.required("result")? {
            pb::task_result::Result::Compaction(compaction) => {
                Ok(TaskResult::Compaction(compaction.try_into()?))
            }
            pb::task_result::Result::CreateTable(create_table) => {
                Ok(TaskResult::CreateTable(create_table.try_into()?))
            }
            pb::task_result::Result::Commit(commit) => Ok(TaskResult::Commit(commit.try_into()?)),
        }
    }
}

impl TryFrom<pb::CompleteTaskRequest> for CompleteTaskRequest {
    type Error = WireError;

    fn try_from(request: pb::CompleteTaskRequest) -> Result<Self, Self::Error> {
        let result = match request.result.required("result")? {
            pb::complete_task_request::Result::Success(task_result) => {
                TaskCompletionResult::Success(task_result.try_into()?)
            }
            pb::complete_task_request::Result::Failure(error_message) => {
                TaskCompletionResult::Failure(error_message)
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
    type Error = WireError;

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
