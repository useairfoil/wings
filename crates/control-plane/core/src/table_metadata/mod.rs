pub(crate) mod error;
pub mod stream;
pub mod timestamp;
pub mod tonic;
mod validation;

use std::{
    collections::HashMap,
    fmt,
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use bytesize::ByteSize;
use object_store::path::Path;
use prost::Message;
use serde::{Deserialize, Serialize};
use time::UtcDateTime;
use wings_resources::{NamespaceName, PartitionValue, TableName};
use wings_schema::Datum;

pub use self::{
    error::{TableMetadataError, Result},
    validation::validate_batches_to_commit,
};

#[derive(Default, Debug, Clone, PartialEq)]
pub struct FileMetadata {
    pub file_size: bytesize::ByteSize,
    pub num_rows: usize,
    pub column_sizes: HashMap<u64, u64>,
    pub value_counts: HashMap<u64, u64>,
    pub null_value_counts: HashMap<u64, u64>,
    pub lower_bounds: HashMap<u64, Datum>,
    pub upper_bounds: HashMap<u64, Datum>,
}

#[async_trait]
pub trait TableMetadata: Send + Sync {
    /// Commit several batches across tables and partitions.
    async fn commit(
        &self,
        namespace: NamespaceName,
        batches: Vec<CommitBatchRequest>,
    ) -> Result<Vec<CommittedBatch>>;

    /// Retrieves the locations of the logs for the specified table and partition.
    async fn get_table_location(&self, request: GetTableLocationRequest) -> Result<Vec<TableLocation>>;

    /// List the partitions of a table.
    async fn list_partitions(
        &self,
        request: ListPartitionsRequest,
    ) -> Result<ListPartitionsResponse>;

    /// Request a task to be assigned to a worker.
    async fn request_task(&self, request: RequestTaskRequest) -> Result<RequestTaskResponse>;

    /// Complete a task with the given status.
    async fn complete_task(&self, request: CompleteTaskRequest) -> Result<CompleteTaskResponse>;
}

/// The seqnum of a log together with its timestamp.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct SeqNum {
    /// The seqnum of the log.
    pub seqnum: u64,
    /// The timestamp of the log.
    pub timestamp: SystemTime,
}

/// Information about a batch that was committed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommittedBatch {
    /// The batch was rejected.
    Rejected(RejectedBatchInfo),
    /// The batch was accepted and belongs to the table.
    Accepted(AcceptedBatchInfo),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RejectedBatchInfo {
    /// The request id used to correlate the request with the response.
    pub batch_id: u32,
    /// The number of rows in the batch.
    pub num_rows: u32,
    /// The reason for rejection.
    pub reason: String,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcceptedBatchInfo {
    /// The request id used to correlate the request with the response.
    pub batch_id: u32,
    /// The seqnum of the first row in the batch.
    pub start_seqnum: u64,
    /// The seqnum of the last row in the batch.
    pub end_seqnum: u64,
    /// The timestamp of the batch.
    pub timestamp: SystemTime,
}

/// Represents a single write operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitBatchRequest {
    /// The request id used to correlate the request with the response.
    pub batch_id: u32,
    /// The table id of the batch to commit.
    pub table_name: TableName,
    /// The partition value, if any.
    pub partition_value: Option<PartitionValue>,
    /// The folio file containing the batch.
    pub file_ref: String,
    /// The start seqnum of the batch's page in the folio file.
    pub page_offset_bytes: u64,
    /// The batch size, in bytes.
    pub page_size_bytes: u64,
    /// The requested timestamp for the write request.
    pub timestamp: Option<SystemTime>,
    /// The number of rows in the write request.
    pub num_rows: u32,
}

/// Request to retrieve the location of a log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetTableLocationRequest {
    /// The table name of the log.
    pub table_name: TableName,
    /// The partition value, if any.
    pub partition_value: Option<PartitionValue>,
    /// The seqnum requested.
    pub seqnum: u64,
    /// The options for the request.
    pub options: GetTableLocationOptions,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetTableLocationOptions {
    /// The deadline for the request.
    pub deadline: Duration,
    /// The minimum number of rows to retrieve.
    pub min_rows: usize,
    /// The (soft) maximum number of rows to retrieve.
    pub max_rows: usize,
}

/// Location of a specific log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableLocation {
    /// The Parquet file is inside a folio.
    Folio(FolioLocation),
    /// The Parquet file is in the data lake.
    DataLake(DataLakeLocation),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FolioLocation {
    /// Folio file name.
    pub file_ref: String,
    /// Offset within the folio file.
    pub offset_bytes: u64,
    /// Size of the partition data in the folio file.
    pub size_bytes: u64,
    /// The number of rows in the parquet file.
    pub num_rows: usize,
    /// The batches that were committed.
    pub batches: Vec<CommittedBatch>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataLakeLocation {
    /// Path to the parquet file.
    pub file_ref: String,
    /// Size of the parquet file.
    pub size_bytes: u64,
    /// The number of rows in the parquet file.
    pub num_rows: usize,
    /// The seqnum of the first row in the file.
    pub start_seqnum: SeqNum,
    /// The seqnum of the last row in the file.
    pub end_seqnum: SeqNum,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListPartitionsRequest {
    /// The table name of the log.
    pub table_name: TableName,
    /// The page size for the request.
    pub page_size: Option<usize>,
    /// The continuation token for the request.
    pub page_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListPartitionsResponse {
    /// The partitions of the table.
    pub partitions: Vec<PartitionMetadata>,
    /// The continuation token for the next page.
    pub next_page_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionMetadata {
    /// The partition value.
    pub partition_value: Option<PartitionValue>,
    /// The end seqnum of the log.
    pub end_seqnum: SeqNum,
}

/// The status of a task.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TaskStatus {
    /// Task is pending to be assigned.
    Pending,
    /// Task is currently being processed.
    InProgress,
    /// Task has been completed successfully.
    Completed,
    /// Task has failed.
    Failed,
}

/// A task that can be assigned to a worker.
#[derive(Debug, Clone, PartialEq)]
pub struct TaskMetadata {
    /// The unique identifier of the task.
    pub task_id: String,
    /// The current status of the task.
    pub status: TaskStatus,
    /// The time when the task was created.
    pub created_at: SystemTime,
    /// The time when the task was last updated.
    pub updated_at: SystemTime,
}

/// The operation type for a compaction task.
#[derive(Debug, Clone, PartialEq)]
pub enum CompactionOperation {
    /// Append new data to existing segments.
    Append,
    /// Replace existing segments with new data.
    Replace,
}

/// A task to create a table.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableTask {
    /// The table name for the table to create.
    pub table_name: TableName,
}

/// A compaction task.
#[derive(Debug, Clone, PartialEq)]
pub struct CompactionTask {
    /// The table name to compact.
    pub table_name: TableName,
    /// The partition value to compact, if any.
    pub partition_value: Option<PartitionValue>,
    /// The start seqnum of the compaction range.
    pub start_seqnum: u64,
    /// The end seqnum of the compaction range.
    pub end_seqnum: u64,
    /// The operation type for this compaction.
    pub operation: CompactionOperation,
    /// The target file size for the parquet writer.
    pub target_file_size: ByteSize,
}

/// A task to create a table.
#[derive(Debug, Clone, PartialEq)]
pub struct CommitTask {
    /// The table name for the table to create.
    pub table_name: TableName,
    /// Files to be committed.
    pub new_files: Vec<FileInfo>,
}

/// Result for a create table task.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableResult {
    /// The ID of the created table
    pub table_id: String,
}

/// Information about a file created by compaction.
#[derive(Debug, Clone, PartialEq)]
pub struct FileInfo {
    /// File reference (path) - consistent with PageInfo
    pub file_ref: String,
    /// The partition value for the file
    pub partition_value: Option<PartitionValue>,
    /// First seqnum (inclusive) in this file
    pub start_seqnum: SeqNum,
    /// Last seqnum (inclusive) in this file
    pub end_seqnum: SeqNum,
    /// Parquet file metadata
    pub metadata: FileMetadata,
    /// Timestamp when the file was created
    pub modification_time: SystemTime,
}

/// Result for a compaction task.
#[derive(Debug, Clone, PartialEq)]
pub struct CompactionResult {
    /// Files created by compaction
    pub new_files: Vec<FileInfo>,
    /// The operation performed
    pub operation: CompactionOperation,
}

/// Result for a create table task.
#[derive(Debug, Clone, PartialEq)]
pub struct CommitResult {
    /// The version of the table after the commit
    pub table_version: String,
}

/// Result for different task types.
#[derive(Debug, Clone, PartialEq)]
pub enum TaskResult {
    Compaction(CompactionResult),
    CreateTable(CreateTableResult),
    Commit(CommitResult),
}

/// A task that can be assigned to a worker.
#[derive(Debug, Clone, PartialEq)]
pub enum Task {
    Compaction {
        metadata: TaskMetadata,
        task: CompactionTask,
    },
    CreateTable {
        metadata: TaskMetadata,
        task: CreateTableTask,
    },
    Commit {
        metadata: TaskMetadata,
        task: CommitTask,
    },
}

/// Request to assign a task to a worker.
#[derive(Default, Debug, Clone, PartialEq)]
pub struct RequestTaskRequest {}

/// Response containing the assigned task.
#[derive(Debug, Clone, PartialEq)]
pub struct RequestTaskResponse {
    /// The assigned task, if any.
    pub task: Option<Task>,
}

/// Result of task completion.
#[derive(Debug, Clone, PartialEq)]
pub enum TaskCompletionResult {
    Success(TaskResult),
    Failure(String),
}

/// Request to complete a task.
#[derive(Debug, Clone, PartialEq)]
pub struct CompleteTaskRequest {
    /// The identifier of the task to complete.
    pub task_id: String,
    /// The result of task completion.
    pub result: TaskCompletionResult,
}

/// Response indicating whether the task completion was successful.
#[derive(Debug, Clone, PartialEq)]
pub struct CompleteTaskResponse {
    /// Whether the task completion was successful.
    pub success: bool,
}

impl SeqNum {
    /// Creates a new `SeqNum` with the given seqnum.
    pub fn new(seqnum: u64) -> Self {
        Self {
            seqnum,
            ..Default::default()
        }
    }

    /// Creates a new `SeqNum` with the timestamp.
    pub fn with_timestamp(&self, timestamp: SystemTime) -> SeqNum {
        SeqNum {
            seqnum: self.seqnum,
            timestamp,
        }
    }

    pub fn previous(&self) -> SeqNum {
        SeqNum {
            seqnum: self.seqnum.saturating_sub(1),
            timestamp: self.timestamp,
        }
    }
}

impl CommitBatchRequest {
    pub fn new(num_rows: u32) -> Self {
        Self {
            batch_id: 0,
            table_name: TableName::new_unchecked(
                "test-table",
                NamespaceName::new_unchecked(
                    "test-namespace",
                    wings_resources::TenantName::new_unchecked("test-tenant"),
                ),
            ),
            partition_value: None,
            file_ref: "test-folio".to_string(),
            page_offset_bytes: 0,
            page_size_bytes: 0,
            num_rows,
            timestamp: None,
        }
    }

    pub fn new_with_timestamp(num_rows: u32, timestamp: SystemTime) -> Self {
        Self {
            batch_id: 0,
            table_name: TableName::new_unchecked(
                "test-table",
                NamespaceName::new_unchecked(
                    "test-namespace",
                    wings_resources::TenantName::new_unchecked("test-tenant"),
                ),
            ),
            partition_value: None,
            file_ref: "test-folio".to_string(),
            page_offset_bytes: 0,
            page_size_bytes: 0,
            num_rows,
            timestamp: Some(timestamp),
        }
    }
}

impl Default for SeqNum {
    fn default() -> Self {
        SeqNum {
            seqnum: 0,
            timestamp: SystemTime::UNIX_EPOCH,
        }
    }
}

impl TableLocation {
    pub fn start_seqnum(&self) -> Option<SeqNum> {
        match self {
            TableLocation::Folio(folio) => folio.start_seqnum(),
            TableLocation::DataLake(lake) => lake.start_seqnum.into(),
        }
    }

    pub fn end_seqnum(&self) -> Option<SeqNum> {
        match self {
            TableLocation::Folio(folio) => folio.end_seqnum(),
            TableLocation::DataLake(lake) => lake.end_seqnum.into(),
        }
    }

    pub fn num_rows(&self) -> usize {
        match self {
            TableLocation::Folio(folio) => folio.num_rows,
            TableLocation::DataLake(lake) => lake.num_rows,
        }
    }
}

impl FolioLocation {
    pub fn end_seqnum(&self) -> Option<SeqNum> {
        self.batches.iter().rev().find_map(|batch| match batch {
            CommittedBatch::Rejected(_) => None,
            CommittedBatch::Accepted(info) => Some(SeqNum {
                seqnum: info.end_seqnum,
                timestamp: info.timestamp,
            }),
        })
    }

    pub fn start_seqnum(&self) -> Option<SeqNum> {
        self.batches.iter().find_map(|batch| match batch {
            CommittedBatch::Rejected(_) => None,
            CommittedBatch::Accepted(info) => Some(SeqNum {
                seqnum: info.start_seqnum,
                timestamp: info.timestamp,
            }),
        })
    }

    pub fn path(&self) -> Path {
        Path::from(self.file_ref.as_ref())
    }
}

impl CommittedBatch {
    pub fn batch_id(&self) -> u32 {
        match self {
            CommittedBatch::Rejected(info) => info.batch_id,
            CommittedBatch::Accepted(info) => info.batch_id,
        }
    }
}

impl AcceptedBatchInfo {
    pub fn num_rows(&self) -> u32 {
        (self.end_seqnum - self.start_seqnum + 1) as u32
    }
}

impl fmt::Debug for SeqNum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let timestamp: UtcDateTime = self.timestamp.into();
        f.debug_struct("SeqNum")
            .field("seqnum", &self.seqnum)
            .field("timestamp", &timestamp)
            .finish()
    }
}

impl fmt::Debug for AcceptedBatchInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let timestamp: UtcDateTime = self.timestamp.into();
        f.debug_struct("AcceptedBatchInfo")
            .field("batch_id", &self.batch_id)
            .field("start_seqnum", &self.start_seqnum)
            .field("end_seqnum", &self.end_seqnum)
            .field("timestamp", &timestamp)
            .finish()
    }
}

impl TaskMetadata {
    pub fn new() -> Self {
        let task_id = ulid::Ulid::new().to_string();

        TaskMetadata {
            task_id,
            status: TaskStatus::Pending,
            created_at: std::time::SystemTime::now(),
            updated_at: std::time::SystemTime::now(),
        }
    }
}

impl Default for TaskMetadata {
    fn default() -> Self {
        Self::new()
    }
}

impl Task {
    pub fn new_compaction(task: CompactionTask) -> Self {
        Self::Compaction {
            metadata: Default::default(),
            task,
        }
    }

    pub fn new_create_table(task: CreateTableTask) -> Self {
        Self::CreateTable {
            metadata: Default::default(),
            task,
        }
    }

    pub fn new_commit(task: CommitTask) -> Self {
        Self::Commit {
            metadata: Default::default(),
            task,
        }
    }

    pub fn metadata(&self) -> &TaskMetadata {
        match self {
            Task::Compaction { metadata, .. } => metadata,
            Task::CreateTable { metadata, .. } => metadata,
            Task::Commit { metadata, .. } => metadata,
        }
    }

    pub fn type_url(&self) -> &str {
        match self {
            Task::Compaction { .. } => CompactionTask::TYPE_URL,
            Task::CreateTable { .. } => CreateTableTask::TYPE_URL,
            Task::Commit { .. } => CommitTask::TYPE_URL,
        }
    }

    pub fn task_id(&self) -> &str {
        match self {
            Task::Compaction { metadata, .. } => &metadata.task_id,
            Task::CreateTable { metadata, .. } => &metadata.task_id,
            Task::Commit { metadata, .. } => &metadata.task_id,
        }
    }

    pub fn as_commit(&self) -> Option<&CommitTask> {
        match self {
            Task::Commit { task, .. } => Some(task),
            _ => None,
        }
    }
}

impl TaskResult {
    pub fn take_compaction(self) -> Option<CompactionResult> {
        match self {
            TaskResult::Compaction(task) => Some(task),
            _ => None,
        }
    }

    pub fn take_create_table(self) -> Option<CreateTableResult> {
        match self {
            TaskResult::CreateTable(task) => Some(task),
            _ => None,
        }
    }

    pub fn take_commit(self) -> Option<CommitResult> {
        match self {
            TaskResult::Commit(task) => Some(task),
            _ => None,
        }
    }
}

impl CompleteTaskRequest {
    pub fn new_completed(task_id: String, result: TaskResult) -> Self {
        Self {
            task_id,
            result: TaskCompletionResult::Success(result),
        }
    }

    pub fn new_failed(task_id: String, error_message: String) -> Self {
        Self {
            task_id,
            result: TaskCompletionResult::Failure(error_message),
        }
    }
}

pub trait DatabaseTask {
    const TYPE_URL: &str;
    type Serialized: Message;

    fn into_serialized(self) -> Self::Serialized;
}

impl DatabaseTask for CommitTask {
    const TYPE_URL: &str = "types.googleapis.com/wings.table_metadata.CommitTask";
    type Serialized = crate::pb::CommitTask;

    fn into_serialized(self) -> Self::Serialized {
        self.into()
    }
}

impl DatabaseTask for CompactionTask {
    const TYPE_URL: &str = "types.googleapis.com/wings.table_metadata.CompactionTask";
    type Serialized = crate::pb::CompactionTask;

    fn into_serialized(self) -> Self::Serialized {
        self.into()
    }
}

impl DatabaseTask for CreateTableTask {
    const TYPE_URL: &str = "types.googleapis.com/wings.table_metadata.CreateTableTask";
    type Serialized = crate::pb::CreateTableTask;

    fn into_serialized(self) -> Self::Serialized {
        self.into()
    }
}
