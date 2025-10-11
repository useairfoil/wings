mod error;
mod memory;
pub mod stream;
pub mod timestamp;
pub mod tonic;

use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use object_store::path::Path;

use crate::resources::{NamespaceName, PartitionValue, TopicName};

pub use self::error::{LogMetadataError, Result};
pub use self::memory::InMemoryLogMetadata;

#[async_trait]
pub trait LogMetadata: Send + Sync {
    /// Commit a folio with several batches across topics and partitions.
    async fn commit_folio(
        &self,
        namespace: NamespaceName,
        file_ref: String,
        pages: &[CommitPageRequest],
    ) -> Result<Vec<CommitPageResponse>>;

    /// Retrieves the locations of the logs for the specified topic and partition.
    async fn get_log_location(&self, request: GetLogLocationRequest) -> Result<Vec<LogLocation>>;

    /// List the partitions of a topic.
    async fn list_partitions(
        &self,
        request: ListPartitionsRequest,
    ) -> Result<ListPartitionsResponse>;
}

/// The offset of a log together with its timestamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogOffset {
    /// The offset of the log.
    pub offset: u64,
    /// The timestamp of the log.
    pub timestamp: SystemTime,
}

/// Information about a batch that was committed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommittedBatch {
    /// The batch was rejected.
    Rejected(RejectedBatchInfo),
    /// The batch was accepted and belongs to the topic.
    Accepted(AcceptedBatchInfo),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RejectedBatchInfo {
    /// The number of messages in the batch.
    pub num_messages: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptedBatchInfo {
    /// The offset of the first message in the batch.
    pub start_offset: u64,
    /// The offset of the last message in the batch.
    pub end_offset: u64,
    /// The timestamp of the batch.
    pub timestamp: SystemTime,
}

/// Request to commit a page of batches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitPageRequest<B = CommitBatchRequest> {
    /// The topic id of the batch to commit.
    pub topic_name: TopicName,
    /// The partition value, if any.
    pub partition_value: Option<PartitionValue>,
    /// The individual batches to commit.
    pub batches: Vec<B>,
    /// The number of messages in the batch.
    pub num_messages: u32,
    /// The start offset of the batch in the folio file.
    pub offset_bytes: u64,
    /// The batch size, in bytes.
    pub batch_size_bytes: u64,
}

/// A page that has been successfully committed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitPageResponse<B = CommittedBatch> {
    /// The topic id of the batch that was committed.
    pub topic_name: TopicName,
    /// The partition value, if any.
    pub partition_value: Option<PartitionValue>,
    /// The result of committing the batches.
    pub batches: Vec<B>,
}

/// Represents a single write operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitBatchRequest {
    /// The requested timestamp for the write request.
    pub timestamp: Option<SystemTime>,
    /// The number of messages in the write request.
    pub num_messages: u32,
}

/// Request to retrieve the location of a log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetLogLocationRequest {
    /// The topic name of the log.
    pub topic_name: TopicName,
    /// The partition value, if any.
    pub partition_value: Option<PartitionValue>,
    /// The location request.
    pub location: LogLocationRequest,
    /// The options for the request.
    pub options: GetLogLocationOptions,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetLogLocationOptions {
    /// The deadline for the request.
    pub deadline: Duration,
    /// The minimum number of rows to retrieve.
    pub min_rows: usize,
    /// The (soft) maximum number of rows to retrieve.
    pub max_rows: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogLocationRequest {
    /// Request the location of the logs starting at the specified offset.
    Offset(u64),
}

/// Location of a specific log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogLocation {
    Folio(FolioLocation),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FolioLocation {
    /// Folio file name.
    pub file_ref: String,
    /// Offset within the folio file.
    pub offset_bytes: u64,
    /// Size of the partition data in the folio file.
    pub size_bytes: u64,
    /// The batches that were committed.
    pub batches: Vec<CommittedBatch>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListPartitionsRequest {
    /// The topic name of the log.
    pub topic_name: TopicName,
    /// The page size for the request.
    pub page_size: Option<usize>,
    /// The continuation token for the request.
    pub page_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListPartitionsResponse {
    /// The partitions of the topic.
    pub partitions: Vec<PartitionMetadata>,
    /// The continuation token for the next page.
    pub next_page_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionMetadata {
    /// The partition value.
    pub partition_value: Option<PartitionValue>,
    /// The end offset of the log.
    pub end_offset: LogOffset,
}

impl LogOffset {
    /// Creates a new `LogOffset` with the timestamp.
    pub fn with_timestamp(&self, timestamp: SystemTime) -> LogOffset {
        LogOffset {
            offset: self.offset,
            timestamp,
        }
    }

    pub fn previous(&self) -> LogOffset {
        LogOffset {
            offset: self.offset.saturating_sub(1),
            timestamp: self.timestamp,
        }
    }
}

impl CommitBatchRequest {
    pub fn new(num_messages: u32) -> Self {
        Self {
            num_messages,
            timestamp: None,
        }
    }

    pub fn new_with_timestamp(num_messages: u32, timestamp: SystemTime) -> Self {
        Self {
            num_messages,
            timestamp: Some(timestamp),
        }
    }
}

impl Default for LogOffset {
    fn default() -> Self {
        LogOffset {
            offset: 0,
            timestamp: SystemTime::UNIX_EPOCH,
        }
    }
}

impl LogLocation {
    pub fn start_offset(&self) -> Option<LogOffset> {
        match self {
            LogLocation::Folio(folio) => folio.start_offset(),
        }
    }

    pub fn end_offset(&self) -> Option<LogOffset> {
        match self {
            LogLocation::Folio(folio) => folio.end_offset(),
        }
    }
}

impl FolioLocation {
    pub fn end_offset(&self) -> Option<LogOffset> {
        self.batches.iter().rev().find_map(|batch| match batch {
            CommittedBatch::Rejected(_) => None,
            CommittedBatch::Accepted(info) => Some(LogOffset {
                offset: info.end_offset,
                timestamp: info.timestamp,
            }),
        })
    }

    pub fn start_offset(&self) -> Option<LogOffset> {
        self.batches.iter().find_map(|batch| match batch {
            CommittedBatch::Rejected(_) => None,
            CommittedBatch::Accepted(info) => Some(LogOffset {
                offset: info.start_offset,
                timestamp: info.timestamp,
            }),
        })
    }

    pub fn path(&self) -> Path {
        Path::from(self.file_ref.as_ref())
    }
}

impl AcceptedBatchInfo {
    pub fn num_messages(&self) -> u32 {
        (self.end_offset - self.start_offset + 1) as u32
    }
}
