//! Data types for the offset registry.

use std::time::SystemTime;

use crate::{admin::TopicName, partition::PartitionValue};

use super::timestamp::LogOffset;

/// Represents a single write operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitBatchRequest {
    /// The requested timestamp for the write request.
    pub timestamp: Option<SystemTime>,
    /// The number of messages in the write request.
    pub num_messages: u32,
}

/// Information about a batch that was committed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommittedBatch {
    /// The batch was rejected.
    Rejected { num_messages: u32 },
    /// The batch was accepted and belongs to the topic.
    Accepted(AcceptedBatchInfo),
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
pub struct CommitPageRequest {
    /// The topic id of the batch to commit.
    pub topic_name: TopicName,
    /// The partition value, if any.
    pub partition_value: Option<PartitionValue>,
    /// The individual batches to commit.
    pub batches: Vec<CommitBatchRequest>,
    /// The number of messages in the batch.
    pub num_messages: u32,
    /// The start offset of the batch in the folio file.
    pub offset_bytes: u64,
    /// The batch size, in bytes.
    pub batch_size_bytes: u64,
}

/// A batch that has been successfully committed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitPageResponse {
    /// The topic id of the batch that was committed.
    pub topic_name: TopicName,
    /// The partition value, if any.
    pub partition_value: Option<PartitionValue>,
    /// The result of committing the batches.
    pub batches: Vec<CommittedBatch>,
}

/// Location of a specific offset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OffsetLocation {
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
pub struct PartitionValueState {
    pub partition_value: Option<PartitionValue>,
    pub next_offset: LogOffset,
}

pub struct ListTopicPartitionStatesRequest {
    pub topic_name: TopicName,
    pub page_size: Option<usize>,
    pub page_token: Option<String>,
}

pub struct ListTopicPartitionStatesResponse {
    pub states: Vec<PartitionValueState>,
    pub next_page_token: Option<String>,
}

impl CommitBatchRequest {
    pub fn new(num_messages: u32) -> Self {
        CommitBatchRequest {
            timestamp: None,
            num_messages,
        }
    }

    pub fn new_with_timestamp(num_messages: u32, timestamp: SystemTime) -> Self {
        CommitBatchRequest {
            timestamp: Some(timestamp),
            num_messages,
        }
    }
}

impl OffsetLocation {
    pub fn as_folio(&self) -> Option<&FolioLocation> {
        match self {
            OffsetLocation::Folio(location) => Some(location),
        }
    }

    pub fn file_ref(&self) -> &str {
        match self {
            OffsetLocation::Folio(location) => &location.file_ref,
        }
    }

    pub fn start_offset(&self) -> Option<u64> {
        match self {
            OffsetLocation::Folio(location) => location.start_offset(),
        }
    }

    pub fn end_offset(&self) -> Option<u64> {
        match self {
            OffsetLocation::Folio(location) => location.end_offset(),
        }
    }
}

impl FolioLocation {
    pub fn start_offset(&self) -> Option<u64> {
        self.batches
            .iter()
            .find_map(|b| b.as_accepted())
            .map(|info| info.start_offset)
    }

    pub fn end_offset(&self) -> Option<u64> {
        self.batches
            .iter()
            .rev()
            .find_map(|b| b.as_accepted())
            .map(|info| info.end_offset)
    }
}

impl CommittedBatch {
    pub fn as_accepted(&self) -> Option<&AcceptedBatchInfo> {
        match self {
            CommittedBatch::Accepted(info) => Some(info),
            _ => None,
        }
    }
}

impl AcceptedBatchInfo {
    pub fn num_messages(&self) -> u32 {
        (self.end_offset - self.start_offset + 1) as _
    }
}
