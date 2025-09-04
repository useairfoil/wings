//! Data types for the offset registry.

use std::time::SystemTime;

use crate::{admin::TopicName, partition::PartitionValue};

/// Represents a single write operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitBatchRequest {
    /// The requested timestamp for the write request.
    pub timestamp: Option<SystemTime>,
    /// The number of messages in the write request.
    pub num_messages: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitBatchResponse {
    Success {
        start_offset: u64,
        end_offset: u64,
        timestamp: SystemTime,
    },
    Error {
        code: u32,
        message: String,
    },
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
    /// The first assigned offset of the batch.
    pub start_offset: u64,
    /// The last assigned offset of the batch.
    pub end_offset: u64,
    /// The result of committing the batches.
    pub batches: Vec<CommitBatchResponse>,
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
    /// First offset of the partition data in the folio file.
    pub start_offset: u64,
    /// Last offset of the partition data in the folio file.
    pub end_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionValueState {
    pub partition_value: Option<PartitionValue>,
    pub next_offset: u64,
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

    pub fn start_offset(&self) -> u64 {
        match self {
            OffsetLocation::Folio(location) => location.start_offset,
        }
    }

    pub fn end_offset(&self) -> u64 {
        match self {
            OffsetLocation::Folio(location) => location.end_offset,
        }
    }
}
