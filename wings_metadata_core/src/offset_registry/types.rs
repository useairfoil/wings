//! Data types for the offset registry.

use std::time::SystemTime;

use crate::{admin::TopicName, partition::PartitionValue};

/// Represents metadata associated with a write operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteMetadata {
    /// The requested timestamp for the batch.
    pub timestamp: Option<SystemTime>,
}

/// A batch that needs to be committed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteToCommit {
    /// The topic id of the batch to commit.
    pub topic_name: TopicName,
    /// The partition value, if any.
    pub partition_value: Option<PartitionValue>,
    /// The metadata associated with each record batch.
    pub metadata: Vec<WriteMetadata>,
    /// The number of messages in the batch.
    pub num_messages: u32,
    /// The start offset of the batch in the folio file.
    pub offset_bytes: u64,
    /// The batch size, in bytes.
    pub batch_size_bytes: u64,
}

/// A batch that has been successfully committed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedWrite {
    /// The topic id of the batch that was committed.
    pub topic_name: TopicName,
    /// The partition value, if any.
    pub partition_value: Option<PartitionValue>,
    /// The first assigned offset of the batch.
    pub start_offset: u64,
    /// The last assigned offset of the batch.
    pub end_offset: u64,
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
