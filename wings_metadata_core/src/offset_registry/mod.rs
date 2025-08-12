//! The batch committer trait and related types.

pub mod error;
pub mod memory;
pub mod proto;
pub mod remote;
pub mod server;
pub mod stream;
pub mod types;

use std::time::SystemTime;

pub use error::{OffsetRegistryError, OffsetRegistryResult};
pub use memory::InMemoryOffsetRegistry;
pub use stream::{
    PaginatedPartitionValueStream, PartitionValuePageStream, SendablePartitionValuePageStream,
};
pub use types::*;

use crate::{
    admin::{NamespaceName, TopicName},
    partition::PartitionValue,
};
use async_trait::async_trait;

/// The OffsetRegistry trait provides methods for assigning and querying offsets.
#[async_trait]
pub trait OffsetRegistry: Send + Sync {
    /// Commit a folio to assign offsets.
    async fn commit_folio(
        &self,
        namespace: NamespaceName,
        file_ref: String,
        batches: &[BatchToCommit],
    ) -> OffsetRegistryResult<Vec<CommittedBatch>>;

    /// Returns the location of the offset for a given topic and partition.
    ///
    /// If the specified offset is not found, the method has up to the deadline to find it.
    /// If the offset is not found, returns None.
    async fn offset_location(
        &self,
        topic: TopicName,
        partition_value: Option<PartitionValue>,
        offset: u64,
        deadline: SystemTime,
    ) -> OffsetRegistryResult<Option<OffsetLocation>>;

    async fn list_topic_partition_values(
        &self,
        request: ListTopicPartitionValuesRequest,
    ) -> OffsetRegistryResult<ListTopicPartitionValuesResponse>;
}
