use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use arrow::array::RecordBatch;
use error_stack::ResultExt;
use tokio_util::sync::CancellationToken;
use wings_metadata_core::admin::{Admin, TopicName};
use wings_metadata_core::offset_registry::OffsetRegistry;
use wings_metadata_core::partition::PartitionValue;
use wings_object_store::ObjectStoreFactory;

use crate::error::{ServerError, ServerResult};

/// Shared computation constraints for fetch operations.
///
/// This struct tracks how many messages have been collected across multiple
/// topic and partition fetches, along with the minimum and maximum number
/// of messages requested by the user and the deadline.
#[derive(Debug, Clone)]
pub struct FetchState {
    /// Atomic counter for the total number of messages collected.
    messages_collected: Arc<AtomicUsize>,
    /// Minimum number of messages to fetch.
    min_messages: usize,
    /// Maximum number of messages to fetch.
    max_messages: usize,
    /// Deadline for the fetch operation.
    deadline: Instant,
}

/// Response from a fetch operation for a specific topic and partition.
#[derive(Debug, Clone)]
pub struct FetchResponse {
    /// The topic name.
    pub topic: TopicName,
    /// The partition value, if any.
    pub partition: Option<PartitionValue>,
    /// The schema of the messages returned.
    pub schema: arrow::datatypes::SchemaRef,
    /// The offset of the first record returned.
    pub start_offset: u64,
    /// The offset of the last record returned.
    pub end_offset: u64,
    /// The messages as an Arrow RecordBatch.
    pub batch: RecordBatch,
}

/// The main fetcher service for retrieving messages from Wings.
///
/// This struct is responsible for implementing the logic to fetch data
/// according to the constraints specified in the fetch method.
#[derive(Clone)]
pub struct Fetcher {
    /// Client for fetching metadata about topics and namespaces.
    admin: Arc<dyn Admin>,
    /// Client for interacting with the offset registry.
    offset_registry: Arc<dyn OffsetRegistry>,
    /// Factory for creating object store clients.
    object_store_factory: Arc<dyn ObjectStoreFactory>,
}

impl Fetcher {
    /// Create a new `Fetcher` instance.
    ///
    /// # Arguments
    ///
    /// * `admin` - Client for fetching metadata
    /// * `object_store_factory` - Factory for creating object store clients
    /// * `offset_registry` - Client for offset registry operations
    pub fn new(
        admin: Arc<dyn Admin>,
        object_store_factory: Arc<dyn ObjectStoreFactory>,
        offset_registry: Arc<dyn OffsetRegistry>,
    ) -> Self {
        Self {
            admin,
            offset_registry,
            object_store_factory,
        }
    }

    /// Fetch messages for a specific topic and partition.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic name
    /// * `partition` - The partition value, if any
    /// * `offset` - The offset of the first record to fetch
    /// * `state` - Shared computation constraints
    /// * `ct` - Token for cancelling the operation
    ///
    /// # Returns
    ///
    /// Returns a `FetchResponse` containing the fetched messages and metadata.
    pub async fn fetch(
        &self,
        topic: TopicName,
        partition: Option<PartitionValue>,
        offset: u64,
        state: FetchState,
        ct: CancellationToken,
    ) -> ServerResult<FetchResponse> {
        // TODO: Implement the actual fetching logic
        // For now, return a placeholder response
        Err(error_stack::report!(ServerError::InvalidRequest(
            "fetch method not implemented yet".to_string(),
        )))
    }
}

impl FetchState {
    /// Create a new `FetchState` with the given constraints.
    ///
    /// # Arguments
    ///
    /// * `min_messages` - Minimum number of messages to fetch (must be >= 1)
    /// * `max_messages` - Maximum number of messages to fetch (must be <= 100_000)
    /// * `deadline` - Deadline for the fetch operation
    ///
    /// # Returns
    ///
    /// Returns a new `FetchState` instance.
    pub fn new(min_messages: usize, max_messages: usize, deadline: Instant) -> ServerResult<Self> {
        if min_messages < 1 {
            return Err(error_stack::report!(ServerError::InvalidRequest(
                "min_messages must be at least 1".to_string(),
            )))
            .attach_printable("invalid min_messages parameter");
        }

        if max_messages > 100_000 {
            return Err(error_stack::report!(ServerError::InvalidRequest(
                "max_messages must be at most 100_000".to_string(),
            )))
            .attach_printable("invalid max_messages parameter");
        }

        if min_messages > max_messages {
            return Err(error_stack::report!(ServerError::InvalidRequest(
                "min_messages must be less than or equal to max_messages".to_string(),
            )))
            .attach_printable("invalid min/max messages relationship");
        }

        Ok(Self {
            messages_collected: Arc::new(AtomicUsize::new(0)),
            min_messages,
            max_messages,
            deadline,
        })
    }

    /// Increment the message counter and return the new total.
    pub fn increment_messages(&self, count: usize) -> usize {
        self.messages_collected.fetch_add(count, Ordering::Relaxed) + count
    }

    /// Get the current message count.
    pub fn current_count(&self) -> usize {
        self.messages_collected.load(Ordering::Relaxed)
    }

    /// Check if we should stop fetching based on the message count.
    pub fn should_stop_on_count(&self) -> bool {
        self.current_count() >= self.max_messages
    }

    /// Check if we have collected the minimum required messages.
    pub fn has_min_messages(&self) -> bool {
        self.current_count() >= self.min_messages
    }

    /// Get the deadline for this fetch operation.
    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    /// Get the minimum messages requirement.
    pub fn min_messages(&self) -> usize {
        self.min_messages
    }

    /// Get the maximum messages limit.
    pub fn max_messages(&self) -> usize {
        self.max_messages
    }
}
