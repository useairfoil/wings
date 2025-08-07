use std::sync::Arc;

use parquet::errors::ParquetError;
use snafu::Snafu;
use wings_metadata_core::offset_registry::OffsetRegistryError;

/// Ingestor error types.
///
/// The message associated with an error is forwarded to the client,
/// for this reason it should contain information that is useful to the user.
#[derive(Debug, Clone, Snafu)]
#[snafu(visibility(pub))]
pub enum IngestorError {
    /// Internal server error.
    ///
    /// This errors are used when something goes wrong internally.
    #[snafu(display("internal server error: {message}"))]
    Internal { message: String },
    /// Schema error.
    ///
    /// This is for errors related to the topic's schema.
    #[snafu(display("schema error: {message}"))]
    Schema { message: String },
    #[snafu(display("parquet error: {message}"))]
    /// Parquet error.
    Parquet {
        message: &'static str,
        #[snafu(source(from(ParquetError, Arc::new)))]
        source: Arc<ParquetError>,
    },
    /// Offset registry error.
    ///
    /// This errors are used when something goes wrong with the offset registry.
    #[snafu(display("offset registry error: {message}"))]
    OffsetRegistry {
        message: &'static str,
        source: OffsetRegistryError,
    },
    /// Object store error.
    #[snafu(display("object store error: {message}"))]
    ObjectStore {
        message: &'static str,
        #[snafu(source(from(object_store::Error, Arc::new)))]
        source: Arc<object_store::Error>,
    },
    /// Validation error.
    ///
    /// This errors are used when a precondition is not met.
    #[snafu(display("validation error: {message}"))]
    Validation { message: String },
    /// Reply channel closed.
    #[snafu(display("reply channel closed"))]
    ReplyChannelClosed,
}

pub type Result<T, E = IngestorError> = std::result::Result<T, E>;

impl IngestorError {
    /// Returns the user-visible error message.
    pub fn message(&self) -> String {
        self.to_string()
    }
}
