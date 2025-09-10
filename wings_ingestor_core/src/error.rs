use std::sync::Arc;

use parquet::errors::ParquetError;
use snafu::Snafu;
use wings_control_plane::log_metadata::LogMetadataError;

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
    /// Log metadata error.
    ///
    /// This errors are used when something goes wrong with the log metadata.
    #[snafu(display("log metadata error: {message}"))]
    LogMetadata {
        message: &'static str,
        source: LogMetadataError,
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
