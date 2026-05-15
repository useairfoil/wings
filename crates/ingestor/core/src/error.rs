use std::sync::Arc;

use parquet::errors::ParquetError;
use snafu::Snafu;
use wings_control_plane_core::log_metadata::LogMetadataError;

#[derive(Debug, Clone, Snafu)]
#[snafu(visibility(pub))]
pub enum IngestorError {
    #[snafu(display("validation error: {}", message))]
    Validation { message: String },
    #[snafu(display("parquet error: {}", message))]
    Parquet {
        message: String,
        #[snafu(source(from(ParquetError, Arc::new)))]
        source: Arc<ParquetError>,
    },
    #[snafu(display("object store error: {}", message))]
    ObjectStore {
        message: String,
        #[snafu(source(from(object_store::Error, Arc::new)))]
        source: Arc<object_store::Error>,
    },
    #[snafu(display("log metadata error"))]
    LogMetadata { source: LogMetadataError },
    #[snafu(display("response channel closed"))]
    ChannelClosed,
}

pub type Result<T, E = IngestorError> = std::result::Result<T, E>;
