use datafusion::error::DataFusionError;
use snafu::Snafu;
use wings_control_plane::{
    ErrorKind, cluster_metadata::ClusterMetadataError, data_lake::DataLakeError,
    log_metadata::LogMetadataError,
};

/// Errors that can occur in the worker pool.
#[derive(Debug, Snafu)]
pub enum WorkerPoolError {}

/// Errors that can occur in a worker.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum WorkerError {
    #[snafu(display("Failed log metadata operation: {}", operation))]
    LogMetadata {
        source: LogMetadataError,
        operation: &'static str,
    },
    #[snafu(display("Failed cluster metadata operation: {}", operation))]
    ClusterMetadata {
        source: ClusterMetadataError,
        operation: &'static str,
    },
    #[snafu(display("Failed data lake operation: {}", operation))]
    DataLake {
        source: DataLakeError,
        operation: &'static str,
    },
    #[snafu(display("DataFusion error"))]
    DataFusion { source: DataFusionError },
}

pub type Result<T, E = WorkerError> = std::result::Result<T, E>;

impl WorkerError {
    pub fn kind(&self) -> ErrorKind {
        match self {
            Self::LogMetadata { source, .. } => source.kind(),
            Self::ClusterMetadata { source, .. } => source.kind(),
            Self::DataLake { source, .. } => source.kind(),
            Self::DataFusion { .. } => ErrorKind::Temporary,
        }
    }
}
