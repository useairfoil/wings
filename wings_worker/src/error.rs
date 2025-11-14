use datafusion::error::DataFusionError;
use parquet::errors::ParquetError;
use snafu::Snafu;
use wings_control_plane::{
    cluster_metadata::ClusterMetadataError, log_metadata::LogMetadataError, paths::ParquetPathError,
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
    #[snafu(display("DataFusion error"))]
    DataFusion { source: DataFusionError },
    #[snafu(display("Parquet error"))]
    Parquet { source: ParquetError },
    #[snafu(display("Object store error"))]
    ObjectStore { source: object_store::Error },
    #[snafu(display("Failed to build Parquet file path"))]
    ParquetPath { source: ParquetPathError },
}

pub type Result<T, E = WorkerError> = std::result::Result<T, E>;
