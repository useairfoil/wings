use datafusion::error::DataFusionError;
use snafu::Snafu;
use wings_control_plane_core::{
    cluster_metadata::ClusterMetadataError, table_metadata::TableMetadataError,
};
use wings_data_lake::DataLakeError;

/// Errors that can occur in the worker pool.
#[derive(Debug, Snafu)]
pub enum WorkerPoolError {}

/// Errors that can occur in a worker.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum WorkerError {
    #[snafu(display("Failed table metadata operation: {}", operation))]
    TableMetadata {
        source: TableMetadataError,
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
    #[snafu(transparent)]
    DataFusion { source: DataFusionError },
    #[snafu(transparent)]
    Query {
        source: wings_query::TableLogicalPlanError,
    },
}

pub type Result<T, E = WorkerError> = std::result::Result<T, E>;
