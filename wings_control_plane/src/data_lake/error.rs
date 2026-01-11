use parquet::errors::ParquetError;
use snafu::Snafu;

use crate::{ErrorKind, cluster_metadata::ClusterMetadataError, paths::ParquetPathError};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum DataLakeError {
    #[snafu(display("Cluster metadata error: {}", operation))]
    ClusterMetadata {
        operation: &'static str,
        source: ClusterMetadataError,
    },
    #[snafu(display("Object store error"))]
    ObjectStore { source: object_store::Error },
    #[snafu(display("Parquet error"))]
    Parquet { source: ParquetError },
    #[snafu(display("Failed to create parquet file path"))]
    ParquetPath { source: ParquetPathError },
}

pub type Result<T, E = DataLakeError> = std::result::Result<T, E>;

impl DataLakeError {
    pub fn kind(&self) -> ErrorKind {
        match self {
            Self::ClusterMetadata { source, .. } => source.kind(),
            Self::ObjectStore { .. } | Self::Parquet { .. } => ErrorKind::Temporary,
            Self::ParquetPath { .. } => ErrorKind::Validation,
        }
    }
}
