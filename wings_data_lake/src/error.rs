use deltalake_core::DeltaTableError;
use snafu::Snafu;
use wings_control_plane_core::ClusterMetadataError;
use wings_observability::ErrorKind;
use wings_schema::SchemaError;

use crate::parquet_writer::error::Error as ParquetError;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum DataLakeError {
    #[snafu(display("Cluster metadata error: {}", operation))]
    ClusterMetadata {
        operation: &'static str,
        source: ClusterMetadataError,
    },
    #[snafu(transparent)]
    ObjectStore { source: object_store::Error },
    #[snafu(transparent)]
    Parquet { source: ParquetError },
    #[snafu(transparent)]
    DataFusion {
        source: datafusion::error::DataFusionError,
    },
    #[snafu(transparent)]
    Delta { source: DeltaTableError },
    #[snafu(display("Failed to create file path"))]
    Path { source: object_store::path::Error },
    #[snafu(display("Unsupported operation: {}", operation))]
    UnsupportedOperation { operation: &'static str },
    #[snafu(display("Failed to create table schema"))]
    InvalidSchema { source: SchemaError },
    #[snafu(display("Internal error: {}", message))]
    Internal { message: String },
}

pub type Result<T, E = DataLakeError> = std::result::Result<T, E>;

impl DataLakeError {
    pub fn kind(&self) -> ErrorKind {
        match self {
            Self::ClusterMetadata { source, .. } => source.kind(),
            Self::Internal { .. } => ErrorKind::Internal,
            Self::ObjectStore { .. }
            | Self::Parquet { .. }
            | Self::DataFusion { .. }
            | Self::Delta { .. } => ErrorKind::Temporary,
            Self::Path { .. } | Self::UnsupportedOperation { .. } | Self::InvalidSchema { .. } => {
                ErrorKind::Validation
            }
        }
    }
}
