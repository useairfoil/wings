use axum::http::uri::InvalidUri;
use snafu::Snafu;
use tokio::task::JoinError;
use wings_client::ClientError;
use wings_control_plane::{
    ErrorKind, cluster_metadata::ClusterMetadataError, resources::ResourceError,
};

use crate::helpers::RangeParserError;

/// CLI error types.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum CliError {
    #[snafu(display("Invalid namespace name"))]
    InvalidNamespaceName { source: ResourceError },
    #[snafu(display("Invalid range format"))]
    InvalidRange { source: RangeParserError },
    #[snafu(display("Invalid remote URL"))]
    InvalidRemoteUrl { source: InvalidUri },
    #[snafu(display("Connection error"))]
    Connection { source: tonic::transport::Error },
    #[snafu(display("Tonic server error"))]
    TonicServer { source: tonic::transport::Error },
    #[snafu(display("Failed admin operation {operation}"))]
    ClusterMetadata {
        operation: &'static str,
        source: ClusterMetadataError,
    },
    #[snafu(display("Failed client operation"))]
    ClientError { source: ClientError },
    #[snafu(display("Failed join operation"))]
    JoinError { source: JoinError },
}

pub type Result<T, E = CliError> = std::result::Result<T, E>;

impl CliError {
    pub fn kind(&self) -> ErrorKind {
        match self {
            Self::InvalidNamespaceName { .. } | Self::InvalidRange { .. } => ErrorKind::Validation,
            Self::InvalidRemoteUrl { .. } => ErrorKind::Configuration,
            Self::Connection { .. } | Self::TonicServer { .. } => ErrorKind::Temporary,
            Self::ClusterMetadata { source, .. } => source.kind(),
            Self::ClientError { source } => source.kind(),
            Self::JoinError { .. } => ErrorKind::Internal,
        }
    }
}
