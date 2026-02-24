use axum::http::uri::InvalidUri;
use snafu::Snafu;
use tokio::{sync::mpsc::error::SendError, task::JoinError};
use wings_client::ClientError;
use wings_control_plane_core::cluster_metadata::ClusterMetadataError;
use wings_observability::ErrorKind;
use wings_resources::ResourceError;

use crate::log::Event;

/// CLI error types.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum CliError {
    #[snafu(display("Invalid {resource} name"))]
    InvalidResourceName {
        resource: &'static str,
        source: ResourceError,
    },
    #[snafu(display("Invalid remote URL"))]
    InvalidRemoteUrl { source: InvalidUri },
    #[snafu(display("Failed admin operation {operation}"))]
    ClusterMetadata {
        operation: &'static str,
        source: ClusterMetadataError,
    },
    #[snafu(display("Failed to push data"))]
    PushError { source: ClientError },
    #[snafu(display("Failed to fetch data"))]
    FetchError { source: ClientError },
    #[snafu(transparent)]
    Connection { source: tonic::transport::Error },
    #[snafu(transparent)]
    JoinError { source: JoinError },
    #[snafu(transparent)]
    ArrowError {
        source: datafusion::common::arrow::error::ArrowError,
    },
    #[snafu(transparent)]
    EventChannelClosed { source: SendError<Event> },
}

pub type Result<T, E = CliError> = std::result::Result<T, E>;

impl CliError {
    pub fn kind(&self) -> ErrorKind {
        match self {
            Self::ClusterMetadata { source, .. } => source.kind(),
            Self::PushError { source } | Self::FetchError { source } => source.kind(),
            Self::InvalidResourceName { .. } => ErrorKind::Validation,
            Self::InvalidRemoteUrl { .. } => ErrorKind::Configuration,
            Self::Connection { .. } => ErrorKind::Temporary,
            Self::JoinError { .. } | Self::ArrowError { .. } | Self::EventChannelClosed { .. } => {
                ErrorKind::Internal
            }
        }
    }
}
