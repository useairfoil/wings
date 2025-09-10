use axum::http::uri::InvalidUri;
use snafu::Snafu;
use wings_control_plane::{cluster_metadata::ClusterMetadataError, resources::ResourceError};
use wings_push_client::HttpPushClientError;

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
    #[snafu(display("Failed HTTP push operation"))]
    HttpPushError { source: HttpPushClientError },
}

pub type Result<T, E = CliError> = std::result::Result<T, E>;
