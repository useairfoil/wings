use axum::http::uri::InvalidUri;
use snafu::Snafu;
use wings_metadata_core::{admin::AdminError, resource::ResourceError};
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
    Admin {
        operation: &'static str,
        source: AdminError,
    },
    #[snafu(display("Failed HTTP push operation"))]
    HttpPushError { source: HttpPushClientError },
}

pub type Result<T, E = CliError> = std::result::Result<T, E>;
