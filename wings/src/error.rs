use std::net::AddrParseError;

use axum::http::uri::InvalidUri;
use datafusion::error::DataFusionError;
use snafu::Snafu;
use wings_control_plane::{
    admin::AdminError, offset_registry::OffsetRegistryError, partition::PartitionValueParseError,
    resource::ResourceError,
};

/// CLI error types.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum CliError {
    #[snafu(display("Invalid {resource} name"))]
    InvalidResourceName {
        resource: &'static str,
        source: ResourceError,
    },
    #[snafu(display("Failed admin operation {operation}"))]
    Admin {
        operation: &'static str,
        source: AdminError,
    },
    #[snafu(display("Failed offset registry operation {operation}"))]
    OffsetRegistry {
        operation: &'static str,
        source: OffsetRegistryError,
    },
    #[snafu(display("Invalid {name} argument: {message}"))]
    InvalidArgument { name: &'static str, message: String },
    #[snafu(display("Object store error"))]
    ObjectStore { source: object_store::Error },
    #[snafu(display("IO error"))]
    Io { source: std::io::Error },
    #[snafu(display("Invalid partition value"))]
    InvalidPartitionValue,
    #[snafu(display("Failed to parse partition value"))]
    PartitionValueParse { source: PartitionValueParseError },
    #[snafu(display("Invalid timestamp format"))]
    InvalidTimestampFormat { source: chrono::ParseError },
    #[snafu(display("Invalid remote URL"))]
    InvalidRemoteUrl { source: InvalidUri },
    #[snafu(display("Invalid server URL"))]
    InvalidServerUrl { source: AddrParseError },
    #[snafu(display("Connection error"))]
    Connection { source: tonic::transport::Error },
    #[snafu(display("Tonic reflection error"))]
    TonicReflection {
        source: tonic_reflection::server::Error,
    },
    #[snafu(display("Tonic server error"))]
    TonicServer { source: tonic::transport::Error },
    #[snafu(display("Push client error"))]
    PushClient {
        source: wings_push_client::HttpPushClientError,
    },
    #[snafu(display("JSON parse error"))]
    JsonParse { source: serde_json::Error },
    #[snafu(display("DataFusion error"))]
    DataFusion { source: DataFusionError },
}

pub type Result<T, E = CliError> = std::result::Result<T, E>;
