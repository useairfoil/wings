use std::net::AddrParseError;

use axum::http::uri::InvalidUri;
use datafusion::error::DataFusionError;
use snafu::Snafu;
use wings_control_plane::{
    cluster_metadata::ClusterMetadataError,
    log_metadata::LogMetadataError,
    resources::{PartitionValueParseError, ResourceError},
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
    #[snafu(display("Failed cluster metadata operation {operation}"))]
    ClusterMetadata {
        operation: &'static str,
        source: ClusterMetadataError,
    },
    #[snafu(display("Failed log metadata operation {operation}"))]
    LogMetadata {
        operation: &'static str,
        source: LogMetadataError,
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
    #[snafu(display("Arrow error"))]
    Arrow { source: arrow::error::ArrowError },
    #[snafu(display("Flight error"))]
    Flight {
        source: arrow_flight::error::FlightError,
    },
}

pub type Result<T, E = CliError> = std::result::Result<T, E>;
