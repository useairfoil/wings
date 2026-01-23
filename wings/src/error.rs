use std::net::AddrParseError;

use axum::http::uri::InvalidUri;
use snafu::Snafu;
use wings_control_plane::{
    ErrorKind,
    cluster_metadata::ClusterMetadataError,
    resources::{PartitionValueParseError, ResourceError},
    schema::SchemaError,
};
use wings_observability::ObservabilityError;

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
        #[snafu(source(from(ClusterMetadataError, Box::new)))]
        source: Box<ClusterMetadataError>,
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
    #[snafu(display("Invalid schema"))]
    InvalidSchema { source: SchemaError },
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
    #[snafu(display("Client error"))]
    Client {
        #[snafu(source(from(wings_client::ClientError, Box::new)))]
        source: Box<wings_client::ClientError>,
    },
    #[snafu(display("Arrow error"))]
    Arrow { source: arrow::error::ArrowError },
    #[snafu(display("Flight error"))]
    Flight {
        source: arrow_flight::error::FlightError,
    },
    #[snafu(display("Failed to initialize observability"))]
    Observability { source: ObservabilityError },
}

pub type Result<T, E = CliError> = std::result::Result<T, E>;

impl CliError {
    #[allow(dead_code)]
    pub fn kind(&self) -> ErrorKind {
        match self {
            Self::InvalidResourceName { .. }
            | Self::InvalidArgument { .. }
            | Self::InvalidPartitionValue
            | Self::PartitionValueParse { .. }
            | Self::InvalidSchema { .. }
            | Self::InvalidTimestampFormat { .. } => ErrorKind::Validation,
            Self::InvalidRemoteUrl { .. } | Self::InvalidServerUrl { .. } => {
                ErrorKind::Configuration
            }
            Self::ClusterMetadata { source, .. } => source.kind(),
            Self::ObjectStore { .. } | Self::Io { .. } => ErrorKind::Temporary,
            Self::Connection { .. }
            | Self::TonicReflection { .. }
            | Self::TonicServer { .. }
            | Self::Client { .. }
            | Self::Flight { .. } => ErrorKind::Temporary,
            Self::Arrow { .. } => ErrorKind::Internal,
            Self::Observability { .. } => ErrorKind::Internal,
        }
    }
}
