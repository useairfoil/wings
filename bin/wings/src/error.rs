use std::net::AddrParseError;

use http::uri::InvalidUri;
use snafu::Snafu;
use wings_observability::ObservabilityError;

/// CLI error types.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum CliError {
    #[snafu(display("Invalid gRPC server address"))]
    InvalidGrpcAddress { source: AddrParseError },
    #[snafu(display("Failed to create object store"))]
    ObjectStore { source: object_store::Error },
    #[snafu(display("Failed to create secret manager"))]
    SecretManager { source: wings_secret_manager::Error },
    #[snafu(display("Failed to create gRPC reflection service"))]
    TonicReflection {
        source: tonic_reflection::server::Error,
    },
    #[snafu(display("gRPC server error"))]
    TonicServer { source: tonic::transport::Error },
    #[snafu(display("Failed to initialize observability"))]
    Observability { source: ObservabilityError },
    #[snafu(display("Invalid remote URL"))]
    InvalidRemoteUrl { source: InvalidUri },
    #[snafu(display("Connection error"))]
    Connection { source: tonic::transport::Error },
    #[snafu(display("Arrow error"))]
    Arrow { source: arrow::error::ArrowError },
    #[snafu(display("Flight error"))]
    Flight {
        source: arrow_flight::error::FlightError,
    },
}

pub type Result<T, E = CliError> = std::result::Result<T, E>;
