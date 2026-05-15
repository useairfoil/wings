use snafu::Snafu;
use wings_observability::{ErrorExt, StatusCode};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum FlightServerError {
    #[snafu(display("invalid Flight ticket: {}", message))]
    InvalidTicket { message: String },
    #[snafu(display("internal error: {}", message))]
    Internal { message: String },
    #[snafu(transparent)]
    Arrow {
        source: datafusion::common::arrow::error::ArrowError,
    },
    #[snafu(transparent)]
    ClusterMetadata {
        source: wings_control_plane_core::cluster_metadata::ClusterMetadataError,
    },
    #[snafu(transparent)]
    DataFusion {
        source: datafusion::error::DataFusionError,
    },
    #[snafu(transparent)]
    Flight {
        source: arrow_flight::error::FlightError,
    },
    #[snafu(transparent)]
    Ingestor {
        source: wings_ingestor_core::IngestorError,
    },
    #[snafu(transparent)]
    Query {
        source: wings_query::TableLogicalPlanError,
    },
}

impl FlightServerError {
    pub fn invalid_ticket(message: impl Into<String>) -> Self {
        Self::InvalidTicket {
            message: message.into(),
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal {
            message: message.into(),
        }
    }
}

impl ErrorExt for FlightServerError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::InvalidTicket { .. } => StatusCode::InvalidArgument,
            Self::ClusterMetadata { source } => source.status_code(),
            _ => StatusCode::Internal,
        }
    }
}

impl From<FlightServerError> for tonic::Status {
    fn from(err: FlightServerError) -> Self {
        let status_code = err.status_code();
        tonic::Status::new(status_code.to_tonic_code(), err.to_string())
    }
}
