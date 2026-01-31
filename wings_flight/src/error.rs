use snafu::Snafu;
use wings_observability::ErrorKind;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum FlightServerError {
    #[snafu(transparent)]
    Arrow {
        source: datafusion::common::arrow::error::ArrowError,
    },
    #[snafu(transparent)]
    DataFusion {
        source: datafusion::error::DataFusionError,
    },
    #[snafu(transparent)]
    Flight {
        source: arrow_flight::error::FlightError,
    },
    #[snafu(display("Invalid Flight ticket: {}", message))]
    InvalidTicket { message: String },
}

impl FlightServerError {
    pub fn invalid_ticket(message: impl Into<String>) -> Self {
        Self::InvalidTicket {
            message: message.into(),
        }
    }
}

impl FlightServerError {
    pub fn kind(&self) -> ErrorKind {
        match self {
            Self::Arrow { .. } | Self::DataFusion { .. } | Self::Flight { .. } => {
                ErrorKind::Temporary
            }
            Self::InvalidTicket { .. } => ErrorKind::Validation,
        }
    }
}

impl From<FlightServerError> for tonic::Status {
    fn from(err: FlightServerError) -> Self {
        let code = match err.kind() {
            ErrorKind::Validation => tonic::Code::InvalidArgument,
            ErrorKind::NotFound => tonic::Code::NotFound,
            _ => tonic::Code::Internal,
        };
        let message = err.to_string();
        tonic::Status::new(code, message)
    }
}
