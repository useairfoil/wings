use snafu::Snafu;
use wings_observability::{ErrorExt, StatusCode};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("invalid Flight ticket: {}", message))]
    InvalidTicket { message: String },
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
}

impl Error {
    pub fn invalid_ticket(message: impl Into<String>) -> Self {
        Self::InvalidTicket {
            message: message.into(),
        }
    }
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::InvalidTicket { .. } => StatusCode::InvalidArgument,
            _ => StatusCode::Internal,
        }
    }
}

impl From<Error> for tonic::Status {
    fn from(err: Error) -> Self {
        let status_code = err.status_code();
        tonic::Status::new(status_code.to_tonic_code(), err.to_string())
    }
}
