use snafu::Snafu;

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

impl From<FlightServerError> for tonic::Status {
    fn from(err: FlightServerError) -> Self {
        // For now they're all internal errors
        let code = tonic::Code::Internal;
        let message = err.to_string();
        tonic::Status::new(code, message)
    }
}
