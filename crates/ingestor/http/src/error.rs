use snafu::Snafu;

/// Errors that can occur in the HTTP ingestor.
#[derive(Debug, Snafu)]
pub enum HttpIngestorError {
    #[snafu(display("failed to bind to address: {address}"))]
    BindError { address: String },
    #[snafu(display("server error: {message}"))]
    ServerError { message: String },
    #[snafu(display("internal server error: {message}"))]
    Internal { message: String },
    #[snafu(display("bad request: {message}"))]
    BadRequest { message: String },
    #[snafu(display("not found: {message}"))]
    NotFound { message: String },
}

pub type Result<T, E = HttpIngestorError> = std::result::Result<T, E>;
