use snafu::Snafu;

/// Errors that can occur in the HTTP server.
#[derive(Debug, Snafu)]
pub enum HttpServerError {}

pub type Result<T, E = HttpServerError> = std::result::Result<T, E>;
