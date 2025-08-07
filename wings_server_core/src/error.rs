use snafu::Snafu;

/// Error type for the wings_server_core crate.
#[derive(Debug, Snafu)]
pub enum ServerError {}

pub type Result<T, E = ServerError> = std::result::Result<T, E>;
