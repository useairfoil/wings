use snafu::Snafu;

/// CLI error types.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum CliError {}

pub type Result<T, E = CliError> = std::result::Result<T, E>;
