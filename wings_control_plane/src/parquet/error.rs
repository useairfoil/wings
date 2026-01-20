use parquet::errors::ParquetError;
use snafu::Snafu;

use crate::ErrorKind;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(transparent)]
    Parquet { source: ParquetError },
    #[snafu(transparent)]
    Iceberg { source: iceberg::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    pub fn kind(&self) -> ErrorKind {
        ErrorKind::Internal
    }
}
