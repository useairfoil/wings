use parquet::errors::ParquetError;
use snafu::Snafu;

use crate::{ErrorKind, parquet::parquet_name_index::DuplicateFieldNameError};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(transparent)]
    Parquet { source: ParquetError },
    #[snafu(transparent)]
    Iceberg { source: iceberg::Error },
    #[snafu(transparent)]
    DuplicateFieldName { source: DuplicateFieldNameError },
    #[snafu(display("Statistics type did not match field type: {field_name} (id={field_id})"))]
    InvalidStatisticsType { field_id: u64, field_name: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    pub fn kind(&self) -> ErrorKind {
        ErrorKind::Internal
    }
}
