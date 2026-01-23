use std::{array::TryFromSliceError, string::FromUtf8Error};

use snafu::Snafu;

use crate::{ErrorKind, schema::DataType};

#[derive(Debug, Snafu)]
pub enum SchemaError {
    #[snafu(display("Conversion error: {message}"))]
    ConversionError { message: String },
    #[snafu(display("Duplicate field id {id}: {f1_name} - {f2_name}"))]
    DuplicateFieldId {
        id: u64,
        f1_name: String,
        f2_name: String,
    },
    #[snafu(display("Failed to deserialize number"))]
    NumberDeserializationError { source: TryFromSliceError },
    #[snafu(display("Failed to deserialize string"))]
    StringDeserializationError { source: FromUtf8Error },
    #[snafu(display("Unsupported data type: {data_type}"))]
    UnsupportedDataType { data_type: DataType },
}

pub type Result<T, E = SchemaError> = std::result::Result<T, E>;

impl SchemaError {
    pub fn kind(&self) -> ErrorKind {
        match self {
            Self::ConversionError { .. } => ErrorKind::Validation,
            Self::DuplicateFieldId { .. } => ErrorKind::Validation,
            Self::UnsupportedDataType { .. } => ErrorKind::Validation,
            Self::NumberDeserializationError { .. } => ErrorKind::Internal,
            Self::StringDeserializationError { .. } => ErrorKind::Internal,
        }
    }
}

impl From<TryFromSliceError> for SchemaError {
    fn from(source: TryFromSliceError) -> Self {
        Self::NumberDeserializationError { source }
    }
}

impl From<FromUtf8Error> for SchemaError {
    fn from(source: FromUtf8Error) -> Self {
        Self::StringDeserializationError { source }
    }
}

impl From<SchemaError> for ErrorKind {
    fn from(error: SchemaError) -> Self {
        error.kind()
    }
}
