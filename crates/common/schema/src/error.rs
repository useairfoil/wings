use std::{array::TryFromSliceError, string::FromUtf8Error};

use snafu::Snafu;
use wings_observability::{ErrorExt, StatusCode};

use crate::DataType;

#[derive(Debug, Snafu)]
pub enum SchemaError {
    #[snafu(display("duplicate field id {id}: {f1_name} - {f2_name}"))]
    DuplicateFieldId {
        id: u64,
        f1_name: String,
        f2_name: String,
    },
    #[snafu(display("failed to deserialize number"))]
    NumberDeserializationError { source: TryFromSliceError },
    #[snafu(display("failed to deserialize string"))]
    StringDeserializationError { source: FromUtf8Error },
    #[snafu(display("unsupported data type: {data_type}"))]
    UnsupportedDataType { data_type: DataType },
}

pub type Result<T, E = SchemaError> = std::result::Result<T, E>;

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

impl ErrorExt for SchemaError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::DuplicateFieldId { .. } => StatusCode::DuplicateField,
            Self::NumberDeserializationError { .. } => StatusCode::Schema,
            Self::StringDeserializationError { .. } => StatusCode::Schema,
            Self::UnsupportedDataType { .. } => StatusCode::DataType,
        }
    }
}
