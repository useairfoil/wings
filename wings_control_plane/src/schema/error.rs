use snafu::Snafu;

use crate::ErrorKind;

#[derive(Debug, Snafu)]
pub enum SchemaError {
    #[snafu(display("Conversion error: {message}"))]
    ConversionError { message: String },
}

pub type Result<T> = std::result::Result<T, SchemaError>;

impl SchemaError {
    pub fn kind(&self) -> ErrorKind {
        match self {
            Self::ConversionError { .. } => ErrorKind::Validation,
        }
    }
}
