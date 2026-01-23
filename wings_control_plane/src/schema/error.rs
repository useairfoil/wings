use snafu::Snafu;

use crate::ErrorKind;

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
}

pub type Result<T> = std::result::Result<T, SchemaError>;

impl SchemaError {
    pub fn kind(&self) -> ErrorKind {
        match self {
            Self::ConversionError { .. } => ErrorKind::Validation,
            Self::DuplicateFieldId { .. } => ErrorKind::Validation,
        }
    }
}
