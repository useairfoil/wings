use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum SchemaError {
    #[snafu(display("Conversion error: {message}"))]
    ConversionError { message: String },
}

pub type Result<T> = std::result::Result<T, SchemaError>;
