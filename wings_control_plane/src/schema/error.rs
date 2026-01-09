use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum ArrowTypeError {
    #[snafu(display("Missing required field: {field}"))]
    MissingRequiredField { field: String },

    #[snafu(display("Invalid data type conversion: {message}"))]
    InvalidDataType { message: String },

    #[snafu(display("Unknown enum variant for {name}: {value}"))]
    UnknownEnumVariant { name: String, value: i32 },

    #[snafu(display("Conversion error: {message}"))]
    ConversionError { message: String },
}

pub type Result<T> = std::result::Result<T, ArrowTypeError>;
