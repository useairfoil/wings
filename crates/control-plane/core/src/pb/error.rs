use snafu::Snafu;
use wings_observability::{ErrorExt, StatusCode};
use wings_resources::ResourceError;
use wings_schema::SchemaError;

/// Represents an error that occurred while serializing or deserializing a protobuf message.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum WireError {
    #[snafu(display("missing field: {}", field_name))]
    MissingField { field_name: String },
    #[snafu(display("unspecified time unit: got {value}"))]
    UnspecifiedTimeUnit { value: i32 },
    #[snafu(display("invalid {resource}"))]
    Resource {
        resource: &'static str,
        source: ResourceError,
    },
    #[snafu(display("unspecified {enum} received"))]
    Unspecified { r#enum: &'static str },
    #[snafu(display("{type} value out of range: {value}"))]
    ValueOutOfRange { r#type: &'static str, value: String },
    #[snafu(transparent)]
    Timestamp { source: prost_types::TimestampError },
    #[snafu(transparent)]
    Duration { source: prost_types::DurationError },
    #[snafu(transparent)]
    Schema { source: SchemaError },
}

pub type Result<T, E = WireError> = std::result::Result<T, E>;

impl ErrorExt for WireError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::Schema { source } => source.status_code(),
            _ => StatusCode::Internal,
        }
    }
}
