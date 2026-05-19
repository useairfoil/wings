use datafusion::error::DataFusionError;
use prost::Message;
use snafu::Snafu;
use wings_observability::{ErrorExt, StatusCode};

use crate::pb::WireError;

/// Errors related to log metadata operations.
#[derive(Clone, Debug, Snafu)]
pub enum TableMetadataError {
    #[snafu(display("{resource} not found: {name}"))]
    NotFound { resource: String, name: String },
    #[snafu(display("invalid {resource} name: {message}"))]
    InvalidResourceName { resource: String, message: String },
    #[snafu(display("invalid argument: {message}"))]
    InvalidArgument { message: String },
    #[snafu(display("schema error: {message}"))]
    Schema { message: String },
    #[snafu(display("internal error: {message}"))]
    Internal { message: String },
}

pub type Result<T, E = TableMetadataError> = ::std::result::Result<T, E>;

/// Convert `TableMetadataError` into protobuf `Any` for use in gRPC responses.
mod details {
    use prost::{DecodeError, Message};
    use prost_types::Any;

    use super::TableMetadataError;

    #[derive(Message)]
    pub struct NotFound {
        #[prost(string, tag = "1")]
        pub resource: String,
        #[prost(string, tag = "2")]
        pub name: String,
    }

    impl NotFound {
        pub const TYPE_URL: &'static str = "type.googleapis.com/wings.table_metadata.NotFound";

        pub fn into_any(self) -> Any {
            let value = self.encode_to_vec();
            Any {
                type_url: Self::TYPE_URL.to_string(),
                value,
            }
        }

        pub fn from_any(any: Any) -> Result<Self, DecodeError> {
            Self::decode(any.value.as_slice())
        }
    }

    impl From<NotFound> for TableMetadataError {
        fn from(err: NotFound) -> Self {
            TableMetadataError::NotFound {
                resource: err.resource,
                name: err.name,
            }
        }
    }

    #[derive(Message)]
    pub struct InvalidResourceName {
        #[prost(string, tag = "1")]
        pub resource: String,
        #[prost(string, tag = "2")]
        pub message: String,
    }

    impl InvalidResourceName {
        pub const TYPE_URL: &'static str =
            "type.googleapis.com/wings.table_metadata.InvalidResourceName";

        pub fn into_any(self) -> Any {
            let value = self.encode_to_vec();
            Any {
                type_url: Self::TYPE_URL.to_string(),
                value,
            }
        }

        pub fn from_any(any: Any) -> Result<Self, DecodeError> {
            Self::decode(any.value.as_slice())
        }
    }

    impl From<InvalidResourceName> for TableMetadataError {
        fn from(err: InvalidResourceName) -> Self {
            TableMetadataError::InvalidResourceName {
                resource: err.resource,
                message: err.message,
            }
        }
    }

    #[derive(Message)]
    pub struct InvalidArgument {
        #[prost(string, tag = "1")]
        pub message: String,
    }

    impl InvalidArgument {
        pub const TYPE_URL: &'static str = "type.googleapis.com/wings.table_metadata.InvalidArgument";

        pub fn into_any(self) -> Any {
            let value = self.encode_to_vec();
            Any {
                type_url: Self::TYPE_URL.to_string(),
                value,
            }
        }

        pub fn from_any(any: Any) -> Result<Self, DecodeError> {
            Self::decode(any.value.as_slice())
        }
    }

    impl From<InvalidArgument> for TableMetadataError {
        fn from(err: InvalidArgument) -> Self {
            TableMetadataError::InvalidArgument {
                message: err.message,
            }
        }
    }

    #[derive(Message)]
    pub struct Schema {
        #[prost(string, tag = "1")]
        pub message: String,
    }

    impl Schema {
        pub const TYPE_URL: &'static str = "type.googleapis.com/wings.table_metadata.Schema";

        pub fn into_any(self) -> Any {
            let value = self.encode_to_vec();
            Any {
                type_url: Self::TYPE_URL.to_string(),
                value,
            }
        }

        pub fn from_any(any: Any) -> Result<Self, DecodeError> {
            Self::decode(any.value.as_slice())
        }
    }

    impl From<Schema> for TableMetadataError {
        fn from(err: Schema) -> Self {
            TableMetadataError::Schema {
                message: err.message,
            }
        }
    }

    #[derive(Message)]
    pub struct Internal {
        #[prost(string, tag = "1")]
        pub message: String,
    }

    impl Internal {
        pub const TYPE_URL: &'static str = "type.googleapis.com/wings.table_metadata.Internal";

        pub fn into_any(self) -> Any {
            let value = self.encode_to_vec();
            Any {
                type_url: Self::TYPE_URL.to_string(),
                value,
            }
        }

        pub fn from_any(any: Any) -> Result<Self, DecodeError> {
            Self::decode(any.value.as_slice())
        }
    }

    impl From<Internal> for TableMetadataError {
        fn from(err: Internal) -> Self {
            TableMetadataError::Internal {
                message: err.message,
            }
        }
    }
}

impl From<TableMetadataError> for DataFusionError {
    fn from(err: TableMetadataError) -> Self {
        DataFusionError::External(Box::new(err))
    }
}

impl ErrorExt for TableMetadataError {
    fn status_code(&self) -> StatusCode {
        match self {
            TableMetadataError::NotFound { .. } => StatusCode::NotFound,
            TableMetadataError::InvalidArgument { .. } => StatusCode::InvalidArgument,
            TableMetadataError::InvalidResourceName { .. } => StatusCode::ResourceName,
            TableMetadataError::Schema { .. } => StatusCode::Schema,
            TableMetadataError::Internal { .. } => StatusCode::Internal,
        }
    }
}

impl From<tonic::Status> for TableMetadataError {
    fn from(status: tonic::Status) -> Self {
        use prost_types::Any;
        let Ok(details) = Any::decode(status.details()) else {
            return TableMetadataError::Internal {
                message: "failed to decode error details".to_string(),
            };
        };

        match details.type_url.as_str() {
            details::NotFound::TYPE_URL => details::NotFound::from_any(details).map(Into::into),
            details::InvalidArgument::TYPE_URL => {
                details::InvalidArgument::from_any(details).map(Into::into)
            }
            details::InvalidResourceName::TYPE_URL => {
                details::InvalidResourceName::from_any(details).map(Into::into)
            }
            details::Schema::TYPE_URL => details::Schema::from_any(details).map(Into::into),
            details::Internal::TYPE_URL => details::Internal::from_any(details).map(Into::into),
            _ => {
                return TableMetadataError::Internal {
                    message: format!("unknown error type {}", details.type_url),
                };
            }
        }
        .unwrap_or_else(|err| TableMetadataError::Internal {
            message: format!("failed to decode error details: {err}"),
        })
    }
}

impl From<TableMetadataError> for tonic::Status {
    fn from(err: TableMetadataError) -> Self {
        use prost::Message;

        let code = err.status_code().to_tonic_code();
        let message = err.to_string();
        let details = match err {
            TableMetadataError::NotFound { resource, name } => {
                details::NotFound { resource, name }.into_any()
            }
            TableMetadataError::InvalidResourceName { message, resource } => {
                details::InvalidResourceName { resource, message }.into_any()
            }
            TableMetadataError::InvalidArgument { message } => {
                details::InvalidArgument { message }.into_any()
            }
            TableMetadataError::Schema { message } => details::Schema { message }.into_any(),
            TableMetadataError::Internal { message } => details::Internal { message }.into_any(),
        };

        tonic::Status::with_details(code, message, details.encode_to_vec().into())
    }
}

impl From<WireError> for TableMetadataError {
    fn from(err: WireError) -> Self {
        Self::Internal {
            message: err.to_string(),
        }
    }
}
