use datafusion::error::DataFusionError;
use prost::Message;
use snafu::Snafu;
use wings_observability::{ErrorExt, StatusCode};

use crate::pb::WireError;

/// Represents errors that can occur while interacting with cluster metadata.
///
/// Notice that this error type does not propagate errors since it's intended to be used
/// at the edge between the server and the client.
/// Server modules should have their own error types that are converted into this type.
#[derive(Debug, Snafu)]
pub enum ClusterMetadataError {
    #[snafu(display("{resource} not found: {name}"))]
    NotFound { resource: String, name: String },
    #[snafu(display("{resource} already exists: {name}"))]
    AlreadyExists { resource: String, name: String },
    #[snafu(display("invalid {resource} argument: {message}"))]
    InvalidArgument { resource: String, message: String },
    #[snafu(display("invalid {resource} name: {message}"))]
    InvalidResourceName { resource: String, message: String },
    #[snafu(display("failed precondition: {message}"))]
    FailedPrecondition { message: String },
    #[snafu(display("schema error: {message}"))]
    Schema { message: String },
    #[snafu(display("internal error: {message}"))]
    Internal { message: String },
}

pub type Result<T, E = ClusterMetadataError> = ::std::result::Result<T, E>;

/// Convert `ClusterMetadataError` into protobuf `Any` for use in gRPC responses.
mod details {
    use prost::{DecodeError, Message};
    use prost_types::Any;

    use super::ClusterMetadataError;

    #[derive(Message)]
    pub struct NotFound {
        #[prost(string, tag = "1")]
        pub resource: String,
        #[prost(string, tag = "2")]
        pub name: String,
    }

    impl NotFound {
        pub const TYPE_URL: &'static str = "type.googleapis.com/wings.cluster_metadata.NotFound";

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

    impl From<NotFound> for ClusterMetadataError {
        fn from(err: NotFound) -> Self {
            ClusterMetadataError::NotFound {
                resource: err.resource,
                name: err.name,
            }
        }
    }

    #[derive(Message)]
    pub struct AlreadyExists {
        #[prost(string, tag = "1")]
        pub resource: String,
        #[prost(string, tag = "2")]
        pub name: String,
    }

    impl AlreadyExists {
        pub const TYPE_URL: &'static str =
            "type.googleapis.com/wings.cluster_metadata.AlreadyExists";

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

    impl From<AlreadyExists> for ClusterMetadataError {
        fn from(err: AlreadyExists) -> Self {
            ClusterMetadataError::AlreadyExists {
                resource: err.resource,
                name: err.name,
            }
        }
    }

    #[derive(Message)]
    pub struct InvalidArgument {
        #[prost(string, tag = "1")]
        pub resource: String,
        #[prost(string, tag = "2")]
        pub message: String,
    }

    impl InvalidArgument {
        pub const TYPE_URL: &'static str =
            "type.googleapis.com/wings.cluster_metadata.InvalidArgument";

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

    impl From<InvalidArgument> for ClusterMetadataError {
        fn from(err: InvalidArgument) -> Self {
            ClusterMetadataError::InvalidArgument {
                resource: err.resource,
                message: err.message,
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
            "type.googleapis.com/wings.cluster_metadata.InvalidResourceName";

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

    impl From<InvalidResourceName> for ClusterMetadataError {
        fn from(err: InvalidResourceName) -> Self {
            ClusterMetadataError::InvalidResourceName {
                resource: err.resource,
                message: err.message,
            }
        }
    }

    #[derive(Message)]
    pub struct FailedPrecondition {
        #[prost(string, tag = "1")]
        pub message: String,
    }

    impl FailedPrecondition {
        pub const TYPE_URL: &'static str =
            "type.googleapis.com/wings.cluster_metadata.FailedPrecondition";

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

    impl From<FailedPrecondition> for ClusterMetadataError {
        fn from(err: FailedPrecondition) -> Self {
            ClusterMetadataError::FailedPrecondition {
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
        pub const TYPE_URL: &'static str = "type.googleapis.com/wings.cluster_metadata.Schema";

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

    impl From<Schema> for ClusterMetadataError {
        fn from(err: Schema) -> Self {
            ClusterMetadataError::Schema {
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
        pub const TYPE_URL: &'static str = "type.googleapis.com/wings.cluster_metadata.Internal";

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

    impl From<Internal> for ClusterMetadataError {
        fn from(err: Internal) -> Self {
            ClusterMetadataError::Internal {
                message: err.message,
            }
        }
    }
}

impl ClusterMetadataError {
    /// Returns whether the error is a not found error.
    pub fn is_not_found(&self) -> bool {
        matches!(self, ClusterMetadataError::NotFound { .. })
    }
}

impl From<ClusterMetadataError> for DataFusionError {
    fn from(err: ClusterMetadataError) -> Self {
        DataFusionError::External(Box::new(err))
    }
}

impl ErrorExt for ClusterMetadataError {
    fn status_code(&self) -> StatusCode {
        match self {
            ClusterMetadataError::NotFound { .. } => StatusCode::NotFound,
            ClusterMetadataError::AlreadyExists { .. } => StatusCode::AlreadyExists,
            ClusterMetadataError::InvalidArgument { .. } => StatusCode::InvalidArgument,
            ClusterMetadataError::InvalidResourceName { .. } => StatusCode::ResourceName,
            ClusterMetadataError::FailedPrecondition { .. } => StatusCode::FailedPrecondition,
            ClusterMetadataError::Schema { .. } => StatusCode::Schema,
            ClusterMetadataError::Internal { .. } => StatusCode::Internal,
        }
    }
}

impl From<tonic::Status> for ClusterMetadataError {
    fn from(status: tonic::Status) -> Self {
        use prost_types::Any;
        let Ok(details) = Any::decode(status.details()) else {
            return ClusterMetadataError::Internal {
                message: format!("unknown gRPC error {}: {}", status.code(), status.message()),
            };
        };

        match details.type_url.as_str() {
            details::NotFound::TYPE_URL => details::NotFound::from_any(details).map(Into::into),
            details::AlreadyExists::TYPE_URL => {
                details::AlreadyExists::from_any(details).map(Into::into)
            }
            details::InvalidArgument::TYPE_URL => {
                details::InvalidArgument::from_any(details).map(Into::into)
            }
            details::InvalidResourceName::TYPE_URL => {
                details::InvalidResourceName::from_any(details).map(Into::into)
            }
            details::FailedPrecondition::TYPE_URL => {
                details::FailedPrecondition::from_any(details).map(Into::into)
            }
            details::Schema::TYPE_URL => details::Schema::from_any(details).map(Into::into),
            details::Internal::TYPE_URL => details::Internal::from_any(details).map(Into::into),
            _ => {
                return ClusterMetadataError::Internal {
                    message: format!("unknown error type {}", details.type_url),
                };
            }
        }
        .unwrap_or_else(|err| ClusterMetadataError::Internal {
            message: format!("failed to decode error details: {err}"),
        })
    }
}

impl From<ClusterMetadataError> for tonic::Status {
    fn from(err: ClusterMetadataError) -> Self {
        use prost::Message;

        let code = err.status_code().to_tonic_code();
        let message = err.to_string();
        let details = match err {
            ClusterMetadataError::NotFound { resource, name } => {
                details::NotFound { resource, name }.into_any()
            }
            ClusterMetadataError::AlreadyExists { resource, name } => {
                details::AlreadyExists { resource, name }.into_any()
            }
            ClusterMetadataError::InvalidArgument { message, resource } => {
                details::InvalidArgument { resource, message }.into_any()
            }
            ClusterMetadataError::InvalidResourceName { message, resource } => {
                details::InvalidResourceName { resource, message }.into_any()
            }
            ClusterMetadataError::FailedPrecondition { message } => {
                details::FailedPrecondition { message }.into_any()
            }
            ClusterMetadataError::Schema { message } => details::Schema { message }.into_any(),
            ClusterMetadataError::Internal { message } => details::Internal { message }.into_any(),
        };

        tonic::Status::with_details(code, message, details.encode_to_vec().into())
    }
}

impl From<WireError> for ClusterMetadataError {
    fn from(err: WireError) -> Self {
        Self::Internal {
            message: err.to_string(),
        }
    }
}
