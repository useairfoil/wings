use datafusion::error::DataFusionError;
use snafu::Snafu;

use crate::{ErrorKind, resources::ResourceError, schema::SchemaError};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum ClusterMetadataError {
    #[snafu(display("{resource} not found: {message}"))]
    NotFound {
        resource: &'static str,
        message: String,
    },
    #[snafu(display("{resource} already exists: {message}"))]
    AlreadyExists {
        resource: &'static str,
        message: String,
    },
    #[snafu(display("invalid {resource} argument: {message}"))]
    InvalidArgument {
        resource: &'static str,
        message: String,
    },
    #[snafu(display("invalid {resource} name"))]
    InvalidResourceName {
        resource: &'static str,
        source: ResourceError,
    },
    #[snafu(display("invalid schema"))]
    Schema { source: SchemaError },
    #[snafu(display("internal error: {message}"))]
    Internal { message: String },
}

pub type Result<T, E = ClusterMetadataError> = ::std::result::Result<T, E>;

impl ClusterMetadataError {
    pub fn kind(&self) -> ErrorKind {
        match self {
            Self::NotFound { .. } => ErrorKind::NotFound,
            Self::AlreadyExists { .. } => ErrorKind::Conflict,
            Self::InvalidArgument { .. }
            | Self::InvalidResourceName { .. }
            | Self::Schema { .. } => ErrorKind::Validation,
            Self::Internal { .. } => ErrorKind::Internal,
        }
    }

    pub fn is_not_found(&self) -> bool {
        matches!(self, Self::NotFound { .. })
    }

    pub fn is_already_exists(&self) -> bool {
        matches!(self, Self::AlreadyExists { .. })
    }

    pub fn is_invalid_argument(&self) -> bool {
        matches!(self, Self::InvalidArgument { .. })
    }

    pub fn is_invalid_resource_name(&self) -> bool {
        matches!(self, Self::InvalidResourceName { .. })
    }

    pub fn is_schema(&self) -> bool {
        matches!(self, Self::Schema { .. })
    }

    pub fn is_internal(&self) -> bool {
        matches!(self, Self::Internal { .. })
    }
}

impl From<ClusterMetadataError> for DataFusionError {
    fn from(err: ClusterMetadataError) -> Self {
        DataFusionError::External(Box::new(err))
    }
}
