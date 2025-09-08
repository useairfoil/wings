use snafu::Snafu;

use crate::resources::ResourceError;

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
    #[snafu(display("internal error: {message}"))]
    Internal { message: String },
}

pub type Result<T, E = ClusterMetadataError> = ::std::result::Result<T, E>;

impl ClusterMetadataError {
    pub fn is_not_found(&self) -> bool {
        matches!(self, ClusterMetadataError::NotFound { .. })
    }

    pub fn is_already_exists(&self) -> bool {
        matches!(self, ClusterMetadataError::AlreadyExists { .. })
    }

    pub fn is_invalid_argument(&self) -> bool {
        matches!(self, ClusterMetadataError::InvalidArgument { .. })
    }

    pub fn is_invalid_resource_name(&self) -> bool {
        matches!(self, ClusterMetadataError::InvalidResourceName { .. })
    }

    pub fn is_internal(&self) -> bool {
        matches!(self, ClusterMetadataError::Internal { .. })
    }
}
