use sea_orm::DbErr;
use snafu::Snafu;
use wings_control_plane_core::ClusterMetadataError;
use wings_resources::ResourceError;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
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
    #[snafu(transparent)]
    Json { source: serde_json::Error },
    #[snafu(transparent)]
    Orm { source: DbErr },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<Error> for ClusterMetadataError {
    fn from(err: Error) -> Self {
        match err {
            Error::AlreadyExists { resource, message } => {
                ClusterMetadataError::AlreadyExists { resource, message }
            }
            Error::InvalidArgument { resource, message } => {
                ClusterMetadataError::InvalidArgument { resource, message }
            }
            Error::InvalidResourceName { resource, source } => {
                ClusterMetadataError::InvalidResourceName { resource, source }
            }
            Error::Json { source } => ClusterMetadataError::Internal {
                message: format!("json error: {source}"),
            },
            Error::Orm { source } => ClusterMetadataError::Internal {
                message: format!("db error: {source}"),
            },
        }
    }
}
