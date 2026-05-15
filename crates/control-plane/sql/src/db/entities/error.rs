use sea_orm::DbErr;
use snafu::Snafu;
use wings_control_plane_core::{
    ClusterMetadataError, log_metadata::LogMetadataError, pb::WireError,
};
use wings_resources::ResourceError;
use wings_schema::SchemaError;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("{resource} {name} not found"))]
    NotFound {
        resource: &'static str,
        name: String,
    },
    #[snafu(display("invalid {resource} name: {source}"))]
    InvalidResourceName {
        resource: &'static str,
        source: ResourceError,
    },
    #[snafu(display("internal error: {message}"))]
    Internal { message: String },
    #[snafu(transparent)]
    Schema { source: SchemaError },
    #[snafu(transparent)]
    Wire { source: WireError },
    #[snafu(transparent)]
    Prost { source: prost::DecodeError },
    #[snafu(transparent)]
    Json { source: serde_json::Error },
    #[snafu(transparent)]
    Db { source: DbErr },
}

impl From<Error> for ClusterMetadataError {
    fn from(err: Error) -> Self {
        match err {
            Error::NotFound { resource, name } => ClusterMetadataError::NotFound {
                resource: resource.to_string(),
                name,
            },
            Error::InvalidResourceName { resource, source } => {
                ClusterMetadataError::InvalidResourceName {
                    resource: resource.to_string(),
                    message: source.to_string(),
                }
            }
            Error::Internal { message } => ClusterMetadataError::Internal { message },
            Error::Schema { source } => ClusterMetadataError::Schema {
                message: source.to_string(),
            },
            Error::Wire { source } => ClusterMetadataError::Internal {
                message: format!("wire error: {source}"),
            },
            Error::Prost { source } => ClusterMetadataError::Internal {
                message: format!("prost decode error: {source}"),
            },
            Error::Json { source } => ClusterMetadataError::Internal {
                message: format!("json error: {source}"),
            },
            Error::Db { source } => ClusterMetadataError::Internal {
                message: format!("db error: {source}"),
            },
        }
    }
}

impl From<Error> for LogMetadataError {
    fn from(err: Error) -> Self {
        match err {
            Error::NotFound { resource, name } => LogMetadataError::NotFound {
                resource: resource.to_string(),
                name,
            },
            Error::InvalidResourceName { resource, source } => {
                LogMetadataError::InvalidResourceName {
                    resource: resource.to_string(),
                    message: format!("{source}"),
                }
            }
            Error::Internal { message } => LogMetadataError::Internal { message },
            Error::Schema { source } => LogMetadataError::Schema {
                message: source.to_string(),
            },
            Error::Wire { source } => LogMetadataError::Internal {
                message: format!("wire error: {source}"),
            },
            Error::Prost { source } => LogMetadataError::Internal {
                message: format!("prost decode error: {source}"),
            },
            Error::Json { source } => LogMetadataError::Internal {
                message: format!("json error: {source}"),
            },
            Error::Db { source } => LogMetadataError::Internal {
                message: format!("db error: {source}"),
            },
        }
    }
}
