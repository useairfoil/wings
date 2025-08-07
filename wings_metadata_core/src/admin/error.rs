use datafusion::error::DataFusionError;
use snafu::Snafu;

use crate::resource::ResourceError;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum AdminError {
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

pub type AdminResult<T, E = AdminError> = ::std::result::Result<T, E>;

impl From<AdminError> for DataFusionError {
    fn from(err: AdminError) -> Self {
        DataFusionError::External(Box::new(err))
    }
}

impl AdminError {
    pub fn is_not_found(&self) -> bool {
        matches!(self, AdminError::NotFound { .. })
    }
}
