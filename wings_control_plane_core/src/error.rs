use wings_resources::ResourceError;

use crate::{ClusterMetadataError, log_metadata::LogMetadataError};

pub trait ResourceErrorExt {
    fn to_cluster_metadata_error(&self, resource: &'static str) -> ClusterMetadataError;
    fn to_log_metadata_error(&self, resource: &'static str) -> LogMetadataError;
}

impl ResourceErrorExt for ResourceError {
    fn to_cluster_metadata_error(&self, resource: &'static str) -> ClusterMetadataError {
        ClusterMetadataError::InvalidResourceName {
            resource: resource.to_string(),
            message: self.to_string(),
        }
    }

    fn to_log_metadata_error(&self, resource: &'static str) -> LogMetadataError {
        LogMetadataError::InvalidResourceName {
            resource: resource.to_string(),
            message: self.to_string(),
        }
    }
}
