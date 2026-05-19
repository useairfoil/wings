use wings_resources::ResourceError;

use crate::{ClusterMetadataError, table_metadata::TableMetadataError};

pub trait ResourceErrorExt {
    fn to_cluster_metadata_error(&self, resource: &'static str) -> ClusterMetadataError;
    fn to_table_metadata_error(&self, resource: &'static str) -> TableMetadataError;
}

impl ResourceErrorExt for ResourceError {
    fn to_cluster_metadata_error(&self, resource: &'static str) -> ClusterMetadataError {
        ClusterMetadataError::InvalidResourceName {
            resource: resource.to_string(),
            message: self.to_string(),
        }
    }

    fn to_table_metadata_error(&self, resource: &'static str) -> TableMetadataError {
        TableMetadataError::InvalidResourceName {
            resource: resource.to_string(),
            message: self.to_string(),
        }
    }
}
