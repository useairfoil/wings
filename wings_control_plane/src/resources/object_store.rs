use std::sync::Arc;

use datafusion::{error::DataFusionError, execution::object_store::ObjectStoreUrl};

use crate::resource_type;

use super::tenant::TenantName;

resource_type!(ObjectStore, "object-stores", Tenant);

/// Object store configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectStore {
    pub name: ObjectStoreName,
    pub object_store: ObjectStoreConfiguration,
}

/// Object store configuration.
///
/// Different cloud providers require different object store configurations.
/// This enum represents the various supported object store types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectStoreConfiguration {
    /// AWS S3 object store configuration.
    Aws(AwsConfiguration),
    /// Azure Blob Storage object store configuration.
    Azure(AzureConfiguration),
    /// Google Cloud Storage object store configuration.
    Google(GoogleConfiguration),
    /// S3-compatible storage object store configuration.
    S3Compatible(S3CompatibleConfiguration),
}

/// AWS S3 object store configuration.
#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct AwsConfiguration {}

/// Azure Blob Storage object store configuration.
#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct AzureConfiguration {}

/// Google Cloud Storage object store configuration.
#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct GoogleConfiguration {}

/// S3-compatible storage object store configuration.
#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct S3CompatibleConfiguration {}

pub type ObjectStoreRef = Arc<ObjectStore>;

impl ObjectStoreName {
    /// Returns the URL of the object store associated with this object store configuration.
    ///
    /// Notice that this URL is only necessary for registering the client with DataFusion.
    pub fn wings_object_store_url(&self) -> Result<ObjectStoreUrl, DataFusionError> {
        ObjectStoreUrl::parse(format!("wings://{}", self.id))
    }
}
