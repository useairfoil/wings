use std::sync::Arc;

use datafusion::{error::DataFusionError, execution::object_store::ObjectStoreUrl};

use super::tenant::TenantName;
use crate::resource_type;

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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AwsConfiguration {
    /// Bucket name.
    pub bucket_name: String,
    /// Bucket prefix.
    pub prefix: Option<String>,
    /// `AWS_ACCESS_KEY_ID`
    pub access_key_id: String,
    /// `AWS_SECRET_ACCESS_KEY`
    pub secret_access_key: String,
    /// `AWS_DEFAULT_REGION`
    pub region: Option<String>,
}

/// Azure Blob Storage object store configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AzureConfiguration {
    /// Azure container name.
    pub container_name: String,
    /// Container prefix.
    pub prefix: Option<String>,
    /// `AZURE_STORAGE_ACCOUNT_NAME`
    pub storage_account_name: String,
    /// `AZURE_STORAGE_ACCOUNT_KEY`
    pub storage_account_key: String,
}

/// Google Cloud Storage object store configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoogleConfiguration {
    /// Bucket name.
    pub bucket_name: String,
    /// Bucket prefix.
    pub prefix: Option<String>,
    /// `GOOGLE_SERVICE_ACCOUNT`
    pub service_account: String,
    /// `GOOGLE_SERVICE_ACCOUNT_KEY`
    pub service_account_key: String,
}

/// S3-compatible storage object store configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3CompatibleConfiguration {
    /// Bucket name.
    pub bucket_name: String,
    /// Bucket prefix.
    pub prefix: Option<String>,
    /// `AWS_ACCESS_KEY_ID`
    pub access_key_id: String,
    /// `AWS_SECRET_ACCESS_KEY`
    pub secret_access_key: String,
    /// `AWS_ENDPOINT`
    pub endpoint: String,
    /// `AWS_DEFAULT_REGION`
    pub region: Option<String>,
    /// Allow HTTP connections.
    pub allow_http: bool,
}

pub type ObjectStoreRef = Arc<ObjectStore>;

impl ObjectStoreName {
    /// Returns the URL of the object store associated with this object store configuration.
    ///
    /// Notice that this URL is only necessary for registering the client with DataFusion.
    pub fn wings_object_store_url(&self) -> Result<ObjectStoreUrl, DataFusionError> {
        ObjectStoreUrl::parse(format!("wings://{}", self.id))
    }
}
