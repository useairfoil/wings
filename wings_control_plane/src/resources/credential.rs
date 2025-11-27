use std::sync::Arc;

use datafusion::{error::DataFusionError, execution::object_store::ObjectStoreUrl};

use crate::resource_type;

use super::tenant::TenantName;

resource_type!(Credential, "credentials", Tenant);

/// Credential configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Credential {
    pub name: CredentialName,
    pub object_store: ObjectStoreConfiguration,
}

/// Object store configuration.
///
/// Different cloud providers require different credential types.
/// This enum represents the various supported credentials.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectStoreConfiguration {
    /// AWS S3 credentials.
    Aws(AwsConfiguration),
    /// Azure Blob Storage credentials.
    Azure(AzureConfiguration),
    /// Google Cloud Storage credentials.
    Google(GoogleConfiguration),
    /// S3-compatible storage credentials.
    S3Compatible(S3CompatibleConfiguration),
}

/// AWS S3 credentials.
#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct AwsConfiguration {}

/// Azure Blob Storage credentials.
#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct AzureConfiguration {}

/// Google Cloud Storage credentials.
#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct GoogleConfiguration {}

/// S3-compatible storage credentials.
#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct S3CompatibleConfiguration {}

pub type CredentialRef = Arc<Credential>;

impl CredentialName {
    /// Returns the URL of the object store associated with this credential.
    ///
    /// Notice that this URL is only necessary for registering the client with DataFusion.
    pub fn wings_object_store_url(&self) -> Result<ObjectStoreUrl, DataFusionError> {
        ObjectStoreUrl::parse(format!("wings://{}", self.id))
    }
}
