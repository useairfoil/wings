use serde::{Deserialize, Serialize};

use crate::redacted::REDACTED_FIELD_VALUE;

/// Object store configuration.
///
/// Different cloud providers require different object store configurations.
/// This enum represents the various supported object store types.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

impl ObjectStoreConfiguration {
    pub fn into_redacted(self) -> Self {
        match self {
            ObjectStoreConfiguration::Aws(c) => c.into_redacted().into(),
            ObjectStoreConfiguration::Azure(c) => c.into_redacted().into(),
            ObjectStoreConfiguration::Google(c) => c.into_redacted().into(),
            ObjectStoreConfiguration::S3Compatible(c) => c.into_redacted().into(),
        }
    }
}

impl AwsConfiguration {
    pub fn into_redacted(self) -> Self {
        Self {
            bucket_name: self.bucket_name,
            prefix: self.prefix,
            access_key_id: REDACTED_FIELD_VALUE.to_string(),
            secret_access_key: REDACTED_FIELD_VALUE.to_string(),
            region: self.region,
        }
    }
}

impl AzureConfiguration {
    pub fn into_redacted(self) -> Self {
        Self {
            container_name: self.container_name,
            prefix: self.prefix,
            storage_account_name: REDACTED_FIELD_VALUE.to_string(),
            storage_account_key: REDACTED_FIELD_VALUE.to_string(),
        }
    }
}

impl GoogleConfiguration {
    pub fn into_redacted(self) -> Self {
        Self {
            bucket_name: self.bucket_name,
            prefix: self.prefix,
            service_account: REDACTED_FIELD_VALUE.to_string(),
            service_account_key: REDACTED_FIELD_VALUE.to_string(),
        }
    }
}

impl S3CompatibleConfiguration {
    pub fn into_redacted(self) -> Self {
        Self {
            bucket_name: self.bucket_name,
            prefix: self.prefix,
            access_key_id: REDACTED_FIELD_VALUE.to_string(),
            secret_access_key: REDACTED_FIELD_VALUE.to_string(),
            endpoint: self.endpoint,
            region: self.region,
            allow_http: self.allow_http,
        }
    }
}

impl From<AwsConfiguration> for ObjectStoreConfiguration {
    fn from(c: AwsConfiguration) -> Self {
        ObjectStoreConfiguration::Aws(c)
    }
}

impl From<AzureConfiguration> for ObjectStoreConfiguration {
    fn from(c: AzureConfiguration) -> Self {
        ObjectStoreConfiguration::Azure(c)
    }
}

impl From<GoogleConfiguration> for ObjectStoreConfiguration {
    fn from(c: GoogleConfiguration) -> Self {
        ObjectStoreConfiguration::Google(c)
    }
}

impl From<S3CompatibleConfiguration> for ObjectStoreConfiguration {
    fn from(c: S3CompatibleConfiguration) -> Self {
        ObjectStoreConfiguration::S3Compatible(c)
    }
}
