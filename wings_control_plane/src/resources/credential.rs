use std::sync::Arc;

use crate::resource_type;

use super::tenant::TenantName;

resource_type!(Credential, "credentials", Tenant);

/// Credential configuration.
///
/// Different cloud providers require different credential types.
/// This enum represents the various supported credentials.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Credential {
    /// AWS S3 credentials.
    AwsCredential(AwsCredential),
    /// Azure Blob Storage credentials.
    AzureCredential(AzureCredential),
    /// Google Cloud Storage credentials.
    GoogleCredential(GoogleCredential),
    /// S3-compatible storage credentials.
    S3CompatibleCredential(S3CompatibleCredential),
}

/// AWS S3 credentials.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AwsCredential {
    pub name: CredentialName,
}

/// Azure Blob Storage credentials.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AzureCredential {
    pub name: CredentialName,
}

/// Google Cloud Storage credentials.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoogleCredential {
    pub name: CredentialName,
}

/// S3-compatible storage credentials.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3CompatibleCredential {
    pub name: CredentialName,
}

pub type CredentialRef = Arc<Credential>;

impl Credential {
    /// Create a new AWS credential.
    pub fn aws(name: CredentialName) -> Self {
        Self::AwsCredential(AwsCredential { name })
    }

    /// Create a new Azure credential.
    pub fn azure(name: CredentialName) -> Self {
        Self::AzureCredential(AzureCredential { name })
    }

    /// Create a new Google credential.
    pub fn google(name: CredentialName) -> Self {
        Self::GoogleCredential(GoogleCredential { name })
    }

    /// Create a new S3-compatible credential.
    pub fn s3_compatible(name: CredentialName) -> Self {
        Self::S3CompatibleCredential(S3CompatibleCredential { name })
    }

    /// Get the name of this credential.
    pub fn name(&self) -> &CredentialName {
        match self {
            Self::AwsCredential(credential) => &credential.name,
            Self::AzureCredential(credential) => &credential.name,
            Self::GoogleCredential(credential) => &credential.name,
            Self::S3CompatibleCredential(credential) => &credential.name,
        }
    }
}
