use std::sync::Arc;

use azure_identity::AzureCliCredential;
use azure_security_keyvault_secrets::SecretClient;
use snafu::{OptionExt, Snafu};

use crate::{
    Result,
    azure::{AzureCredentialProvider, AzureKeyVault, MANAGER_NAME},
};

#[derive(Debug, Default)]
pub struct AzureKeyVaultBuilder {
    vault_name: Option<String>,
    endpoint: Option<String>,
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Azure Key Vault requires either a vault name or an endpoint"))]
    MissingVaultBaseUrl,

    Azure {
        source: azure_core::Error,
    },
}

impl AzureKeyVaultBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_vault_name(mut self, vault_name: impl Into<String>) -> Self {
        self.vault_name = Some(vault_name.into());
        self
    }

    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    pub fn from_env() -> Self {
        let mut builder = Self::new();

        if let Ok(endpoint) = std::env::var("AZURE_KEY_VAULT_ENDPOINT") {
            builder = builder.with_endpoint(endpoint);
        }

        if let Ok(vault_name) = std::env::var("AZURE_KEY_VAULT_NAME") {
            builder = builder.with_vault_name(vault_name);
        }

        builder
    }

    pub fn build(&self) -> Result<AzureKeyVault> {
        let endpoint = self.vault_base_url()?;
        let credentials = self.credentials()?;

        let client: Arc<_> = SecretClient::new(&endpoint, credentials, None)?.into();

        Ok(AzureKeyVault { client })
    }

    fn vault_base_url(&self) -> Result<String> {
        let url = self
            .endpoint
            .clone()
            .or_else(|| {
                self.vault_name
                    .as_ref()
                    .map(|vault_name| format!("https://{vault_name}.vault.azure.net"))
            })
            .context(MissingVaultBaseUrlSnafu {})?;

        Ok(url)
    }

    fn credentials(&self) -> Result<AzureCredentialProvider> {
        let credentials = AzureCliCredential::new(None)?;
        Ok(credentials)
    }
}

impl From<Error> for crate::Error {
    fn from(err: Error) -> Self {
        Self::Generic {
            manager: MANAGER_NAME,
            source: Box::new(err),
        }
    }
}
