mod builder;

use std::sync::Arc;

use async_trait::async_trait;
use azure_security_keyvault_secrets::{SecretClient, models::SetSecretParameters};

pub use self::builder::AzureKeyVaultBuilder;
use crate::{GetResult, Result, SecretId, SecretManager};

pub type AzureCredentialProvider = Arc<dyn azure_core::credentials::TokenCredential>;

pub(crate) const MANAGER_NAME: &str = "azure-key-vault";

#[derive(Clone)]
pub struct AzureKeyVault {
    client: Arc<SecretClient>,
}

#[async_trait]
impl SecretManager for AzureKeyVault {
    async fn get_secret(&self, id: &SecretId) -> Result<GetResult> {
        let secret = self
            .client
            .get_secret(id.as_ref(), None)
            .await
            .map_err(|err| response_error(id, err))?
            .into_model()
            .map_err(|err| response_error(id, err))?;

        Ok(secret.into())
    }

    async fn create_secret(&self, id: &SecretId, value: String) -> Result<()> {
        let set_secret_params = SetSecretParameters {
            value: Some(value),
            ..Default::default()
        }
        .try_into()?;

        self.client
            .set_secret(id.as_ref(), set_secret_params, None)
            .await
            .map_err(|err| response_error(id, err))?
            .into_model()
            .map_err(|err| response_error(id, err))?;

        Ok(())
    }

    async fn delete_secret(&self, id: &SecretId) -> Result<()> {
        self.client
            .delete_secret(id.as_ref(), None)
            .await
            .map_err(|err| response_error(id, err))?
            .into_model()
            .map_err(|err| response_error(id, err))?;

        Ok(())
    }
}

impl std::fmt::Display for AzureKeyVault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::fmt::Debug for AzureKeyVault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "AzureKeyVault {{ endpoint: {} }}",
            self.client.endpoint()
        )
    }
}

fn response_error(secret_id: &SecretId, err: azure_core::Error) -> crate::Error {
    use azure_core::{error::ErrorKind, http::StatusCode};

    match err.kind() {
        ErrorKind::Credential => crate::Error::Unauthorized {
            secret_id: secret_id.to_string(),
            source: Box::new(err),
        },
        ErrorKind::HttpResponse { status, .. } if status == &StatusCode::NotFound => {
            crate::Error::NotFound {
                secret_id: secret_id.to_string(),
                source: Box::new(err),
            }
        }
        _ => err.into(),
    }
}

impl From<azure_core::Error> for crate::Error {
    fn from(err: azure_core::Error) -> Self {
        crate::Error::Generic {
            manager: MANAGER_NAME,
            source: Box::new(err),
        }
    }
}

impl From<azure_security_keyvault_secrets::models::Secret> for GetResult {
    fn from(secret: azure_security_keyvault_secrets::models::Secret) -> Self {
        GetResult {
            value: secret.value.unwrap_or_default(),
        }
    }
}
