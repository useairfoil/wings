pub mod azure;
mod secret_id;

use std::fmt::Debug;

use snafu::Snafu;

pub use self::secret_id::SecretId;

pub struct GetResult {
    /// The secret value.
    pub value: String,
}

pub trait SecretManager: std::fmt::Display + std::fmt::Debug + Send + Sync + 'static {
    fn get_secret(&self, id: &SecretId) -> impl Future<Output = Result<GetResult>> + Send;

    fn create_secret(
        &self,
        id: &SecretId,
        value: String,
    ) -> impl Future<Output = Result<()>> + Send;

    fn delete_secret(&self, id: &SecretId) -> impl Future<Output = Result<()>> + Send;
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Generic {manager} error: {source}"))]
    Generic {
        manager: &'static str,
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[snafu(display("Secret {secret_id} not found: {source}"))]
    NotFound {
        secret_id: String,
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[snafu(display("Unauthorized access to secret {secret_id}: {source}"))]
    Unauthorized {
        secret_id: String,
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Debug for GetResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GetResult").finish()
    }
}
