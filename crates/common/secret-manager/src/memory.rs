use std::sync::Arc;

use dashmap::DashMap;

use crate::{GetResult, Result, SecretId, SecretManager};

#[derive(Clone, Default)]
pub struct MemorySecretManager {
    secrets: Arc<DashMap<String, String>>,
}

impl MemorySecretManager {
    pub fn new() -> Self {
        Self::default()
    }
}

impl SecretManager for MemorySecretManager {
    async fn get_secret(&self, id: &SecretId) -> Result<GetResult> {
        let value = self
            .secrets
            .get(id.as_ref())
            .map(|secret| secret.value().clone())
            .ok_or_else(|| not_found(id))?;

        Ok(GetResult { value })
    }

    async fn create_secret(&self, id: &SecretId, value: String) -> Result<()> {
        self.secrets.insert(id.as_ref().to_string(), value);
        Ok(())
    }

    async fn delete_secret(&self, id: &SecretId) -> Result<()> {
        self.secrets
            .remove(id.as_ref())
            .map(|_| ())
            .ok_or_else(|| not_found(id))
    }
}

impl std::fmt::Display for MemorySecretManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::fmt::Debug for MemorySecretManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemorySecretManager").finish()
    }
}

fn not_found(secret_id: &SecretId) -> crate::Error {
    crate::Error::NotFound {
        secret_id: secret_id.to_string(),
        source: Box::new(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "secret not found",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn creates_and_gets_secret() {
        let manager = MemorySecretManager::new();
        let secret_id = SecretId::parse("my-secret").unwrap();

        manager
            .create_secret(&secret_id, "very secret".to_string())
            .await
            .unwrap();

        let secret = manager.get_secret(&secret_id).await.unwrap();
        assert_eq!(secret.value, "very secret");
    }

    #[tokio::test]
    async fn overwrites_secret() {
        let manager = MemorySecretManager::new();
        let secret_id = SecretId::parse("my-secret").unwrap();

        manager
            .create_secret(&secret_id, "old".to_string())
            .await
            .unwrap();
        manager
            .create_secret(&secret_id, "new".to_string())
            .await
            .unwrap();

        let secret = manager.get_secret(&secret_id).await.unwrap();
        assert_eq!(secret.value, "new");
    }

    #[tokio::test]
    async fn deletes_secret() {
        let manager = MemorySecretManager::new();
        let secret_id = SecretId::parse("my-secret").unwrap();

        manager
            .create_secret(&secret_id, "very secret".to_string())
            .await
            .unwrap();
        manager.delete_secret(&secret_id).await.unwrap();

        let error = manager.get_secret(&secret_id).await.unwrap_err();
        assert!(matches!(error, crate::Error::NotFound { .. }));
    }

    #[tokio::test]
    async fn delete_missing_secret_returns_not_found() {
        let manager = MemorySecretManager::new();
        let secret_id = SecretId::parse("my-secret").unwrap();

        let error = manager.delete_secret(&secret_id).await.unwrap_err();
        assert!(matches!(error, crate::Error::NotFound { .. }));
    }
}
