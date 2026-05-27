use std::{marker::PhantomData, sync::Arc};

use serde::{Serialize, de::DeserializeOwned};

use crate::{Error, Result, SecretId, SecretManager};

#[derive(Clone)]
pub struct TypedSecretManager<T> {
    inner: Arc<dyn SecretManager>,
    _type: PhantomData<fn() -> T>,
}

impl<T> TypedSecretManager<T> {
    pub fn new(inner: Arc<dyn SecretManager>) -> Self {
        Self {
            inner,
            _type: PhantomData,
        }
    }

    pub fn inner(&self) -> &Arc<dyn SecretManager> {
        &self.inner
    }

    pub fn into_inner(self) -> Arc<dyn SecretManager> {
        self.inner
    }

    pub async fn get_secret(&self, id: &SecretId) -> Result<T>
    where
        T: DeserializeOwned,
    {
        let secret = self.inner.get_secret(id).await?;
        serde_json::from_str(&secret.value).map_err(|source| Error::Deserialize {
            secret_id: id.to_string(),
            source,
        })
    }

    pub async fn create_secret(&self, id: &SecretId, value: &T) -> Result<()>
    where
        T: Serialize,
    {
        let value = serde_json::to_string(value).map_err(|source| Error::Serialize {
            secret_id: id.to_string(),
            source,
        })?;

        self.inner.create_secret(id, value).await
    }

    pub async fn delete_secret(&self, id: &SecretId) -> Result<()> {
        self.inner.delete_secret(id).await
    }
}

impl<T> std::fmt::Display for TypedSecretManager<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TypedSecretManager({})", self.inner)
    }
}

impl<T> std::fmt::Debug for TypedSecretManager<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TypedSecretManager")
            .field("inner", &self.inner)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::memory::MemorySecretManager;

    #[derive(Debug, Deserialize, PartialEq, Serialize)]
    struct Credentials {
        username: String,
        password: String,
    }

    struct FailingSerialize;

    impl Serialize for FailingSerialize {
        fn serialize<S>(&self, _serializer: S) -> std::result::Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            Err(serde::ser::Error::custom("serialization failed"))
        }
    }

    #[tokio::test]
    async fn creates_and_gets_typed_secret() {
        let manager = TypedSecretManager::<Credentials>::new(Arc::new(MemorySecretManager::new()));
        let secret_id = SecretId::parse("credentials").unwrap();
        let credentials = Credentials {
            username: "user".to_string(),
            password: "password".to_string(),
        };

        manager
            .create_secret(&secret_id, &credentials)
            .await
            .unwrap();

        let secret = manager.get_secret(&secret_id).await.unwrap();
        assert_eq!(secret, credentials);
    }

    #[tokio::test]
    async fn create_serializes_secret_as_json() {
        let inner = Arc::new(MemorySecretManager::new());
        let manager = TypedSecretManager::<Credentials>::new(inner.clone());
        let secret_id = SecretId::parse("credentials").unwrap();
        let credentials = Credentials {
            username: "user".to_string(),
            password: "password".to_string(),
        };

        manager
            .create_secret(&secret_id, &credentials)
            .await
            .unwrap();

        let secret = inner.get_secret(&secret_id).await.unwrap();
        assert_eq!(secret.value, r#"{"username":"user","password":"password"}"#);
    }

    #[tokio::test]
    async fn get_invalid_json_returns_deserialize_error() {
        let inner = Arc::new(MemorySecretManager::new());
        let manager = TypedSecretManager::<Credentials>::new(inner.clone());
        let secret_id = SecretId::parse("credentials").unwrap();

        inner
            .create_secret(&secret_id, "not json".to_string())
            .await
            .unwrap();

        let error = manager.get_secret(&secret_id).await.unwrap_err();
        assert!(matches!(error, Error::Deserialize { .. }));
    }

    #[tokio::test]
    async fn create_unserializable_secret_returns_serialize_error() {
        let manager =
            TypedSecretManager::<FailingSerialize>::new(Arc::new(MemorySecretManager::new()));
        let secret_id = SecretId::parse("credentials").unwrap();

        let error = manager
            .create_secret(&secret_id, &FailingSerialize)
            .await
            .unwrap_err();

        assert!(matches!(error, Error::Serialize { .. }));
    }

    #[tokio::test]
    async fn delete_secret_delegates_to_inner_manager() {
        let manager = TypedSecretManager::<Credentials>::new(Arc::new(MemorySecretManager::new()));
        let secret_id = SecretId::parse("credentials").unwrap();
        let credentials = Credentials {
            username: "user".to_string(),
            password: "password".to_string(),
        };

        manager
            .create_secret(&secret_id, &credentials)
            .await
            .unwrap();
        manager.delete_secret(&secret_id).await.unwrap();

        let error = manager.get_secret(&secret_id).await.unwrap_err();
        assert!(matches!(error, Error::NotFound { .. }));
    }
}
