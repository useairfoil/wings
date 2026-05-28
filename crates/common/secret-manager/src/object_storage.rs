use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use object_store::{ObjectStore as DynObjectStore, UpdateVersion, path::Path};
use tokio::sync::Mutex;
use tracing::warn;
use wings_txn_obj::{
    ObjectCodec, SimpleTransactionalObject, TransactionalObject, TransactionalObjectError,
    TransactionalStorageProtocol, singleton::ObjectStoreSingletonStorageProtocol,
};

use crate::{GetResult, Result, SecretId, SecretManager};

const MANAGER: &str = "UnsecureObjectStorageSecretManager";
const DEFAULT_SECRETS_PATH: &str = "secrets.json";

type Secrets = BTreeMap<String, String>;
type SecretsObject = SimpleTransactionalObject<Secrets, UpdateVersion>;

/// Stores secrets as plaintext JSON on object storage.
pub struct UnsecureObjectStorageSecretManager {
    path: Path,
    secrets: Mutex<SecretsObject>,
}

struct SecretsJsonCodec;

impl ObjectCodec<Secrets> for SecretsJsonCodec {
    fn encode(&self, value: &Secrets) -> Bytes {
        serde_json::to_vec(value)
            .expect("SecretsJsonCodec failed to serialize secrets")
            .into()
    }

    fn decode(
        &self,
        bytes: &Bytes,
    ) -> std::result::Result<Secrets, Box<dyn std::error::Error + Send + Sync>> {
        serde_json::from_slice(bytes)
            .map_err(|err| Box::new(err) as Box<dyn std::error::Error + Send + Sync>)
    }
}

impl UnsecureObjectStorageSecretManager {
    pub async fn new(object_store: Arc<dyn DynObjectStore>) -> Result<Self> {
        Self::new_with_path(object_store, Path::from(DEFAULT_SECRETS_PATH)).await
    }

    pub async fn new_with_path(object_store: Arc<dyn DynObjectStore>, path: Path) -> Result<Self> {
        warn!("Storing secrets in secrets.json. DO NOT USE WITH PRODUCTION SECRETS");
        let store: Arc<dyn TransactionalStorageProtocol<Secrets, UpdateVersion>> =
            Arc::new(ObjectStoreSingletonStorageProtocol::new(
                object_store,
                path.clone(),
                Box::new(SecretsJsonCodec),
            ));

        let secrets = load_or_init(store).await?;

        Ok(Self {
            path,
            secrets: Mutex::new(secrets),
        })
    }

    async fn update<F>(&self, change: F) -> Result<()>
    where
        F: Fn(&mut Secrets) -> Result<()> + Send + Sync,
    {
        self.secrets
            .lock()
            .await
            .maybe_apply_update(|secrets| {
                let mut dirty = secrets
                    .prepare_dirty()
                    .map_err(transactional_object_error)?;
                change(&mut dirty.value)?;

                if &dirty.value == secrets.object() {
                    Ok::<_, crate::Error>(None)
                } else {
                    Ok(Some(dirty))
                }
            })
            .await
            .map_err(transactional_object_error)
    }
}

async fn load_or_init(
    store: Arc<dyn TransactionalStorageProtocol<Secrets, UpdateVersion>>,
) -> Result<SecretsObject> {
    loop {
        if let Some(secrets) = SecretsObject::try_load(store.clone())
            .await
            .map_err(transactional_object_error)?
        {
            return Ok(secrets);
        }

        match SecretsObject::init(store.clone(), Secrets::default()).await {
            Ok(secrets) => return Ok(secrets),
            Err(err) if err.is_sequenced_write_conflict() => continue,
            Err(err) => return Err(transactional_object_error(err)),
        }
    }
}

fn transactional_object_error(err: TransactionalObjectError) -> crate::Error {
    crate::Error::Generic {
        manager: MANAGER,
        source: Box::new(err),
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

#[async_trait]
impl SecretManager for UnsecureObjectStorageSecretManager {
    async fn get_secret(&self, id: &SecretId) -> Result<GetResult> {
        let value = self
            .secrets
            .lock()
            .await
            .object()
            .get(id.as_ref())
            .cloned()
            .ok_or_else(|| not_found(id))?;

        Ok(GetResult { value })
    }

    async fn create_secret(&self, id: &SecretId, value: String) -> Result<()> {
        self.update(|secrets| {
            secrets.insert(id.as_ref().to_string(), value.clone());
            Ok(())
        })
        .await
    }

    async fn delete_secret(&self, id: &SecretId) -> Result<()> {
        self.update(|secrets| {
            secrets
                .remove(id.as_ref())
                .map(|_| ())
                .ok_or_else(|| not_found(id))
        })
        .await
    }
}

impl std::fmt::Display for UnsecureObjectStorageSecretManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::fmt::Debug for UnsecureObjectStorageSecretManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnsecureObjectStorageSecretManager")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use object_store::{ObjectStoreExt, memory::InMemory};

    use super::*;

    fn secret_id(id: &str) -> SecretId {
        SecretId::parse(id).unwrap()
    }

    async fn stored_secrets(object_store: &Arc<dyn DynObjectStore>) -> BTreeMap<String, String> {
        let bytes = object_store
            .get(&Path::from(DEFAULT_SECRETS_PATH))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();

        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn creates_and_loads_secret_from_object_storage() {
        let object_store: Arc<dyn DynObjectStore> = Arc::new(InMemory::new());
        let manager = UnsecureObjectStorageSecretManager::new(object_store.clone())
            .await
            .unwrap();
        let id = secret_id("my-secret");

        manager
            .create_secret(&id, "very secret".to_string())
            .await
            .unwrap();

        let loaded_manager = UnsecureObjectStorageSecretManager::new(object_store)
            .await
            .unwrap();
        let secret = loaded_manager.get_secret(&id).await.unwrap();
        assert_eq!(secret.value, "very secret");
    }

    #[tokio::test]
    async fn retries_create_after_stale_empty_state() {
        let object_store: Arc<dyn DynObjectStore> = Arc::new(InMemory::new());
        let stale_manager = UnsecureObjectStorageSecretManager::new(object_store.clone())
            .await
            .unwrap();
        let fresh_manager = UnsecureObjectStorageSecretManager::new(object_store.clone())
            .await
            .unwrap();

        fresh_manager
            .create_secret(&secret_id("first"), "one".to_string())
            .await
            .unwrap();
        stale_manager
            .create_secret(&secret_id("second"), "two".to_string())
            .await
            .unwrap();

        let secrets = stored_secrets(&object_store).await;

        insta::assert_yaml_snapshot!(secrets, @r"
        first: one
        second: two
        ");
    }

    #[tokio::test]
    async fn retries_update_after_stale_etag() {
        let object_store: Arc<dyn DynObjectStore> = Arc::new(InMemory::new());
        let seed_manager = UnsecureObjectStorageSecretManager::new(object_store.clone())
            .await
            .unwrap();
        seed_manager
            .create_secret(&secret_id("seed"), "seed".to_string())
            .await
            .unwrap();

        let stale_manager = UnsecureObjectStorageSecretManager::new(object_store.clone())
            .await
            .unwrap();
        let fresh_manager = UnsecureObjectStorageSecretManager::new(object_store.clone())
            .await
            .unwrap();

        fresh_manager
            .create_secret(&secret_id("first"), "one".to_string())
            .await
            .unwrap();
        stale_manager
            .create_secret(&secret_id("second"), "two".to_string())
            .await
            .unwrap();

        let secrets = stored_secrets(&object_store).await;
        insta::assert_yaml_snapshot!(secrets, @r"
        first: one
        second: two
        seed: seed
        ");
    }

    #[tokio::test]
    async fn deletes_secret_from_object_storage() {
        let object_store: Arc<dyn DynObjectStore> = Arc::new(InMemory::new());
        let manager = UnsecureObjectStorageSecretManager::new(object_store.clone())
            .await
            .unwrap();
        let id = secret_id("my-secret");
        manager
            .create_secret(&id, "very secret".to_string())
            .await
            .unwrap();

        manager.delete_secret(&id).await.unwrap();

        let error = manager.get_secret(&id).await.unwrap_err();
        insta::assert_compact_debug_snapshot!(error, @r#"NotFound { secret_id: "my-secret", source: Custom { kind: NotFound, error: "secret not found" } }"#);
        let secrets = stored_secrets(&object_store).await;
        insta::assert_yaml_snapshot!(secrets, @"{}");
    }
}
