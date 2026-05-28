use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use object_store::{
    ObjectStore as DynObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion,
    path::Path,
};
use tokio::sync::Mutex;
use tracing::warn;

use crate::{GetResult, Result, SecretId, SecretManager};

const MANAGER: &str = "UnsecureObjectStorageSecretManager";
const DEFAULT_SECRETS_PATH: &str = "secrets.json";

/// Stores secrets as plaintext JSON on object storage.
pub struct UnsecureObjectStorageSecretManager {
    object_store: Arc<dyn DynObjectStore>,
    path: Path,
    state: Mutex<State>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct State {
    secrets: BTreeMap<String, String>,
    update_version: Option<UpdateVersion>,
}

impl UnsecureObjectStorageSecretManager {
    pub async fn new(object_store: Arc<dyn DynObjectStore>) -> Result<Self> {
        Self::new_with_path(object_store, Path::from(DEFAULT_SECRETS_PATH)).await
    }

    pub async fn new_with_path(object_store: Arc<dyn DynObjectStore>, path: Path) -> Result<Self> {
        warn!("Storing secrets in secrets.json. DO NOT USE WITH PRODUCTION SECRETS");
        let state = load_state(&object_store, &path).await?;

        Ok(Self {
            object_store,
            path,
            state: Mutex::new(state),
        })
    }

    async fn update<F>(&self, mut change: F) -> Result<()>
    where
        F: FnMut(&mut BTreeMap<String, String>) -> Result<()>,
    {
        loop {
            let (secrets, update_version) = {
                let state = self.state.lock().await;
                let mut secrets = state.secrets.clone();
                change(&mut secrets)?;

                if secrets == state.secrets {
                    return Ok(());
                }

                (secrets, state.update_version.clone())
            };

            let payload = serialize_secrets(&self.path, &secrets)?;
            let mode = match update_version {
                Some(update_version) => PutMode::Update(update_version),
                None => PutMode::Create,
            };

            match self
                .object_store
                .put_opts(
                    &self.path,
                    PutPayload::from(payload),
                    PutOptions::from(mode),
                )
                .await
            {
                Ok(put_result) => {
                    let mut state = self.state.lock().await;
                    state.secrets = secrets;
                    state.update_version = Some(put_result.into());
                    return Ok(());
                }
                Err(err) if is_conditional_write_failure(&err) => {
                    let reloaded = load_state(&self.object_store, &self.path).await?;
                    let mut state = self.state.lock().await;
                    *state = reloaded;
                }
                Err(err) => return Err(object_store_error(err)),
            }
        }
    }
}

#[async_trait]
impl SecretManager for UnsecureObjectStorageSecretManager {
    async fn get_secret(&self, id: &SecretId) -> Result<GetResult> {
        let value = self
            .state
            .lock()
            .await
            .secrets
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

async fn load_state(object_store: &Arc<dyn DynObjectStore>, path: &Path) -> Result<State> {
    let result = match object_store.get(path).await {
        Ok(result) => result,
        Err(object_store::Error::NotFound { .. }) => return Ok(State::default()),
        Err(err) => return Err(object_store_error(err)),
    };

    let update_version = UpdateVersion {
        e_tag: result.meta.e_tag.clone(),
        version: result.meta.version.clone(),
    };
    let bytes = result.bytes().await.map_err(object_store_error)?;
    let secrets = serde_json::from_slice(&bytes).map_err(|source| crate::Error::Deserialize {
        secret_id: path.to_string(),
        source,
    })?;

    Ok(State {
        secrets,
        update_version: Some(update_version),
    })
}

fn serialize_secrets(path: &Path, secrets: &BTreeMap<String, String>) -> Result<Vec<u8>> {
    serde_json::to_vec(secrets).map_err(|source| crate::Error::Serialize {
        secret_id: path.to_string(),
        source,
    })
}

fn is_conditional_write_failure(err: &object_store::Error) -> bool {
    matches!(
        err,
        object_store::Error::AlreadyExists { .. } | object_store::Error::Precondition { .. }
    )
}

fn object_store_error(err: object_store::Error) -> crate::Error {
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

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;

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
