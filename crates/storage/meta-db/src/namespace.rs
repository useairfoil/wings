use std::sync::Arc;

use bytes::Bytes;
use object_store::{ObjectStore as DynObjectStore, PutMode, PutOptions, PutPayload, path::Path};
use serde::{Deserialize, Serialize};
use slatedb_txn_obj::ObjectCodec;
use time::OffsetDateTime;
use ulid::Ulid;
use wings_resources::{
    Lake, Namespace, NamespaceName, NamespaceOptions, ObjectStore as ResourceObjectStore,
};
use wings_secret_manager::{SecretId, SecretManager, TypedSecretManager};

use crate::error::{AlreadyExistsSnafu, DecodeSnafu, NotFoundSnafu, Result};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NamespaceManifest {
    pub name: NamespaceName,
    pub object_store_secret_id: SecretId,
    pub lake_secret_id: SecretId,
    #[serde(with = "time::serde::iso8601")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::iso8601")]
    pub updated_at: OffsetDateTime,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ListNamespaceNamesResult {
    pub names: Vec<NamespaceName>,
    pub next_page_token: Option<String>,
}

pub struct NamespaceStore {
    object_store: Arc<dyn DynObjectStore>,
    codec: Box<dyn ObjectCodec<NamespaceManifest>>,
}

struct NamespaceManifestJsonCodec;

impl ObjectCodec<NamespaceManifest> for NamespaceManifestJsonCodec {
    fn encode(&self, value: &NamespaceManifest) -> Bytes {
        serde_json::to_vec(value)
            .expect("NamespaceManifestJsonCodec failed")
            .into()
    }

    fn decode(
        &self,
        bytes: &Bytes,
    ) -> Result<NamespaceManifest, Box<dyn std::error::Error + Send + Sync>> {
        serde_json::from_slice(bytes)
            .map_err(|err| Box::new(err) as Box<dyn std::error::Error + Send + Sync + 'static>)
    }
}

impl NamespaceStore {
    pub fn new(object_store: Arc<dyn DynObjectStore>) -> Self {
        Self {
            object_store,
            codec: Box::new(NamespaceManifestJsonCodec),
        }
    }

    pub async fn create_namespace(
        &self,
        secret_manager: Arc<dyn SecretManager>,
        name: NamespaceName,
        options: NamespaceOptions,
    ) -> Result<()> {
        let object_store_secrets =
            TypedSecretManager::<ResourceObjectStore>::new(secret_manager.clone());
        let lake_secrets = TypedSecretManager::<Lake>::new(secret_manager);
        let object_store_secret_id = namespace_secret_id("object-store");
        let lake_secret_id = namespace_secret_id("lake");

        object_store_secrets
            .create_secret(&object_store_secret_id, &options.object_store)
            .await?;
        if let Err(err) = lake_secrets
            .create_secret(&lake_secret_id, &options.lake)
            .await
        {
            let _ = object_store_secrets
                .delete_secret(&object_store_secret_id)
                .await;
            return Err(err.into());
        }

        let now = OffsetDateTime::now_utc();
        let manifest = NamespaceManifest {
            name,
            object_store_secret_id: object_store_secret_id.clone(),
            lake_secret_id: lake_secret_id.clone(),
            created_at: now,
            updated_at: now,
        };

        if let Err(err) = self.write_manifest(&manifest).await {
            let _ = object_store_secrets
                .delete_secret(&object_store_secret_id)
                .await;
            let _ = lake_secrets.delete_secret(&lake_secret_id).await;
            return Err(err);
        }

        Ok(())
    }

    pub async fn get_namespace(
        &self,
        secret_manager: Arc<dyn SecretManager>,
        name: &NamespaceName,
    ) -> Result<Namespace> {
        let manifest = self.read_manifest(name).await?;
        let object_store = TypedSecretManager::<ResourceObjectStore>::new(secret_manager.clone())
            .get_secret(&manifest.object_store_secret_id)
            .await?;
        let lake = TypedSecretManager::<Lake>::new(secret_manager)
            .get_secret(&manifest.lake_secret_id)
            .await?;

        Ok(Namespace {
            name: manifest.name,
            object_store,
            lake,
        })
    }

    pub async fn delete_namespace(&self, name: &NamespaceName) -> Result<()> {
        self.delete_manifest(name).await
    }

    pub async fn list_namespace_names(
        &self,
        page_size: Option<usize>,
        page_token: Option<String>,
    ) -> Result<ListNamespaceNamesResult> {
        let prefix = Path::from("namespaces");
        let page_size = page_size.unwrap_or(100).clamp(1, 1000);
        let mut namespace_names = self
            .object_store
            .list_with_delimiter(Some(&prefix))
            .await?
            .common_prefixes
            .into_iter()
            .filter_map(|path| NamespaceName::parse(path.as_ref()).ok())
            .collect::<Vec<_>>();

        namespace_names.sort_by_key(|name| name.to_string());

        let mut names: Vec<NamespaceName> = Vec::new();
        let mut next_page_token = None;
        for name in namespace_names {
            let token = name.to_string();
            if page_token.as_ref().is_some_and(|offset| token <= *offset) {
                continue;
            }

            if names.len() == page_size {
                next_page_token = names.last().map(|name| name.to_string());
                break;
            }

            names.push(name);
        }

        Ok(ListNamespaceNamesResult {
            names,
            next_page_token,
        })
    }

    pub async fn write_manifest(&self, manifest: &NamespaceManifest) -> Result<()> {
        let payload = PutPayload::from(self.codec.encode(&manifest));
        self.object_store
            .put_opts(
                &manifest_path(&manifest.name),
                payload,
                PutOptions::from(PutMode::Create),
            )
            .await
            .map_err(|err| match err {
                object_store::Error::AlreadyExists { path, .. } => {
                    AlreadyExistsSnafu { path }.build()
                }
                _ => err.into(),
            })?;

        Ok(())
    }

    pub async fn read_manifest(&self, name: &NamespaceName) -> Result<NamespaceManifest> {
        let path = manifest_path(name);
        let result =
            self.object_store
                .get(&manifest_path(name))
                .await
                .map_err(|err| match err {
                    object_store::Error::NotFound { path, .. } => NotFoundSnafu { path }.build(),
                    _ => err.into(),
                })?;

        let bytes = result.bytes().await?;
        self.codec
            .decode(&bytes)
            .map_err(|_| DecodeSnafu { path }.build())
    }

    pub async fn delete_manifest(&self, name: &NamespaceName) -> Result<()> {
        self.object_store
            .delete(&manifest_path(name))
            .await
            .map_err(|err| match err {
                object_store::Error::NotFound { path, .. } => NotFoundSnafu { path }.build(),
                _ => err.into(),
            })?;

        Ok(())
    }
}

fn manifest_path(name: &NamespaceName) -> Path {
    Path::from(format!("{name}/manifest.json"))
}

fn namespace_secret_id(kind: &str) -> SecretId {
    SecretId::parse(format!("namespace-{kind}-{}", Ulid::new()))
        .expect("generated namespace secret id must not be empty")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::{ObjectStore as DynObjectStore, PutPayload, memory::InMemory};
    use wings_resources::{ParquetConfiguration, S3CompatibleConfiguration};
    use wings_secret_manager::memory::MemorySecretManager;

    use super::*;

    fn namespace_store(object_store: Arc<dyn DynObjectStore>) -> NamespaceStore {
        NamespaceStore::new(object_store)
    }

    fn test_manifest(namespace: &str) -> NamespaceManifest {
        NamespaceManifest {
            name: NamespaceName::new(namespace).unwrap(),
            object_store_secret_id: SecretId::parse("object-store-secret").unwrap(),
            lake_secret_id: SecretId::parse("lake-secret").unwrap(),
            created_at: OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap(),
            updated_at: OffsetDateTime::from_unix_timestamp(1_700_000_001).unwrap(),
        }
    }

    fn test_options() -> NamespaceOptions {
        NamespaceOptions {
            object_store: ResourceObjectStore::S3Compatible(S3CompatibleConfiguration {
                bucket_name: "test-bucket".to_string(),
                prefix: Some("test-prefix".to_string()),
                access_key_id: "access-key-id".to_string(),
                secret_access_key: "secret-access-key".to_string(),
                endpoint: "http://localhost:9000".to_string(),
                region: Some("test-region".to_string()),
                allow_http: true,
            }),
            lake: Lake::Parquet(ParquetConfiguration::default()),
        }
    }

    #[tokio::test]
    async fn create_namespace_stores_manifest_and_secrets() {
        let store = namespace_store(Arc::new(InMemory::new()));
        let secret_manager = Arc::new(MemorySecretManager::new());
        let name = NamespaceName::new("test-namespace").unwrap();
        let options = test_options();

        store
            .create_namespace(secret_manager.clone(), name.clone(), options.clone())
            .await
            .unwrap();

        let manifest = store.read_manifest(&name).await.unwrap();
        assert_eq!(manifest.name, name);
        assert_eq!(manifest.created_at, manifest.updated_at);

        let object_store = TypedSecretManager::<ResourceObjectStore>::new(secret_manager.clone())
            .get_secret(&manifest.object_store_secret_id)
            .await
            .unwrap();
        let lake = TypedSecretManager::<Lake>::new(secret_manager)
            .get_secret(&manifest.lake_secret_id)
            .await
            .unwrap();

        assert_eq!(object_store, options.object_store);
        assert_eq!(lake, options.lake);
    }

    #[tokio::test]
    async fn get_namespace_reads_manifest_and_secrets() {
        let store = namespace_store(Arc::new(InMemory::new()));
        let secret_manager = Arc::new(MemorySecretManager::new());
        let name = NamespaceName::new("test-namespace").unwrap();
        let options = test_options();

        store
            .create_namespace(secret_manager.clone(), name.clone(), options.clone())
            .await
            .unwrap();

        let namespace = store.get_namespace(secret_manager, &name).await.unwrap();

        assert_eq!(namespace.name, name);
        assert_eq!(namespace.object_store, options.object_store);
        assert_eq!(namespace.lake, options.lake);
    }

    #[tokio::test]
    async fn delete_namespace_deletes_manifest() {
        let store = namespace_store(Arc::new(InMemory::new()));
        let manifest = test_manifest("test-namespace");

        store.write_manifest(&manifest).await.unwrap();
        store.delete_namespace(&manifest.name).await.unwrap();
        let err = store.read_manifest(&manifest.name).await.unwrap_err();

        insta::assert_compact_debug_snapshot!(err, @r#"NotFound { path: "namespaces/test-namespace/manifest.json" }"#);
    }

    #[tokio::test]
    async fn list_namespace_names_returns_namespace_folders_in_path_order() {
        let object_store = Arc::new(InMemory::new());
        let store = namespace_store(object_store.clone());

        store
            .write_manifest(&test_manifest("charlie"))
            .await
            .unwrap();
        store.write_manifest(&test_manifest("alpha")).await.unwrap();
        store.write_manifest(&test_manifest("bravo")).await.unwrap();
        object_store
            .put(
                &Path::from("namespaces/not-a-namespace.json"),
                PutPayload::from(Bytes::from_static(b"not a folder")),
            )
            .await
            .unwrap();

        let result = store.list_namespace_names(None, None).await.unwrap();
        insta::assert_yaml_snapshot!(result, @r"
        names:
          - namespaces/alpha
          - namespaces/bravo
          - namespaces/charlie
        next_page_token: ~
        ");
    }

    #[tokio::test]
    async fn list_namespace_names_paginates_with_next_page_token() {
        let store = namespace_store(Arc::new(InMemory::new()));

        store
            .write_manifest(&test_manifest("charlie"))
            .await
            .unwrap();
        store.write_manifest(&test_manifest("alpha")).await.unwrap();
        store.write_manifest(&test_manifest("bravo")).await.unwrap();

        let first_page = store.list_namespace_names(Some(2), None).await.unwrap();
        insta::assert_yaml_snapshot!(first_page, @r"
        names:
          - namespaces/alpha
          - namespaces/bravo
        next_page_token: namespaces/bravo
        ");

        let second_page = store
            .list_namespace_names(Some(2), first_page.next_page_token)
            .await
            .unwrap();
        insta::assert_yaml_snapshot!(second_page, @r"
        names:
          - namespaces/charlie
        next_page_token: ~
        ");
    }

    #[tokio::test]
    async fn write_then_read_manifest_round_trips() {
        let store = namespace_store(Arc::new(InMemory::new()));
        let manifest = test_manifest("test-namespace");

        store.write_manifest(&manifest).await.unwrap();
        let actual = store.read_manifest(&manifest.name).await.unwrap();

        assert_eq!(actual, manifest);
    }

    #[tokio::test]
    async fn write_manifest_fails_when_manifest_already_exists() {
        let store = namespace_store(Arc::new(InMemory::new()));
        let manifest = test_manifest("test-namespace");

        store.write_manifest(&manifest).await.unwrap();
        let err = store.write_manifest(&manifest).await.unwrap_err();

        insta::assert_compact_debug_snapshot!(err, @r#"AlreadyExists { path: "namespaces/test-namespace/manifest.json" }"#);
    }

    #[tokio::test]
    async fn read_manifest_fails_when_manifest_does_not_exist() {
        let store = namespace_store(Arc::new(InMemory::new()));
        let name = NamespaceName::new("missing-namespace").unwrap();

        let err = store.read_manifest(&name).await.unwrap_err();

        insta::assert_compact_debug_snapshot!(err, @r#"NotFound { path: "namespaces/missing-namespace/manifest.json" }"#);
    }

    #[tokio::test]
    async fn read_manifest_fails_when_manifest_is_invalid_json() {
        let object_store = Arc::new(InMemory::new());
        let store = namespace_store(object_store.clone());
        let name = NamespaceName::new("test-namespace").unwrap();

        object_store
            .put(
                &manifest_path(&name),
                PutPayload::from(Bytes::from_static(b"not json")),
            )
            .await
            .unwrap();

        let err = store.read_manifest(&name).await.unwrap_err();

        insta::assert_compact_debug_snapshot!(err, @r#"Decode { path: "namespaces/test-namespace/manifest.json" }"#);
    }

    #[tokio::test]
    async fn delete_manifest_removes_existing_manifest() {
        let store = namespace_store(Arc::new(InMemory::new()));
        let manifest = test_manifest("test-namespace");

        store.write_manifest(&manifest).await.unwrap();
        store.delete_manifest(&manifest.name).await.unwrap();
        let err = store.read_manifest(&manifest.name).await.unwrap_err();

        insta::assert_compact_debug_snapshot!(err, @r#"NotFound { path: "namespaces/test-namespace/manifest.json" }"#);
    }
}
