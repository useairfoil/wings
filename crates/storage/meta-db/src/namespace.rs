use std::sync::Arc;

use bytes::Bytes;
use object_store::{ObjectStoreExt as _, UpdateVersion, path::Path};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use wings_dst_base::IdGenerator;
use wings_resources::{
    DataLakeConfiguration, Namespace, NamespaceName, NamespaceOptions, ObjectStoreConfiguration,
    TableName,
};
use wings_secret_manager::{SecretId, TypedSecretManager};
use wings_txn_obj::{
    ObjectCodec, SimpleTransactionalObject, TransactionalObject, TransactionalStorageProtocol,
    singleton::ObjectStoreSingletonStorageProtocol,
};

use crate::{
    cluster::ClusterStore,
    error::{Error, NotFoundSnafu, Result, txn_error},
    table::TableStore,
};

/// Access namespace metadata.
#[derive(Clone)]
pub struct NamespaceStore {
    cluster: ClusterStore,
    store: Arc<dyn TransactionalStorageProtocol<NamespaceManifest, UpdateVersion>>,
    name: NamespaceName,
}

/// A namespace stored on object storage.
#[derive(Clone)]
pub struct StoredNamespace {
    manifest: SimpleTransactionalObject<NamespaceManifest, UpdateVersion>,
    object_store: ObjectStoreConfiguration,
    lake: DataLakeConfiguration,
}

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

pub struct NamespaceManifestJsonCodec;

impl NamespaceStore {
    pub(crate) fn new(cluster: ClusterStore, name: NamespaceName) -> Self {
        let store = Arc::new(ObjectStoreSingletonStorageProtocol::new(
            cluster.object_store.clone(),
            manifest_path(&name),
            Box::new(NamespaceManifestJsonCodec),
        ));

        Self {
            cluster,
            store,
            name,
        }
    }

    pub fn table(&self, name: TableName) -> Result<TableStore> {
        if name.parent() != &self.name {
            return Err(Error::InvalidResourceParent {
                name: name.to_string(),
                expected_parent: self.name.to_string(),
                actual_parent: name.parent().to_string(),
            });
        }

        Ok(self.cluster.table(name))
    }

    pub async fn init(&self, options: NamespaceOptions) -> Result<StoredNamespace> {
        let name = self.name.clone();

        let object_store_secrets = TypedSecretManager::<ObjectStoreConfiguration>::new(
            self.cluster.secret_manager.clone(),
        );
        let lake_secrets =
            TypedSecretManager::<DataLakeConfiguration>::new(self.cluster.secret_manager.clone());
        let (object_store_secret_id, lake_secret_id) = {
            let mut rng = self.cluster.rng.rng();
            (
                namespace_secret_id("object-store", &mut *rng),
                namespace_secret_id("lake", &mut *rng),
            )
        };

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

        let now = self.cluster.clock.now();
        let manifest = NamespaceManifest {
            name,
            object_store_secret_id: object_store_secret_id.clone(),
            lake_secret_id: lake_secret_id.clone(),
            created_at: now,
            updated_at: now,
        };

        match SimpleTransactionalObject::init(self.store.clone(), manifest)
            .await
            .map_err(|err| txn_error(manifest_path(&self.name), err))
        {
            Ok(manifest) => Ok(StoredNamespace {
                manifest,
                object_store: options.object_store,
                lake: options.lake,
            }),
            Err(err) => {
                let _ = object_store_secrets
                    .delete_secret(&object_store_secret_id)
                    .await;
                let _ = lake_secrets.delete_secret(&lake_secret_id).await;
                Err(err)
            }
        }
    }

    pub async fn load(&self) -> Result<StoredNamespace> {
        let manifest = SimpleTransactionalObject::load(self.store.clone())
            .await
            .map_err(|err| txn_error(manifest_path(&self.name), err))?;
        let object_store = TypedSecretManager::<ObjectStoreConfiguration>::new(
            self.cluster.secret_manager.clone(),
        )
        .get_secret(&manifest.object().object_store_secret_id)
        .await?;
        let lake =
            TypedSecretManager::<DataLakeConfiguration>::new(self.cluster.secret_manager.clone())
                .get_secret(&manifest.object().lake_secret_id)
                .await?;

        Ok(StoredNamespace {
            manifest,
            object_store,
            lake,
        })
    }

    pub async fn try_load(&self) -> Result<Option<StoredNamespace>> {
        match self.load().await {
            Ok(namespace) => Ok(Some(namespace)),
            Err(Error::NotFound { .. }) => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub async fn delete(&self) -> Result<()> {
        self.cluster
            .object_store
            .delete(&manifest_path(&self.name))
            .await
            .map_err(|err| match err {
                object_store::Error::NotFound { path, .. } => NotFoundSnafu { path }.build(),
                _ => err.into(),
            })
    }
}

impl StoredNamespace {
    pub fn name(&self) -> &NamespaceName {
        &self.manifest().name
    }

    pub fn manifest(&self) -> &NamespaceManifest {
        self.manifest.object()
    }

    pub fn object_store(&self) -> &ObjectStoreConfiguration {
        &self.object_store
    }

    pub fn lake(&self) -> &DataLakeConfiguration {
        &self.lake
    }

    pub fn namespace(&self) -> Namespace {
        Namespace {
            name: self.manifest().name.clone(),
            object_store: self.object_store.clone(),
            lake: self.lake.clone(),
        }
    }
}

pub(crate) fn manifest_path(name: &NamespaceName) -> Path {
    Path::from(format!("{name}/manifest.json"))
}

fn namespace_secret_id(kind: &str, rng: &mut impl IdGenerator) -> SecretId {
    SecretId::parse(format!("{kind}-{}", rng.gen_uuid()))
        .expect("generated namespace secret id must not be empty")
}

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

impl std::fmt::Debug for StoredNamespace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoredNamespace")
            .field("name", &self.manifest().name)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use object_store::{ObjectStoreExt, PutPayload, memory::InMemory};

    use super::*;
    use crate::test_util::{new_test_cluster_store, new_test_namespace_options};

    #[tokio::test]
    async fn init_stores_manifest_and_secrets() {
        let store = new_test_cluster_store(Arc::new(InMemory::new()));
        let name = NamespaceName::new("test-namespace").unwrap();
        let options = new_test_namespace_options();

        let manifest = store
            .namespace(name.clone())
            .init(options.clone())
            .await
            .unwrap();

        insta::assert_yaml_snapshot!(manifest.manifest(), @r#"
        name: namespaces/test-namespace
        object_store_secret_id: object-store-233c1def-caf6-4ae8-b149-a5a5b203a354
        lake_secret_id: lake-456364cd-2c81-40f3-b5bb-9a3fc6395834
        created_at: "+002023-11-14T22:13:20.000000000Z"
        updated_at: "+002023-11-14T22:13:20.000000000Z"
        "#);
    }

    #[tokio::test]
    async fn load_reads_manifest_and_secrets() {
        let store = new_test_cluster_store(Arc::new(InMemory::new()));
        let name = NamespaceName::new("test-namespace").unwrap();
        let options = new_test_namespace_options();
        let namespace_store = store.namespace(name.clone());

        namespace_store.init(options.clone()).await.unwrap();

        let namespace = namespace_store.load().await.unwrap().namespace();

        insta::assert_yaml_snapshot!(namespace, @r#"
        name: namespaces/test-namespace
        object_store:
          S3Compatible:
            bucket_name: test-bucket
            prefix: test-prefix
            access_key_id: access-key-id
            secret_access_key: secret-access-key
            endpoint: "http://localhost:9000"
            region: test-region
            allow_http: true
        lake:
          Parquet: {}
        "#);
    }

    #[tokio::test]
    async fn delete_namespace_deletes_manifest() {
        let store = new_test_cluster_store(Arc::new(InMemory::new()));
        let name = NamespaceName::new("test-namespace").unwrap();
        let options = new_test_namespace_options();
        store.namespace(name.clone()).init(options).await.unwrap();

        let _stored = store.namespace(name.clone()).load().await.unwrap();

        store.namespace(name.clone()).delete().await.unwrap();

        let err = store.namespace(name).load().await.unwrap_err();

        insta::assert_compact_debug_snapshot!(err, @r#"NotFound { path: "namespaces/test-namespace/manifest.json" }"#);
    }

    #[tokio::test]
    async fn write_manifest_fails_when_manifest_already_exists() {
        let store = new_test_cluster_store(Arc::new(InMemory::new()));
        let name = NamespaceName::new("test-namespace").unwrap();
        let options = new_test_namespace_options();

        store
            .namespace(name.clone())
            .init(options.clone())
            .await
            .unwrap();

        let err = store.namespace(name).init(options).await.unwrap_err();

        insta::assert_compact_debug_snapshot!(err, @r#"AlreadyExists { path: "namespaces/test-namespace/manifest.json" }"#);
    }

    #[tokio::test]
    async fn try_read_manifest_returns_none_when_manifest_does_not_exist() {
        let store = new_test_cluster_store(Arc::new(InMemory::new()));
        let name = NamespaceName::new("missing-namespace").unwrap();

        assert!(store.namespace(name).try_load().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn read_manifest_fails_when_manifest_does_not_exist() {
        let store = new_test_cluster_store(Arc::new(InMemory::new()));
        let name = NamespaceName::new("missing-namespace").unwrap();

        let err = store.namespace(name).load().await.unwrap_err();

        insta::assert_compact_debug_snapshot!(err, @r#"NotFound { path: "namespaces/missing-namespace/manifest.json" }"#);
    }

    #[tokio::test]
    async fn read_manifest_fails_when_manifest_is_invalid_json() {
        let object_store = Arc::new(InMemory::new());
        let store = new_test_cluster_store(object_store.clone());
        let name = NamespaceName::new("test-namespace").unwrap();

        object_store
            .put(
                &manifest_path(&name),
                PutPayload::from(Bytes::from_static(b"not json")),
            )
            .await
            .unwrap();

        let err = store.namespace(name).load().await.unwrap_err();

        insta::assert_compact_debug_snapshot!(err, @r#"Decode { path: "namespaces/test-namespace/manifest.json" }"#);
    }
}
