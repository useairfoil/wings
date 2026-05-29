use std::sync::Arc;

use bytes::Bytes;
use object_store::{ObjectStoreExt as _, UpdateVersion, path::Path};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use wings_resources::{Table, TableName, TableOptions};
use wings_txn_obj::{
    ObjectCodec, SimpleTransactionalObject, TransactionalObject, TransactionalStorageProtocol,
    singleton::ObjectStoreSingletonStorageProtocol,
};

use crate::{
    cluster::ClusterStore,
    error::{Error, NotFoundSnafu, Result, txn_error},
};

/// Access table metadata.
#[derive(Clone)]
pub struct TableStore {
    cluster: ClusterStore,
    store: Arc<dyn TransactionalStorageProtocol<TableManifest, UpdateVersion>>,
    name: TableName,
}

/// A table stored on object storage.
#[derive(Clone)]
pub struct StoredTable {
    manifest: SimpleTransactionalObject<TableManifest, UpdateVersion>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableManifest {
    pub table: Table,
    #[serde(with = "time::serde::iso8601")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::iso8601")]
    pub updated_at: OffsetDateTime,
}

pub struct TableManifestJsonCodec;

impl TableStore {
    pub(crate) fn new(cluster: ClusterStore, name: TableName) -> Self {
        let store = Arc::new(ObjectStoreSingletonStorageProtocol::new(
            cluster.object_store.clone(),
            manifest_path(&name),
            Box::new(TableManifestJsonCodec),
        ));

        Self {
            cluster,
            store,
            name,
        }
    }

    pub async fn init(&self, options: TableOptions) -> Result<StoredTable> {
        let table = Table::new(self.name.clone(), options).map_err(|err| Error::InvalidTable {
            name: self.name.to_string(),
            message: err.to_string(),
        })?;
        let now = self.cluster.clock.now();
        let manifest = TableManifest {
            table,
            created_at: now,
            updated_at: now,
        };

        SimpleTransactionalObject::init(self.store.clone(), manifest)
            .await
            .map(|manifest| StoredTable { manifest })
            .map_err(|err| txn_error(manifest_path(&self.name), err))
    }

    pub async fn load(&self) -> Result<StoredTable> {
        let manifest = SimpleTransactionalObject::load(self.store.clone())
            .await
            .map_err(|err| txn_error(manifest_path(&self.name), err))?;

        Ok(StoredTable { manifest })
    }

    pub async fn try_load(&self) -> Result<Option<StoredTable>> {
        match self.load().await {
            Ok(table) => Ok(Some(table)),
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

impl StoredTable {
    pub fn manifest(&self) -> &TableManifest {
        self.manifest.object()
    }

    pub fn table(&self) -> Table {
        self.manifest().table.clone()
    }
}

pub(crate) fn manifest_path(name: &TableName) -> Path {
    Path::from(format!("{name}/manifest.json"))
}

impl ObjectCodec<TableManifest> for TableManifestJsonCodec {
    fn encode(&self, value: &TableManifest) -> Bytes {
        serde_json::to_vec(value)
            .expect("TableManifestJsonCodec failed")
            .into()
    }

    fn decode(
        &self,
        bytes: &Bytes,
    ) -> Result<TableManifest, Box<dyn std::error::Error + Send + Sync>> {
        serde_json::from_slice(bytes)
            .map_err(|err| Box::new(err) as Box<dyn std::error::Error + Send + Sync + 'static>)
    }
}

impl std::fmt::Debug for StoredTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoredTable")
            .field("name", &self.manifest().table.name)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use object_store::{ObjectStoreExt, PutPayload, memory::InMemory};
    use wings_resources::{NamespaceName, TableName, TableOptions};
    use wings_schema::{DataType, Field, SchemaBuilder};

    use super::*;
    use crate::test_util::new_test_cluster_store;

    fn namespace_name() -> NamespaceName {
        NamespaceName::new("test-namespace").unwrap()
    }

    fn table_name() -> TableName {
        TableName::new("test-table", namespace_name()).unwrap()
    }

    fn table_options() -> TableOptions {
        TableOptions::new(
            SchemaBuilder::new(vec![
                Field::new("id", 0, DataType::Int64, false),
                Field::new("version", 1, DataType::UInt64, false),
                Field::new("message", 2, DataType::Utf8, false),
            ])
            .build()
            .unwrap(),
            0,
            1,
        )
    }

    #[tokio::test]
    async fn cluster_table_does_not_check_namespace_exists() {
        let store = new_test_cluster_store(Arc::new(InMemory::new()));
        let name = table_name();

        let stored = store
            .table(name.clone())
            .init(table_options())
            .await
            .unwrap();

        assert_eq!(stored.table().name, name);
    }

    #[tokio::test]
    async fn namespace_table_accepts_current_namespace_parent() {
        let store = new_test_cluster_store(Arc::new(InMemory::new()));
        let name = table_name();

        let stored = store
            .namespace(namespace_name())
            .table(name.clone())
            .unwrap()
            .init(table_options())
            .await
            .unwrap();

        assert_eq!(stored.table().name, name);
    }

    #[test]
    fn namespace_table_rejects_different_namespace_parent() {
        let store = new_test_cluster_store(Arc::new(InMemory::new()));
        let other_namespace = NamespaceName::new("other-namespace").unwrap();
        let name = TableName::new("test-table", other_namespace).unwrap();

        let err = store.namespace(namespace_name()).table(name).err().unwrap();

        insta::assert_compact_debug_snapshot!(err, @r#"InvalidResourceParent { name: "namespaces/other-namespace/tables/test-table", expected_parent: "namespaces/test-namespace", actual_parent: "namespaces/other-namespace" }"#);
    }

    #[tokio::test]
    async fn init_stores_manifest() {
        let store = new_test_cluster_store(Arc::new(InMemory::new()));
        let name = table_name();

        let manifest = store
            .table(name.clone())
            .init(table_options())
            .await
            .unwrap();

        assert_eq!(manifest.manifest().table.name, name);
        assert_eq!(
            manifest.manifest().created_at,
            OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap()
        );
        assert_eq!(
            manifest.manifest().created_at,
            manifest.manifest().updated_at
        );
    }

    #[tokio::test]
    async fn load_reads_manifest() {
        let store = new_test_cluster_store(Arc::new(InMemory::new()));
        let name = table_name();
        let table_store = store.table(name.clone());

        table_store.init(table_options()).await.unwrap();

        let table = table_store.load().await.unwrap().table();
        let expected = Table::new(name, table_options()).unwrap();

        assert_eq!(table, expected);
    }

    #[tokio::test]
    async fn delete_table_deletes_manifest() {
        let store = new_test_cluster_store(Arc::new(InMemory::new()));
        let name = table_name();
        store
            .table(name.clone())
            .init(table_options())
            .await
            .unwrap();

        store.table(name.clone()).delete().await.unwrap();

        let err = store.table(name).load().await.unwrap_err();

        insta::assert_compact_debug_snapshot!(err, @r#"NotFound { path: "namespaces/test-namespace/tables/test-table/manifest.json" }"#);
    }

    #[tokio::test]
    async fn write_manifest_fails_when_manifest_already_exists() {
        let store = new_test_cluster_store(Arc::new(InMemory::new()));
        let name = table_name();

        store
            .table(name.clone())
            .init(table_options())
            .await
            .unwrap();

        let err = store.table(name).init(table_options()).await.unwrap_err();

        insta::assert_compact_debug_snapshot!(err, @r#"AlreadyExists { path: "namespaces/test-namespace/tables/test-table/manifest.json" }"#);
    }

    #[tokio::test]
    async fn try_read_manifest_returns_none_when_manifest_does_not_exist() {
        let store = new_test_cluster_store(Arc::new(InMemory::new()));

        assert!(
            store
                .table(table_name())
                .try_load()
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn read_manifest_fails_when_manifest_does_not_exist() {
        let store = new_test_cluster_store(Arc::new(InMemory::new()));

        let err = store.table(table_name()).load().await.unwrap_err();

        insta::assert_compact_debug_snapshot!(err, @r#"NotFound { path: "namespaces/test-namespace/tables/test-table/manifest.json" }"#);
    }

    #[tokio::test]
    async fn read_manifest_fails_when_manifest_is_invalid_json() {
        let object_store = Arc::new(InMemory::new());
        let store = new_test_cluster_store(object_store.clone());
        let name = table_name();

        object_store
            .put(
                &manifest_path(&name),
                PutPayload::from(Bytes::from_static(b"not json")),
            )
            .await
            .unwrap();

        let err = store.table(name).load().await.unwrap_err();

        insta::assert_compact_debug_snapshot!(err, @r#"Decode { path: "namespaces/test-namespace/tables/test-table/manifest.json" }"#);
    }

    #[tokio::test]
    async fn init_rejects_invalid_table_options() {
        let store = new_test_cluster_store(Arc::new(InMemory::new()));
        let options = TableOptions::new(table_options().schema, 42, 1);

        let err = store.table(table_name()).init(options).await.unwrap_err();

        insta::assert_compact_debug_snapshot!(err, @r#"InvalidTable { name: "namespaces/test-namespace/tables/test-table", message: "key field id 42 is not a root schema field" }"#);
    }
}
