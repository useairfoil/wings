use std::sync::Arc;

use object_store::{ObjectStore, path::Path};
use serde::{Deserialize, Serialize};
use wings_dst_base::{Clock, ThreadRng};
use wings_resources::{NamespaceName, TableName};
use wings_secret_manager::SecretManager;

use crate::{error::Result, namespace::NamespaceStore, table::TableStore};

#[derive(Clone)]
pub struct ClusterStore {
    pub(crate) object_store: Arc<dyn ObjectStore>,
    pub(crate) secret_manager: Arc<dyn SecretManager>,
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) rng: Arc<ThreadRng>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespacesPage {
    pub namespaces: Vec<NamespaceName>,
    pub next_page_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TablesPage {
    pub tables: Vec<TableName>,
    pub next_page_token: Option<String>,
}

impl ClusterStore {
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        secret_manager: Arc<dyn SecretManager>,
        clock: Arc<dyn Clock>,
        rng: Arc<ThreadRng>,
    ) -> Self {
        Self {
            object_store,
            secret_manager,
            clock,
            rng,
        }
    }

    pub fn namespace(&self, name: NamespaceName) -> NamespaceStore {
        NamespaceStore::new(self.clone(), name)
    }

    pub fn table(&self, name: TableName) -> TableStore {
        TableStore::new(self.clone(), name)
    }

    pub async fn list_namespaces(
        &self,
        page_size: Option<usize>,
        page_token: Option<String>,
    ) -> Result<NamespacesPage> {
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

        Ok(NamespacesPage {
            namespaces: names,
            next_page_token,
        })
    }

    pub async fn list_tables(
        &self,
        namespace: NamespaceName,
        page_size: Option<usize>,
        page_token: Option<String>,
    ) -> Result<TablesPage> {
        let prefix = Path::from(format!("{namespace}/tables"));
        let page_size = page_size.unwrap_or(100).clamp(1, 1000);
        let mut table_names = self
            .object_store
            .list_with_delimiter(Some(&prefix))
            .await?
            .common_prefixes
            .into_iter()
            .filter_map(|path| TableName::parse(path.as_ref()).ok())
            .collect::<Vec<_>>();

        table_names.sort_by_key(|name| name.to_string());

        let mut names: Vec<TableName> = Vec::new();
        let mut next_page_token = None;
        for name in table_names {
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

        Ok(TablesPage {
            tables: names,
            next_page_token,
        })
    }
}

#[cfg(test)]
mod tests {

    use object_store::memory::InMemory;
    use wings_resources::TableOptions;
    use wings_schema::{DataType, Field, SchemaBuilder};

    use super::*;
    use crate::test_util::{new_test_cluster_store, new_test_namespace_options};

    fn table_options() -> TableOptions {
        TableOptions::new(
            SchemaBuilder::new(vec![
                Field::new("id", 0, DataType::Int64, false),
                Field::new("version", 1, DataType::UInt64, false),
            ])
            .build()
            .unwrap(),
            0,
            1,
        )
    }

    #[tokio::test]
    async fn list_namespace_names_returns_namespace_folders_in_path_order() {
        let store = new_test_cluster_store(Arc::new(InMemory::new()));

        for i in (0..10).rev() {
            let name = NamespaceName::new_unchecked(format!("doc-{:0>2}", i));
            store
                .namespace(name)
                .init(new_test_namespace_options())
                .await
                .unwrap();
        }

        let out = store.list_namespaces(None, None).await.unwrap();
        insta::assert_yaml_snapshot!(out, @r"
        namespaces:
          - namespaces/doc-00
          - namespaces/doc-01
          - namespaces/doc-02
          - namespaces/doc-03
          - namespaces/doc-04
          - namespaces/doc-05
          - namespaces/doc-06
          - namespaces/doc-07
          - namespaces/doc-08
          - namespaces/doc-09
        next_page_token: ~
        ");
    }

    #[tokio::test]
    async fn list_namespace_names_paginates_with_next_page_token() {
        let store = new_test_cluster_store(Arc::new(InMemory::new()));

        for i in (0..10).rev() {
            let name = NamespaceName::new_unchecked(format!("doc-{:0>2}", i));
            store
                .namespace(name)
                .init(new_test_namespace_options())
                .await
                .unwrap();
        }

        let first_page = store.list_namespaces(Some(2), None).await.unwrap();

        insta::assert_yaml_snapshot!(first_page, @r"
        namespaces:
          - namespaces/doc-00
          - namespaces/doc-01
        next_page_token: namespaces/doc-01
        ");

        let second_page = store
            .list_namespaces(Some(4), first_page.next_page_token)
            .await
            .unwrap();

        insta::assert_yaml_snapshot!(second_page, @r"
        namespaces:
          - namespaces/doc-02
          - namespaces/doc-03
          - namespaces/doc-04
          - namespaces/doc-05
        next_page_token: namespaces/doc-05
        ");

        let third_page = store
            .list_namespaces(Some(4), second_page.next_page_token)
            .await
            .unwrap();

        insta::assert_yaml_snapshot!(third_page, @r"
        namespaces:
          - namespaces/doc-06
          - namespaces/doc-07
          - namespaces/doc-08
          - namespaces/doc-09
        next_page_token: ~
        ");
    }

    #[tokio::test]
    async fn list_table_names_returns_table_folders_in_path_order() {
        let store = new_test_cluster_store(Arc::new(InMemory::new()));
        let namespace = NamespaceName::new_unchecked("test-namespace");

        for i in (0..10).rev() {
            let name = TableName::new_unchecked(format!("table-{:0>2}", i), namespace.clone());
            store.table(name).init(table_options()).await.unwrap();
        }

        let out = store.list_tables(namespace, None, None).await.unwrap();
        insta::assert_yaml_snapshot!(out, @r"
        tables:
          - namespaces/test-namespace/tables/table-00
          - namespaces/test-namespace/tables/table-01
          - namespaces/test-namespace/tables/table-02
          - namespaces/test-namespace/tables/table-03
          - namespaces/test-namespace/tables/table-04
          - namespaces/test-namespace/tables/table-05
          - namespaces/test-namespace/tables/table-06
          - namespaces/test-namespace/tables/table-07
          - namespaces/test-namespace/tables/table-08
          - namespaces/test-namespace/tables/table-09
        next_page_token: ~
        ");
    }

    #[tokio::test]
    async fn list_table_names_paginates_with_next_page_token() {
        let store = new_test_cluster_store(Arc::new(InMemory::new()));
        let namespace = NamespaceName::new_unchecked("test-namespace");

        for i in (0..10).rev() {
            let name = TableName::new_unchecked(format!("table-{:0>2}", i), namespace.clone());
            store.table(name).init(table_options()).await.unwrap();
        }

        let first_page = store
            .list_tables(namespace.clone(), Some(2), None)
            .await
            .unwrap();

        insta::assert_yaml_snapshot!(first_page, @r"
        tables:
          - namespaces/test-namespace/tables/table-00
          - namespaces/test-namespace/tables/table-01
        next_page_token: namespaces/test-namespace/tables/table-01
        ");

        let second_page = store
            .list_tables(namespace.clone(), Some(4), first_page.next_page_token)
            .await
            .unwrap();

        insta::assert_yaml_snapshot!(second_page, @r"
        tables:
          - namespaces/test-namespace/tables/table-02
          - namespaces/test-namespace/tables/table-03
          - namespaces/test-namespace/tables/table-04
          - namespaces/test-namespace/tables/table-05
        next_page_token: namespaces/test-namespace/tables/table-05
        ");

        let third_page = store
            .list_tables(namespace, Some(4), second_page.next_page_token)
            .await
            .unwrap();

        insta::assert_yaml_snapshot!(third_page, @r"
        tables:
          - namespaces/test-namespace/tables/table-06
          - namespaces/test-namespace/tables/table-07
          - namespaces/test-namespace/tables/table-08
          - namespaces/test-namespace/tables/table-09
        next_page_token: ~
        ");
    }
}
