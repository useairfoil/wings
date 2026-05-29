use std::sync::Arc;

use object_store::{ObjectStore, path::Path};
use serde::{Deserialize, Serialize};
use wings_dst_base::{Clock, ThreadRng};
use wings_resources::NamespaceName;
use wings_secret_manager::SecretManager;

use crate::{error::Result, namespace::NamespaceStore};

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
}

#[cfg(test)]
mod tests {

    use object_store::memory::InMemory;

    use super::*;
    use crate::test_util::{new_test_cluster_store, new_test_namespace_options};

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
}
