use std::sync::Arc;

use wings_resources::{NamespaceName, Table};

use super::{ClusterMetadata, ListTablesRequest, Result};

#[derive(Debug, Clone)]
pub struct CollectNamespaceTablesOptions {
    pub page_size: usize,
}

pub async fn collect_namespace_tables(
    cluster_meta: &Arc<dyn ClusterMetadata>,
    namespace: &NamespaceName,
    options: CollectNamespaceTablesOptions,
) -> Result<Vec<Table>> {
    let mut page_token = None;
    let mut tables = Vec::new();
    loop {
        let mut response = cluster_meta
            .list_tables(ListTablesRequest {
                parent: namespace.clone(),
                page_size: options.page_size.into(),
                page_token: page_token.clone(),
            })
            .await?;
        tables.append(&mut response.tables);
        page_token = response.next_page_token;
        if page_token.is_none() {
            break;
        }
    }

    Ok(tables)
}

impl Default for CollectNamespaceTablesOptions {
    fn default() -> Self {
        Self { page_size: 100 }
    }
}
