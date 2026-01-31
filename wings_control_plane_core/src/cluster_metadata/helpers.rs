use std::sync::Arc;

use wings_resources::{NamespaceName, Topic};

use super::{ClusterMetadata, ListTopicsRequest, Result};

#[derive(Debug, Clone)]
pub struct CollectNamespaceTopicsOptions {
    pub page_size: usize,
}

pub async fn collect_namespace_topics(
    cluster_meta: &Arc<dyn ClusterMetadata>,
    namespace: &NamespaceName,
    options: CollectNamespaceTopicsOptions,
) -> Result<Vec<Topic>> {
    let mut page_token = None;
    let mut topics = Vec::new();
    loop {
        let mut response = cluster_meta
            .list_topics(ListTopicsRequest {
                parent: namespace.clone(),
                page_size: options.page_size.into(),
                page_token: page_token.clone(),
            })
            .await?;
        topics.append(&mut response.topics);
        page_token = response.next_page_token;
        if page_token.is_none() {
            break;
        }
    }

    Ok(topics)
}

impl Default for CollectNamespaceTopicsOptions {
    fn default() -> Self {
        Self { page_size: 100 }
    }
}
