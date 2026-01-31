use std::{sync::Arc, time::Duration};

use wings_resources::{NamespaceName, NamespaceRef, TopicName, TopicRef};

use super::{ClusterMetadata, Result};

pub struct CacheOptions {
    max_capacity: usize,
    time_to_live: Duration,
    time_to_idle: Duration,
}

#[derive(Clone)]
pub struct TopicCache {
    cluster_meta: Arc<dyn ClusterMetadata>,
    inner: moka::future::Cache<TopicName, TopicRef>,
}

#[derive(Clone)]
pub struct NamespaceCache {
    cluster_meta: Arc<dyn ClusterMetadata>,
    inner: moka::future::Cache<NamespaceName, NamespaceRef>,
}

impl TopicCache {
    pub fn new(cluster_meta: Arc<dyn ClusterMetadata>) -> Self {
        Self::with_options(cluster_meta, CacheOptions::default())
    }

    pub fn with_options(cluster_meta: Arc<dyn ClusterMetadata>, options: CacheOptions) -> Self {
        let inner = moka::future::Cache::builder()
            .max_capacity(options.max_capacity as u64)
            .time_to_live(options.time_to_live)
            .time_to_idle(options.time_to_idle)
            .build();

        Self {
            cluster_meta,
            inner,
        }
    }

    pub async fn get(&self, name: TopicName) -> Result<TopicRef> {
        let cluster_meta = self.cluster_meta.clone();
        let topic = self
            .inner
            .try_get_with(name.clone(), async move {
                cluster_meta.get_topic(name).await.map(Arc::new)
            })
            .await
            .map_err(|err| {
                Arc::try_unwrap(err).unwrap_or_else(|e| {
                    use super::error::ClusterMetadataError;
                    ClusterMetadataError::Internal {
                        message: format!("Failed to unwrap Arc: {}", e),
                    }
                })
            })?;
        Ok(topic)
    }
}

impl NamespaceCache {
    pub fn new(cluster_meta: Arc<dyn ClusterMetadata>) -> Self {
        Self::with_options(cluster_meta, CacheOptions::default())
    }

    pub fn with_options(cluster_meta: Arc<dyn ClusterMetadata>, options: CacheOptions) -> Self {
        let inner = moka::future::Cache::builder()
            .max_capacity(options.max_capacity as u64)
            .time_to_live(options.time_to_live)
            .time_to_idle(options.time_to_idle)
            .build();

        Self {
            cluster_meta,
            inner,
        }
    }

    pub async fn get(&self, name: NamespaceName) -> Result<NamespaceRef> {
        let admin = self.cluster_meta.clone();
        let namespace = self
            .inner
            .try_get_with(name.clone(), async move {
                admin.get_namespace(name).await.map(Arc::new)
            })
            .await
            .map_err(|e| {
                Arc::try_unwrap(e).unwrap_or_else(|e| {
                    use super::error::ClusterMetadataError;
                    ClusterMetadataError::Internal {
                        message: format!("Failed to unwrap Arc: {}", e),
                    }
                })
            })?;
        Ok(namespace)
    }
}

impl CacheOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_max_capacity(mut self, max_capacity: usize) -> Self {
        self.max_capacity = max_capacity;
        self
    }

    pub fn with_time_to_live(mut self, time_to_live: Duration) -> Self {
        self.time_to_live = time_to_live;
        self
    }

    pub fn with_time_to_idle(mut self, time_to_idle: Duration) -> Self {
        self.time_to_idle = time_to_idle;
        self
    }
}

impl Default for CacheOptions {
    fn default() -> Self {
        Self {
            max_capacity: 1024,
            time_to_live: Duration::from_secs(30 * 60), // 30 minutes
            time_to_idle: Duration::from_secs(5 * 60),  // 5 minutes
        }
    }
}
