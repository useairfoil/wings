use std::sync::Arc;

use futures::{TryStreamExt, stream::FuturesUnordered};
use tokio_util::sync::CancellationToken;
use tracing::warn;
use wings_control_plane::{
    cluster_metadata::{
        ClusterMetadata,
        cache::{NamespaceCache, TopicCache},
    },
    log_metadata::LogMetadata,
    object_store::ObjectStoreFactory,
};
use wings_server_core::query::NamespaceProviderFactory;

mod error;
mod worker;

use crate::{
    error::{Result, WorkerPoolError},
    worker::Worker,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerPoolOptions {
    pub worker_count: usize,
}

pub struct WorkerPool {
    worker: Worker,
    options: WorkerPoolOptions,
}

pub async fn run_worker_pool(
    pool: WorkerPool,
    ct: CancellationToken,
) -> Result<(), WorkerPoolError> {
    pool.run(ct).await
}

impl WorkerPool {
    pub fn new(
        topic_cache: TopicCache,
        namespace_cache: NamespaceCache,
        log_meta: Arc<dyn LogMetadata>,
        cluster_meta: Arc<dyn ClusterMetadata>,
        object_store_factory: Arc<dyn ObjectStoreFactory>,
        namespace_provider_factory: NamespaceProviderFactory,
        options: WorkerPoolOptions,
    ) -> Self {
        let worker = Worker::new(
            topic_cache,
            namespace_cache,
            log_meta,
            cluster_meta,
            object_store_factory,
            namespace_provider_factory,
        );

        WorkerPool { worker, options }
    }

    pub async fn run(self, ct: CancellationToken) -> Result<(), WorkerPoolError> {
        let mut workers_fut = FuturesUnordered::new();
        let worker = self.worker.clone();
        for _ in 0..self.options.worker_count {
            workers_fut.push(worker.clone().run(ct.clone()));
        }

        loop {
            tokio::select! {
                _ = ct.cancelled() => {
                    break;
                }
                res = workers_fut.try_next() => {
                    warn!(result = ?res, "Worker exited");

                    while workers_fut.len() < self.options.worker_count {
                        workers_fut.push(worker.clone().run(ct.clone()));
                    }
                }
            }
        }

        Ok(())
    }
}

impl Default for WorkerPoolOptions {
    fn default() -> Self {
        Self { worker_count: 4 }
    }
}
