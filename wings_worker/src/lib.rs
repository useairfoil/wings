use std::{sync::Arc, time::Duration};

use futures::{TryStreamExt, stream::FuturesUnordered};
use snafu::ResultExt;
use tokio_util::sync::CancellationToken;
use tracing::warn;
use wings_control_plane_core::{
    cluster_metadata::{
        ClusterMetadata,
        cache::{NamespaceCache, TopicCache},
    },
    log_metadata::{LogMetadata, RequestTaskRequest, RequestTaskResponse, Task},
};
use wings_data_lake::DataLakeFactory;
use wings_object_store::ObjectStoreFactory;
use wings_server_core::query::NamespaceProviderFactory;

mod error;
mod tasks;

use crate::error::LogMetadataSnafu;
pub use crate::error::{Result, WorkerPoolError};

#[derive(Clone)]
pub struct Worker {
    pub(crate) topic_cache: TopicCache,
    pub(crate) namespace_cache: NamespaceCache,
    pub(crate) log_meta: Arc<dyn LogMetadata>,
    pub(crate) data_lake_factory: DataLakeFactory,
    pub(crate) namespace_provider_factory: NamespaceProviderFactory,
}

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

impl Worker {
    pub fn new(
        topic_cache: TopicCache,
        namespace_cache: NamespaceCache,
        log_meta: Arc<dyn LogMetadata>,
        cluster_meta: Arc<dyn ClusterMetadata>,
        object_store_factory: Arc<dyn ObjectStoreFactory>,
        namespace_provider_factory: NamespaceProviderFactory,
    ) -> Self {
        let data_lake_factory = DataLakeFactory::new(cluster_meta, object_store_factory).clone();
        Self {
            topic_cache,
            namespace_cache,
            log_meta,
            data_lake_factory,
            namespace_provider_factory,
        }
    }

    pub async fn run(self, ct: CancellationToken) -> Result<()> {
        'outer: loop {
            let RequestTaskResponse { task } = self
                .log_meta
                .request_task(RequestTaskRequest::default())
                .await
                .context(LogMetadataSnafu {
                    operation: "request_task",
                })?;

            let Some(task) = task else {
                continue;
            };

            loop {
                // Keep retrying until the task is executed successfully, since tasks are not supposed to fail.
                match self.execute_task(&task, ct.clone()).await {
                    Ok(_) => {
                        // The task finished because it was cancelled.
                        if ct.is_cancelled() {
                            return Ok(());
                        }

                        // Get the next task.
                        continue 'outer;
                    }
                    Err(err) => {
                        warn!(err = ?err, "Task execution failed. Retrying.");
                        tokio::time::sleep(Duration::from_secs(3)).await;
                    }
                }
            }
        }
    }

    async fn execute_task(&self, task: &Task, ct: CancellationToken) -> Result<()> {
        match task {
            Task::Compaction { metadata, task } => {
                self.execute_compaction_task(metadata, task, ct).await
            }
            Task::CreateTable { metadata, task } => {
                self.execute_create_table_task(metadata, task, ct).await
            }
            Task::Commit { metadata, task } => self.execute_commit_task(metadata, task, ct).await,
        }
    }
}

impl Default for WorkerPoolOptions {
    fn default() -> Self {
        Self { worker_count: 4 }
    }
}
