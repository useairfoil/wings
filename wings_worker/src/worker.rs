use std::{sync::Arc, time::Duration};

use futures::TryStreamExt;
use snafu::ResultExt;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use wings_control_plane::{
    cluster_metadata::{
        ClusterMetadata,
        cache::{NamespaceCache, TopicCache},
    },
    data_lake::DataLakeFactory,
    log_metadata::{
        CommitResult, CommitTask, CompactionOperation, CompactionResult, CompactionTask,
        CompleteTaskRequest, CreateTableResult, CreateTableTask, LogMetadata, RequestTaskRequest,
        RequestTaskResponse, Task, TaskMetadata, TaskResult,
    },
    object_store::ObjectStoreFactory,
};
use wings_server_core::query::NamespaceProviderFactory;

use crate::error::{
    ClusterMetadataSnafu, DataFusionSnafu, DataLakeSnafu, LogMetadataSnafu, Result,
};

#[derive(Clone)]
pub struct Worker {
    topic_cache: TopicCache,
    namespace_cache: NamespaceCache,
    log_meta: Arc<dyn LogMetadata>,
    data_lake_factory: DataLakeFactory,
    namespace_provider_factory: NamespaceProviderFactory,
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

    async fn execute_compaction_task(
        &self,
        metadata: &TaskMetadata,
        task: &CompactionTask,
        ct: CancellationToken,
    ) -> Result<()> {
        let namespace_name = task.topic_name.parent().clone();

        let topic_ref = match self.topic_cache.get(task.topic_name.clone()).await {
            Ok(topic_ref) => topic_ref,
            Err(err) => {
                if err.is_not_found() {
                    warn!(
                        topic = %task.topic_name,
                        "received compaction task for non-existent topic"
                    );
                    return Ok(());
                }
                return Err(err).context(ClusterMetadataSnafu {
                    operation: "get_topic",
                });
            }
        };

        let namespace_ref = self
            .namespace_cache
            .get(namespace_name.clone())
            .await
            .context(ClusterMetadataSnafu {
                operation: "get_namespace",
            })?;

        let provider = self
            .namespace_provider_factory
            .create_provider(namespace_name)
            .await
            .context(DataFusionSnafu {})?;

        let ctx = provider
            .new_session_context()
            .await
            .context(DataFusionSnafu {})?;

        // TODO: rewrite all of this to build the plan programatically
        let partition_query = if let Some(field) = topic_ref.partition_field() {
            format!(
                "AND {} = {}",
                field.name,
                task.partition_value
                    .as_ref()
                    .map(|v| v.to_string())
                    .unwrap_or_default()
            )
        } else {
            String::new()
        };

        let query = format!(
            "SELECT * FROM {} WHERE __offset__ BETWEEN {} AND {} {} ORDER BY __offset__ ASC",
            task.topic_name.id(),
            task.start_offset,
            task.end_offset,
            partition_query
        );

        println!("Compaction query: {}", query);

        let df = ctx.sql(&query).await.context(DataFusionSnafu {})?;

        let output_schema: Arc<_> = df.schema().as_arrow().clone().into();

        let mut stream = df.execute_stream().await.context(DataFusionSnafu {})?;

        let data_lake = self
            .data_lake_factory
            .create_data_lake(namespace_ref.clone())
            .await
            .context(DataLakeSnafu {
                operation: "create",
            })?;

        let mut data_lake_writer = data_lake
            .batch_writer(
                topic_ref.clone(),
                output_schema,
                task.partition_value.clone(),
                task.start_offset,
                task.end_offset,
            )
            .await
            .context(DataLakeSnafu {
                operation: "create writer",
            })?;

        while let Some(batch) = stream.try_next().await.context(DataFusionSnafu {})? {
            if ct.is_cancelled() {
                return Ok(());
            }

            data_lake_writer
                .write_batch(batch)
                .await
                .context(DataLakeSnafu {
                    operation: "write batch",
                })?;
        }

        if ct.is_cancelled() {
            return Ok(());
        }

        data_lake_writer.commit().await.context(DataLakeSnafu {
            operation: "commit",
        })?;

        // Later: update datalake catalog with this data.
        // Notice that multiple partitions may be compacted at the same time
        // so we need to be careful updating the catalog concurrently.

        // TODO: we should include the compacted range and file reference in the complete task request.
        let result = TaskResult::Compaction(CompactionResult {
            new_files: Vec::default(),
            operation: CompactionOperation::Append,
        });

        self.log_meta
            .complete_task(CompleteTaskRequest::new_completed(
                metadata.task_id.clone(),
                result,
            ))
            .await
            .context(LogMetadataSnafu {
                operation: "complete_task",
            })?;

        info!(
            task_id = metadata.task_id,
            // file_ref = %file_ref,
            "Compaction task completed"
        );

        Ok(())
    }

    async fn execute_create_table_task(
        &self,
        metadata: &TaskMetadata,
        task: &CreateTableTask,
        _ct: CancellationToken,
    ) -> Result<()> {
        info!(
            topic_name = %task.topic_name,
            task_id = %metadata.task_id,
            "Executing create table task"
        );

        // TODO: Implement actual table creation logic
        // For now, we'll just log and complete the task

        let result = TaskResult::CreateTable(CreateTableResult {
            table_id: task.topic_name.id.clone(),
        });

        self.log_meta
            .complete_task(CompleteTaskRequest::new_completed(
                metadata.task_id.clone(),
                result,
            ))
            .await
            .context(LogMetadataSnafu {
                operation: "complete_task",
            })?;

        info!(
            task_id = %metadata.task_id,
            topic_name = %task.topic_name,
            "Create table task completed"
        );

        Ok(())
    }

    async fn execute_commit_task(
        &self,
        metadata: &TaskMetadata,
        task: &CommitTask,
        _ct: CancellationToken,
    ) -> Result<()> {
        info!(
            topic_name = %task.topic_name,
            task_id = %metadata.task_id,
            "Executing commit task"
        );

        // TODO: Implement actual commit logic here
        // For now, we'll just mark it as completed

        let result = TaskResult::Commit(CommitResult {});

        self.log_meta
            .complete_task(CompleteTaskRequest::new_completed(
                metadata.task_id.clone(),
                result,
            ))
            .await
            .context(LogMetadataSnafu {
                operation: "complete_task",
            })?;

        info!(
            task_id = %metadata.task_id,
            topic_name = %task.topic_name,
            "Commit task completed"
        );

        Ok(())
    }
}
