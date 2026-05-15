use futures::TryStreamExt;
use snafu::ResultExt;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use wings_control_plane_core::log_metadata::{
    CompactionOperation, CompactionResult, CompactionTask, CompleteTaskRequest, TaskMetadata,
    TaskResult,
};
use wings_query::TopicLogicalPlanExt;

use crate::{
    Worker,
    error::{ClusterMetadataSnafu, DataLakeSnafu, LogMetadataSnafu, Result},
};

impl Worker {
    pub async fn execute_compaction_task(
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
            .await?;

        let ctx = provider.new_session_context().await?;

        let mut partition_columns = Vec::new();
        if let Some(field) = topic_ref.partition_field() {
            partition_columns.push(field.name());
        }

        let plan = topic_ref
            .logical_plan(
                &ctx,
                task.start_offset,
                task.end_offset,
                task.partition_value.clone(),
            )
            .await?;

        let df = ctx
            .execute_logical_plan(plan)
            .await?
            .drop_columns(&partition_columns)?;

        let mut stream = df.execute_stream().await?;

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
                task.partition_value.clone(),
                task.start_offset,
                task.end_offset,
                task.target_file_size,
            )
            .await
            .context(DataLakeSnafu {
                operation: "create writer",
            })?;

        while let Some(batch) = stream.try_next().await? {
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

        let new_files = data_lake_writer.finish().await.context(DataLakeSnafu {
            operation: "commit",
        })?;

        info!(
            task_id = metadata.task_id,
            new_files_count = new_files.len(),
            "Compaction task completed"
        );

        let result = TaskResult::Compaction(CompactionResult {
            new_files,
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

        Ok(())
    }
}
