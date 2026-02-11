use futures::TryStreamExt;
use snafu::ResultExt;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use wings_control_plane_core::log_metadata::{
    CompactionOperation, CompactionResult, CompactionTask, CompleteTaskRequest, TaskMetadata,
    TaskResult,
};

use crate::{
    Worker,
    error::{ClusterMetadataSnafu, DataFusionSnafu, DataLakeSnafu, LogMetadataSnafu, Result},
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
            .await
            .context(DataFusionSnafu {})?;

        let ctx = provider
            .new_session_context()
            .await
            .context(DataFusionSnafu {})?;

        // TODO: rewrite all of this to build the plan programatically
        let mut partition_columns = Vec::new();
        let partition_query = if let Some(field) = topic_ref.partition_field() {
            partition_columns.push(field.name());
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

        // println!("Compaction query: {}", query);

        let df = ctx
            .sql(&query)
            .await
            .context(DataFusionSnafu {})?
            .drop_columns(&partition_columns)
            .context(DataFusionSnafu {})?;

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
                task.partition_value.clone(),
                task.start_offset,
                task.end_offset,
                task.target_file_size,
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
