use snafu::ResultExt;
use tracing::{info, warn};
use wings_control_plane::log_metadata::{
    CommitResult, CommitTask, CompleteTaskRequest, TaskMetadata, TaskResult,
};

use crate::Worker;
use crate::error::{ClusterMetadataSnafu, DataLakeSnafu, LogMetadataSnafu, Result};

impl Worker {
    pub async fn execute_commit_task(
        &self,
        metadata: &TaskMetadata,
        task: &CommitTask,
        _ct: tokio_util::sync::CancellationToken,
    ) -> Result<()> {
        info!(
            topic_name = %task.topic_name,
            task_id = %metadata.task_id,
            "Executing commit task"
        );

        let namespace_name = task.topic_name.parent().clone();

        let topic_ref = match self.topic_cache.get(task.topic_name.clone()).await {
            Ok(topic_ref) => topic_ref,
            Err(err) => {
                if err.is_not_found() {
                    warn!(
                        topic = %task.topic_name,
                        "received commit task for non-existent topic"
                    );
                    return Ok(());
                }
                return Err(err).context(ClusterMetadataSnafu {
                    operation: "get_topic",
                });
            }
        };

        let namespace_ref =
            self.namespace_cache
                .get(namespace_name)
                .await
                .context(ClusterMetadataSnafu {
                    operation: "get_namespace",
                })?;

        let data_lake = self
            .data_lake_factory
            .create_data_lake(namespace_ref)
            .await
            .context(DataLakeSnafu {
                operation: "create_data_lake",
            })?;

        let table_version = data_lake
            .commit_data(topic_ref, &task.new_files)
            .await
            .context(DataLakeSnafu {
                operation: "commit_data",
            })?;

        let result = TaskResult::Commit(CommitResult {
            table_version: table_version.clone(),
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
            table_version = %table_version,
            "Commit task completed"
        );

        Ok(())
    }
}
