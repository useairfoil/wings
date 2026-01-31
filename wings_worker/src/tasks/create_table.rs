use snafu::ResultExt;
use tracing::{info, warn};
use wings_control_plane_core::log_metadata::{
    CompleteTaskRequest, CreateTableResult, CreateTableTask, TaskMetadata, TaskResult,
};

use crate::{
    Worker,
    error::{ClusterMetadataSnafu, DataLakeSnafu, LogMetadataSnafu, Result},
};

impl Worker {
    pub async fn execute_create_table_task(
        &self,
        metadata: &TaskMetadata,
        task: &CreateTableTask,
        _ct: tokio_util::sync::CancellationToken,
    ) -> Result<()> {
        info!(
            topic_name = %task.topic_name,
            task_id = %metadata.task_id,
            "Executing create table task"
        );

        let namespace_name = task.topic_name.parent().clone();

        let topic_ref = match self.topic_cache.get(task.topic_name.clone()).await {
            Ok(topic_ref) => topic_ref,
            Err(err) => {
                if err.is_not_found() {
                    warn!(
                        topic = %task.topic_name,
                        "received create table task for non-existent topic"
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

        let data_lake = self
            .data_lake_factory
            .create_data_lake(namespace_ref.clone())
            .await
            .context(DataLakeSnafu {
                operation: "create",
            })?;

        let complete = match data_lake.create_table(topic_ref).await {
            Ok(table_id) => {
                let result = TaskResult::CreateTable(CreateTableResult { table_id });
                CompleteTaskRequest::new_completed(metadata.task_id.clone(), result)
            }
            Err(err) => CompleteTaskRequest::new_failed(metadata.task_id.clone(), err.to_string()),
        };

        self.log_meta
            .complete_task(complete)
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
}
