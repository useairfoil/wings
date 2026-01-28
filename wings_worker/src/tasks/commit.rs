use snafu::ResultExt;
use tracing::info;
use wings_control_plane::{
    log_metadata::{
        CommitResult, CommitTask, CompleteTaskRequest, 
        TaskMetadata, TaskResult,
    },
};

use crate::error::{
    LogMetadataSnafu, Result,
};
use crate::Worker;

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

        // TODO: Implement actual commit logic here
        // For now, we'll just mark it as completed

        let result = TaskResult::Commit(CommitResult {
            table_version: "0".to_string(),
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
            "Commit task completed"
        );

        Ok(())
    }
}