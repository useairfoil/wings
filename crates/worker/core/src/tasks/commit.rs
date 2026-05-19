use snafu::ResultExt;
use tracing::{info, warn};
use wings_control_plane_core::table_metadata::{
    CommitResult, CommitTask, CompleteTaskRequest, TaskMetadata, TaskResult,
};

use crate::{
    Worker,
    error::{ClusterMetadataSnafu, DataLakeSnafu, TableMetadataSnafu, Result},
};

impl Worker {
    pub async fn execute_commit_task(
        &self,
        metadata: &TaskMetadata,
        task: &CommitTask,
        _ct: tokio_util::sync::CancellationToken,
    ) -> Result<()> {
        info!(
            table_name = %task.table_name,
            task_id = %metadata.task_id,
            "Executing commit task"
        );

        let namespace_name = task.table_name.parent().clone();

        let table_ref = match self.table_cache.get(task.table_name.clone()).await {
            Ok(table_ref) => table_ref,
            Err(err) => {
                if err.is_not_found() {
                    warn!(
                        table = %task.table_name,
                        "received commit task for non-existent table"
                    );
                    return Ok(());
                }
                return Err(err).context(ClusterMetadataSnafu {
                    operation: "get_table",
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
            .commit_data(table_ref, &task.new_files)
            .await
            .context(DataLakeSnafu {
                operation: "commit_data",
            })?;

        let result = TaskResult::Commit(CommitResult {
            table_version: table_version.clone(),
        });

        self.table_metadata
            .complete_task(CompleteTaskRequest::new_completed(
                metadata.task_id.clone(),
                result,
            ))
            .await
            .context(TableMetadataSnafu {
                operation: "complete_task",
            })?;

        info!(
            task_id = %metadata.task_id,
            table_name = %task.table_name,
            table_version = %table_version,
            "Commit task completed"
        );

        Ok(())
    }
}
