use snafu::ResultExt;
use tracing::{info, warn};
use wings_control_plane_core::table_metadata::{
    CompleteTaskRequest, CreateTableResult, CreateTableTask, TaskMetadata, TaskResult,
};

use crate::{
    Worker,
    error::{ClusterMetadataSnafu, DataLakeSnafu, TableMetadataSnafu, Result},
};

impl Worker {
    pub async fn execute_create_table_task(
        &self,
        metadata: &TaskMetadata,
        task: &CreateTableTask,
        _ct: tokio_util::sync::CancellationToken,
    ) -> Result<()> {
        info!(
            table_name = %task.table_name,
            task_id = %metadata.task_id,
            "Executing create table task"
        );

        let namespace_name = task.table_name.parent().clone();

        let table_ref = match self.table_cache.get(task.table_name.clone()).await {
            Ok(table_ref) => table_ref,
            Err(err) => {
                if err.is_not_found() {
                    warn!(
                        table = %task.table_name,
                        "received create table task for non-existent table"
                    );
                    return Ok(());
                }
                return Err(err).context(ClusterMetadataSnafu {
                    operation: "get_table",
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

        let complete = match data_lake.create_table(table_ref).await {
            Ok(table_id) => {
                let result = TaskResult::CreateTable(CreateTableResult { table_id });
                CompleteTaskRequest::new_completed(metadata.task_id.clone(), result)
            }
            Err(err) => CompleteTaskRequest::new_failed(metadata.task_id.clone(), err.to_string()),
        };

        self.table_metadata
            .complete_task(complete)
            .await
            .context(TableMetadataSnafu {
                operation: "complete_task",
            })?;

        info!(
            task_id = %metadata.task_id,
            table_name = %task.table_name,
            "Create table task completed"
        );

        Ok(())
    }
}
