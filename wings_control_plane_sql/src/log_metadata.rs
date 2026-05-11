use std::time::{Duration, Instant};

use async_trait::async_trait;
use wings_control_plane_core::log_metadata::{
    CommitBatchRequest, CommittedBatch, CompleteTaskRequest, CompleteTaskResponse,
    GetLogLocationRequest, ListPartitionsRequest, ListPartitionsResponse, LogLocation, LogMetadata,
    RequestTaskRequest, RequestTaskResponse, Result,
};
use wings_resources::NamespaceName;

use crate::SqlControlPlane;

#[async_trait]
impl LogMetadata for SqlControlPlane {
    async fn commit(
        &self,
        namespace: NamespaceName,
        batches: Vec<CommitBatchRequest>,
    ) -> Result<Vec<CommittedBatch>> {
        self.db.commit(namespace, batches).await.map_err(Into::into)
    }

    async fn get_log_location(&self, request: GetLogLocationRequest) -> Result<Vec<LogLocation>> {
        self.db.get_log_location(request).await.map_err(Into::into)
    }

    async fn list_partitions(
        &self,
        request: ListPartitionsRequest,
    ) -> Result<ListPartitionsResponse> {
        self.db.list_partitions(request).await.map_err(Into::into)
    }

    async fn request_task(&self, request: RequestTaskRequest) -> Result<RequestTaskResponse> {
        // To avoid the client hammering the server, we poll for a task for a few seconds.
        // Only then we return an empty response to the client.
        let deadline = Instant::now() + Duration::from_secs(5);

        while Instant::now() < deadline {
            let response = self.db.request_task(&request).await?;
            if response.task.is_some() {
                return Ok(response);
            }

            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        Ok(RequestTaskResponse { task: None })
    }

    async fn complete_task(&self, _request: CompleteTaskRequest) -> Result<CompleteTaskResponse> {
        todo!()
    }
}
