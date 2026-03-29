use std::time::Duration;

use async_trait::async_trait;
use wings_control_plane_core::log_metadata::{
    CommitPageRequest, CommitPageResponse, CompleteTaskRequest, CompleteTaskResponse,
    GetLogLocationRequest, ListPartitionsRequest, ListPartitionsResponse, LogLocation, LogMetadata,
    RequestTaskRequest, RequestTaskResponse, Result,
};
use wings_resources::NamespaceName;

use crate::SqlControlPlane;

#[async_trait]
impl LogMetadata for SqlControlPlane {
    async fn commit_folio(
        &self,
        namespace: NamespaceName,
        file_ref: String,
        pages: &[CommitPageRequest],
    ) -> Result<Vec<CommitPageResponse>> {
        self.db
            .commit_folio(namespace, file_ref, pages)
            .await
            .map_err(Into::into)
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

    async fn request_task(&self, _request: RequestTaskRequest) -> Result<RequestTaskResponse> {
        tokio::time::sleep(Duration::from_secs(1)).await;
        Ok(RequestTaskResponse { task: None })
    }

    async fn complete_task(&self, _request: CompleteTaskRequest) -> Result<CompleteTaskResponse> {
        todo!()
    }
}
