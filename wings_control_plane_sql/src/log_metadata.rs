use std::time::Duration;

use async_trait::async_trait;
use wings_control_plane_core::log_metadata::{
    CommitPageRequest, CommitPageResponse, CompleteTaskRequest, CompleteTaskResponse,
    GetLogLocationRequest, ListPartitionsRequest, ListPartitionsResponse, LogLocation, LogMetadata,
    RequestTaskRequest, RequestTaskResponse, Result,
};

use crate::SqlControlPlane;

#[async_trait]
impl LogMetadata for SqlControlPlane {
    async fn commit_folio(
        &self,
        _namespace: wings_resources::NamespaceName,
        _file_ref: String,
        _pages: &[CommitPageRequest],
    ) -> Result<Vec<CommitPageResponse>> {
        todo!()
    }

    async fn get_log_location(&self, _request: GetLogLocationRequest) -> Result<Vec<LogLocation>> {
        todo!()
    }

    async fn list_partitions(
        &self,
        _request: ListPartitionsRequest,
    ) -> Result<ListPartitionsResponse> {
        todo!()
    }

    async fn request_task(&self, _request: RequestTaskRequest) -> Result<RequestTaskResponse> {
        tokio::time::sleep(Duration::from_secs(1)).await;
        Ok(RequestTaskResponse { task: None })
    }

    async fn complete_task(&self, _request: CompleteTaskRequest) -> Result<CompleteTaskResponse> {
        todo!()
    }
}
