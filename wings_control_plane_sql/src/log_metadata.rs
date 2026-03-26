use std::time::Duration;

use async_trait::async_trait;
use wings_control_plane_core::log_metadata::{
    CommitPageRequest, CommitPageResponse, CompleteTaskRequest, CompleteTaskResponse,
    GetLogLocationRequest, ListPartitionsRequest, ListPartitionsResponse, LogLocation, LogMetadata,
    LogMetadataError, RequestTaskRequest, RequestTaskResponse, Result,
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
        self.db
            .get_log_location(
                request.topic_name,
                request.partition_value,
                request.offset,
                request.options,
            )
            .await
            .map_err(Into::into)
    }

    async fn list_partitions(
        &self,
        request: ListPartitionsRequest,
    ) -> Result<ListPartitionsResponse> {
        let page_size = request.page_size.unwrap_or(100);
        let page = request
            .page_token
            .map(|t| {
                t.parse::<usize>()
                    .map_err(|_| LogMetadataError::InvalidArgument {
                        message: "invalid page token".to_string(),
                    })
            })
            .transpose()?
            .unwrap_or(0);

        self.db
            .list_partitions(request.topic_name, page_size, page)
            .await
            .map_err(Into::into)
    }

    async fn request_task(&self, _request: RequestTaskRequest) -> Result<RequestTaskResponse> {
        tokio::time::sleep(Duration::from_secs(1)).await;
        Ok(RequestTaskResponse { task: None })
    }

    async fn complete_task(&self, _request: CompleteTaskRequest) -> Result<CompleteTaskResponse> {
        todo!()
    }
}
