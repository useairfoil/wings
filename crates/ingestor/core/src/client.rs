use std::sync::Arc;

use futures::{Stream, StreamExt, stream::FuturesOrdered};
use snafu::ResultExt;
use tokio::sync::{mpsc, oneshot};
use wings_control_plane_core::table_metadata::{
    CommitBatchRequest, CommittedBatch, TableMetadata, RejectedBatchInfo,
};
use wings_resources::NamespaceRef;

use crate::{
    error::{IngestorError, TableMetadataSnafu, Result},
    reply::WithReply,
    request::WriteBatchRequest,
    response::WriteBatchResponse,
};

#[derive(Clone)]
pub struct IngestorClient {
    pub(crate) tx: mpsc::Sender<WithReply<WriteBatchRequestWithNamespace>>,
    pub(crate) table_metadata: Arc<dyn TableMetadata>,
}

#[derive(Debug)]
pub struct WriteBatchRequestWithNamespace {
    pub namespace: NamespaceRef,
    pub request: WriteBatchRequest,
}

#[derive(Debug)]
pub enum IngestionRequest {
    WriteBatch(WriteBatchRequest),
    Abort,
}

impl From<WriteBatchRequest> for IngestionRequest {
    fn from(request: WriteBatchRequest) -> Self {
        Self::WriteBatch(request)
    }
}

impl IngestorClient {
    /// Ingests a stream of requests into the specified namespace.
    ///
    /// Data is atomically committed after the stream is fully consumed.
    pub async fn ingest(
        &self,
        namespace: NamespaceRef,
        requests: impl Stream<Item = IngestionRequest>,
    ) -> Result<Vec<CommittedBatch>> {
        tokio::pin!(requests);

        let mut replies = FuturesOrdered::new();

        while let Some(request) = requests.next().await {
            let IngestionRequest::WriteBatch(request) = request else {
                return Ok(Vec::default());
            };

            let batch_id = request.batch_id;
            let num_rows = request.records.num_rows() as u32;

            let (tx, rx) = oneshot::channel();
            // TODO: return validation error
            assert_eq!(&namespace.name, request.table.name.parent());

            let request_with_namespace = WithReply {
                reply: tx,
                data: WriteBatchRequestWithNamespace {
                    namespace: namespace.clone(),
                    request,
                },
            };

            self.tx
                .send(request_with_namespace)
                .await
                .map_err(|_| IngestorError::ChannelClosed)?;

            replies.push_back(async move {
                let response = rx
                    .await
                    .map_err(|_| IngestorError::ChannelClosed)
                    .and_then(|response| response);
                (batch_id, num_rows, response)
            });
        }

        let mut responses = Vec::new();
        let mut rejected = Vec::new();
        while let Some(response) = replies.next().await {
            let (batch_id, num_rows, response) = response;
            match response {
                Ok(response) => responses.push(response),
                Err(err) => rejected.push(CommittedBatch::Rejected(RejectedBatchInfo {
                    batch_id,
                    num_rows,
                    reason: err.to_string(),
                })),
            }
        }

        self.commit_responses(namespace, responses, rejected).await
    }

    async fn commit_responses(
        &self,
        namespace: NamespaceRef,
        responses: Vec<WriteBatchResponse>,
        out: Vec<CommittedBatch>,
    ) -> Result<Vec<CommittedBatch>> {
        if responses.is_empty() {
            return Ok(out);
        }

        let batches = responses
            .into_iter()
            .map(|response| CommitBatchRequest {
                batch_id: response.batch_id,
                table_name: response.table_name,
                partition_value: response.partition_value,
                file_ref: response.folio.file_ref,
                page_offset_bytes: response.folio.offset_bytes,
                page_size_bytes: response.folio.size_bytes,
                timestamp: response.timestamp,
                num_rows: response.num_rows,
            })
            .collect::<Vec<_>>();

        let mut committed = self
            .table_metadata
            .commit(namespace.name.clone(), batches)
            .await
            .context(TableMetadataSnafu)?;

        committed.extend(out);

        Ok(committed)
    }
}
