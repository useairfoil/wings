use std::sync::Arc;

use futures::{Stream, StreamExt, stream::FuturesOrdered};
use snafu::ResultExt;
use tokio::sync::{mpsc, oneshot};
use wings_control_plane_core::log_metadata::{CommitBatchRequest, CommittedBatch, LogMetadata};
use wings_resources::NamespaceRef;

use crate::{
    error::{IngestorError, LogMetadataSnafu, Result},
    reply::WithReply,
    request::WriteBatchRequest,
    response::WriteBatchResponse,
};

#[derive(Clone)]
pub struct IngestorClient {
    pub(crate) tx: mpsc::Sender<WithReply<WriteBatchRequestWithNamespace>>,
    pub(crate) log_meta: Arc<dyn LogMetadata>,
}

#[derive(Debug)]
pub struct WriteBatchRequestWithNamespace {
    pub namespace: NamespaceRef,
    pub request: WriteBatchRequest,
}

impl IngestorClient {
    /// Ingests a stream of batches into the specified namespace.
    ///
    /// Data is atomically committed after the stream is fully consumed.
    pub async fn ingest(
        &self,
        namespace: NamespaceRef,
        batches: impl Stream<Item = WriteBatchRequest>,
    ) -> Result<Vec<CommittedBatch>> {
        tokio::pin!(batches);

        let mut replies = FuturesOrdered::new();

        while let Some(request) = batches.next().await {
            let (tx, rx) = oneshot::channel();
            // TODO: return validation error
            assert_eq!(&namespace.name, request.topic.name.parent());

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

            replies.push_back(rx);
        }

        let mut responses = Vec::new();
        while let Some(response) = replies.next().await {
            responses.push(response.map_err(|_| IngestorError::ChannelClosed)??);
        }

        self.commit_responses(namespace, responses).await
    }

    async fn commit_responses(
        &self,
        namespace: NamespaceRef,
        responses: Vec<WriteBatchResponse>,
    ) -> Result<Vec<CommittedBatch>> {
        let batches = responses
            .into_iter()
            .map(|response| CommitBatchRequest {
                topic_name: response.topic_name,
                partition_value: response.partition_value,
                file_ref: response.folio.file_ref,
                offset_bytes: response.folio.offset_bytes,
                batch_size_bytes: response.folio.size_bytes,
                timestamp: Some(response.timestamp),
                num_rows: response.num_rows,
            })
            .collect::<Vec<_>>();

        let committed = self
            .log_meta
            .commit(namespace.name.clone(), batches)
            .await
            .context(LogMetadataSnafu)?;

        Ok(committed)
    }
}
