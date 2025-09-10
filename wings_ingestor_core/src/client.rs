use tokio::sync::{mpsc, oneshot};
use wings_control_plane::log_metadata::AcceptedBatchInfo;

use crate::write::{Result, WithReplyChannel, WriteBatchError, WriteBatchRequest};

pub type WriteBatchRequestWithReply = WithReplyChannel<WriteBatchRequest>;

#[derive(Clone)]
pub struct BatchIngestorClient {
    pub(crate) tx: mpsc::Sender<WriteBatchRequestWithReply>,
}

impl BatchIngestorClient {
    pub async fn write(&self, request: WriteBatchRequest) -> Result<AcceptedBatchInfo> {
        request.validate()?;

        let (tx, rx) = oneshot::channel();

        self.tx
            .send(WithReplyChannel {
                data: request,
                reply: tx,
            })
            .await
            .map_err(|_| WriteBatchError::Internal {
                message: "failed to send request".to_string(),
            })?;

        let response = rx.await.map_err(|_| WriteBatchError::Internal {
            message: "reply channel closed".to_string(),
        })?;

        response
    }
}
