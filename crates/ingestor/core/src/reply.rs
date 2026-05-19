use tokio::sync::oneshot;

use crate::{error::IngestorError, response::WriteBatchResponse};

pub type Reply = oneshot::Sender<Result<WriteBatchResponse, IngestorError>>;

#[derive(Debug)]
pub struct WithReply<T> {
    pub reply: Reply,
    pub data: T,
}
