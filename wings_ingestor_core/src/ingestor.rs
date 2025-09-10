use std::sync::Arc;

use futures_util::{StreamExt, stream::FuturesUnordered};
use tokio::sync::mpsc;
use tokio_util::{sync::CancellationToken, time::DelayQueue};
use wings_control_plane::log_metadata::{
    CommitPageRequest, CommitPageResponse, CommittedBatch, LogMetadata,
};
use wings_object_store::ObjectStoreFactory;

use crate::{
    BatchIngestorClient, WriteBatchError,
    batcher::{NamespaceFolio, NamespaceFolioWriter},
    client::WriteBatchRequestWithReply,
    error::Result,
    uploader::FolioUploader,
    write::{ReplyWithWriteBatchError, WithReplyChannel},
};

const BATCH_CHANNEL_SIZE: usize = 100;

pub struct BatchIngestor {
    tx: mpsc::Sender<WriteBatchRequestWithReply>,
    rx: mpsc::Receiver<WriteBatchRequestWithReply>,
    uploader: FolioUploader,
    log_metadata: Arc<dyn LogMetadata>,
}

pub async fn run_background_ingestor(ingestor: BatchIngestor, ct: CancellationToken) -> Result<()> {
    ingestor.run(ct).await
}

struct CommittedFolio {
    pages: Vec<CommitPageResponse<WithReplyChannel<CommittedBatch>>>,
}

impl BatchIngestor {
    pub fn new(
        object_store_factory: Arc<dyn ObjectStoreFactory>,
        log_metadata: Arc<dyn LogMetadata>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(BATCH_CHANNEL_SIZE);
        let uploader = FolioUploader::new_ulid(object_store_factory);

        Self {
            tx,
            rx,
            uploader,
            log_metadata,
        }
    }

    /// Creates a new client for the batch ingestor.
    pub fn client(&self) -> BatchIngestorClient {
        BatchIngestorClient {
            tx: self.tx.clone(),
        }
    }

    pub async fn run(mut self, ct: CancellationToken) -> Result<()> {
        let _ct_guard = ct.child_token().drop_guard();
        let mut folio_timer = DelayQueue::new();
        let mut folio_writer = NamespaceFolioWriter::default();
        let folio_uploader = self.uploader;
        let committer = self.log_metadata;
        let mut upload_tasks = FuturesUnordered::new();

        loop {
            tokio::select! {
                _ = ct.cancelled() => {
                    break;
                }
                expired = folio_timer.next(), if !folio_timer.is_empty() => {
                    let Some(entry) = expired else {
                        continue;
                    };

                    let Some((folio, error)) = folio_writer.expire_namespace(entry.into_inner()) else {
                        continue;
                    };

                    error.send_to_all();

                    // Try to remove any duplicate timer keys.
                    folio_timer.try_remove(&folio.timer_key);

                    upload_tasks.push(upload_and_commit_folio(folio_uploader.clone(), committer.clone(), folio));
                }
                batch_with_reply = self.rx.recv() => {
                    let Some(request) = batch_with_reply else {
                        break;
                    };

                    match folio_writer.write_batch(request, &mut folio_timer) {
                        Ok(None) => {},
                        Ok(Some((folio, errors))) => {
                            folio_timer.remove(&folio.timer_key);

                            errors.send_to_all();

                            upload_tasks.push(upload_and_commit_folio(folio_uploader.clone(), committer.clone(), folio));
                        }
                        Err(error) => {
                            error.send_to_all();
                        }
                    }
                }
                task = upload_tasks.next(), if !upload_tasks.is_empty() => {
                    match task {
                        None => break,
                        Some(Ok(committed_folio)) => {
                            reply_with_committed_folio(committed_folio);
                        }
                        Some(Err(errors)) => {
                            errors.send_to_all();
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

async fn upload_and_commit_folio(
    uploader: FolioUploader,
    log_metadata: Arc<dyn LogMetadata>,
    folio: NamespaceFolio,
) -> Result<CommittedFolio, ReplyWithWriteBatchError> {
    let uploaded = uploader.upload_folio(folio).await?;

    let (pages_replies, pages_to_commit): (Vec<_>, Vec<_>) = uploaded
        .pages
        .into_iter()
        .map(|page| {
            let (replies, batches): (Vec<_>, Vec<_>) = page
                .batches
                .into_iter()
                .map(|batch| (batch.reply, batch.data))
                .unzip();

            let request = CommitPageRequest {
                topic_name: page.topic_name,
                partition_value: page.partition_value,
                num_messages: page.num_messages,
                offset_bytes: page.offset_bytes,
                batch_size_bytes: page.batch_size_bytes,
                batches,
            };
            (replies, request)
        })
        .unzip();

    let commits = match log_metadata
        .commit_folio(
            uploaded.namespace.name.clone(),
            uploaded.file_ref,
            &pages_to_commit,
        )
        .await
    {
        Ok(commits) => commits,
        Err(source) => {
            let error = WriteBatchError::LogMetadata {
                message: "failed to commit folio".to_string(),
                source,
            };
            let replies = pages_replies.into_iter().flatten().collect::<Vec<_>>();
            return ReplyWithWriteBatchError::new_fanout(error, replies).into();
        }
    };

    assert_eq!(
        commits.len(),
        pages_replies.len(),
        "folio pages and replies length mismatch"
    );

    let committed_pages = commits
        .into_iter()
        .zip(pages_replies.into_iter())
        .map(|(commit_response, replies)| {
            assert_eq!(
                commit_response.batches.len(),
                replies.len(),
                "page batches and replies length mismatch"
            );
            let batches = commit_response
                .batches
                .into_iter()
                .zip(replies.into_iter())
                .map(|(data, reply)| WithReplyChannel { reply, data })
                .collect::<Vec<_>>();

            CommitPageResponse {
                topic_name: commit_response.topic_name,
                partition_value: commit_response.partition_value,
                batches,
            }
        })
        .collect::<Vec<_>>();

    Ok(CommittedFolio {
        pages: committed_pages,
    })
}

fn reply_with_committed_folio(committed_folio: CommittedFolio) {
    for page in committed_folio.pages.into_iter() {
        for batch in page.batches.into_iter() {
            match batch.data {
                CommittedBatch::Accepted(accepted) => {
                    let _ = batch.reply.send(Ok(accepted));
                }
                CommittedBatch::Rejected(rejected) => {
                    let _ = batch
                        .reply
                        .send(Err(WriteBatchError::BatchRejected { info: rejected }));
                }
            }
        }
    }
}
