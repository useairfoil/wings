use std::sync::Arc;

use futures::{StreamExt, stream::FuturesUnordered};
use tokio::sync::mpsc;
use tokio_util::{sync::CancellationToken, time::DelayQueue};
use wings_control_plane_core::table_metadata::TableMetadata;
use wings_object_store::ObjectStoreFactory;

use crate::{
    client::{IngestorClient, WriteBatchRequestWithNamespace},
    error::Result,
    namespace_writer::{NamespaceFolio, NamespaceFolioWriter},
    reply::WithReply,
    uploader::FolioUploader,
};

const INGESTOR_CHANNEL_SIZE: usize = 128;

pub struct Ingestor {
    rx: mpsc::Receiver<WithReply<WriteBatchRequestWithNamespace>>,
    client: IngestorClient,
    uploader: FolioUploader,
}

impl Ingestor {
    pub fn new(
        object_store_factory: Arc<dyn ObjectStoreFactory>,
        table_metadata: Arc<dyn TableMetadata>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(INGESTOR_CHANNEL_SIZE);
        let client = IngestorClient { table_metadata, tx };
        let uploader = FolioUploader::new_ulid(object_store_factory);
        Self {
            client,
            uploader,
            rx,
        }
    }

    pub fn client(&self) -> IngestorClient {
        self.client.clone()
    }

    pub async fn run(mut self, ct: CancellationToken) -> Result<()> {
        let _ct_guard = ct.child_token().drop_guard();
        let uploader = self.uploader;
        let mut folio_timer = DelayQueue::new();
        let mut folio_writer = NamespaceFolioWriter::default();
        let mut upload_tasks = FuturesUnordered::new();

        loop {
            tokio::select! {
                _ = ct.cancelled() => {
                    // Exit gracefully.
                    break;
                }
                expired = folio_timer.next(), if !folio_timer.is_empty() => {
                    let Some(entry) = expired else {
                        continue;
                    };
                    // Remove the expired folio from the writer.
                    let Some(folio) = folio_writer.expire_namespace(entry.into_inner()) else {
                        continue;
                    };

                    // Try to remove any duplicate timer keys.
                    folio_timer.try_remove(&folio.timer_key);
                    // Start the upload task.
                    upload_tasks.push(upload_folio(uploader.clone(), folio));
                }
                batch_with_reply = self.rx.recv() => {
                    let Some(request) = batch_with_reply else {
                        break;
                    };

                    let reply = request.reply;
                    let namespace = request.data.namespace;
                    let request = request.data.request;

                    match folio_writer.write_batch(namespace, request, reply, &mut folio_timer) {
                        None => {
                            // Batch written to folio, more can be written before it is uploaded.
                        },
                        Some(folio) => {
                            // The folio is big enough to be uploaded immediately.
                            folio_timer.remove(&folio.timer_key);

                            // Start the upload task.
                            upload_tasks.push(upload_folio(uploader.clone(), folio));
                        }
                    }
                }
                task = upload_tasks.next(), if !upload_tasks.is_empty() => {
                    match task {
                        None => {
                            // This should never happen.
                            break
                        },
                        Some(()) => {
                            // Upload task completed.
                            // Notice that the task has no return value because
                            // we expect it to also submit the success or error
                            // replies.
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

async fn upload_folio(uploader: FolioUploader, folio: NamespaceFolio) {
    uploader.upload_folio(folio).await;
}
