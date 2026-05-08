use std::sync::Arc;

use futures::{Stream, StreamExt, stream::FuturesOrdered};
use tokio::sync::{mpsc, oneshot};
use wings_control_plane_core::log_metadata::{
    CommitBatchRequest, CommitPageRequest, CommittedBatch, LogMetadata,
};
use wings_resources::NamespaceRef;

use crate::{
    error::{IngestorError, Result},
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
        let mut files = Vec::<CommitFile>::new();

        for (response_index, response) in responses.into_iter().enumerate() {
            let file_index = files
                .iter()
                .position(|file| file.file_ref == response.folio.file_ref)
                .unwrap_or_else(|| {
                    files.push(CommitFile {
                        file_ref: response.folio.file_ref.clone(),
                        pages: Vec::new(),
                    });
                    files.len() - 1
                });

            let pages = &mut files[file_index].pages;
            let page_index = pages
                .iter()
                .position(|page| page.matches(&response))
                .unwrap_or_else(|| {
                    pages.push(CommitPage::new(&response));
                    pages.len() - 1
                });

            pages[page_index].push_batch(response_index, response)?;
        }

        let mut committed_batches = vec![None; files.iter().map(|f| f.num_batches()).sum()];

        for file in files {
            let pages = file
                .pages
                .iter()
                .map(CommitPage::to_request)
                .collect::<Vec<_>>();

            let page_responses = self
                .log_meta
                .commit_folio(namespace.name.clone(), file.file_ref, &pages)
                .await
                .map_err(|source| IngestorError::LogMetadata {
                    source: Arc::new(source),
                })?;

            for (page, response) in file.pages.into_iter().zip(page_responses) {
                for (response_index, batch) in
                    page.response_indexes.into_iter().zip(response.batches)
                {
                    committed_batches[response_index] = Some(batch);
                }
            }
        }

        committed_batches
            .into_iter()
            .map(|batch| {
                batch.ok_or_else(|| IngestorError::Validation {
                    message: "log metadata did not return all committed batches".to_string(),
                })
            })
            .collect()
    }
}

struct CommitFile {
    file_ref: String,
    pages: Vec<CommitPage>,
}

impl CommitFile {
    fn num_batches(&self) -> usize {
        self.pages
            .iter()
            .map(|page| page.batches.len())
            .sum::<usize>()
    }
}

struct CommitPage {
    topic_name: wings_resources::TopicName,
    partition_value: Option<wings_resources::PartitionValue>,
    offset_bytes: u64,
    size_bytes: u64,
    num_rows: u32,
    batches: Vec<CommitBatchRequest>,
    response_indexes: Vec<usize>,
}

impl CommitPage {
    fn new(response: &WriteBatchResponse) -> Self {
        Self {
            topic_name: response.topic_name.clone(),
            partition_value: response.partition_value.clone(),
            offset_bytes: response.folio.offset_bytes,
            size_bytes: response.folio.size_bytes,
            num_rows: 0,
            batches: Vec::new(),
            response_indexes: Vec::new(),
        }
    }

    fn matches(&self, response: &WriteBatchResponse) -> bool {
        self.topic_name == response.topic_name
            && self.partition_value == response.partition_value
            && self.offset_bytes == response.folio.offset_bytes
            && self.size_bytes == response.folio.size_bytes
    }

    fn push_batch(&mut self, response_index: usize, response: WriteBatchResponse) -> Result<()> {
        let num_rows = u32::try_from(response.num_rows).map_err(|_| IngestorError::Validation {
            message: "batch row count exceeds u32::MAX".to_string(),
        })?;

        self.num_rows =
            self.num_rows
                .checked_add(num_rows)
                .ok_or_else(|| IngestorError::Validation {
                    message: "page row count exceeds u32::MAX".to_string(),
                })?;
        self.batches.push(CommitBatchRequest {
            timestamp: Some(response.timestamp),
            num_rows,
        });
        self.response_indexes.push(response_index);

        Ok(())
    }

    fn to_request(&self) -> CommitPageRequest {
        CommitPageRequest {
            topic_name: self.topic_name.clone(),
            partition_value: self.partition_value.clone(),
            batches: self.batches.clone(),
            num_rows: self.num_rows,
            offset_bytes: self.offset_bytes,
            batch_size_bytes: self.size_bytes,
        }
    }
}
