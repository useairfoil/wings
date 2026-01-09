use std::{
    collections::{HashMap, hash_map::Entry},
    fmt::Debug,
};

use tokio_util::time::{DelayQueue, delay_queue};
use wings_control_plane::{
    log_metadata::CommitBatchRequest,
    resources::{NamespaceName, NamespaceRef, PartitionValue, TopicName},
};

use crate::{
    WriteBatchRequest,
    write::{ReplyWithWriteBatchError, WithReplyChannel},
};

use self::{metrics::IngestionMetrics, partition::PartitionFolioWriter};

mod metrics;
mod partition;

#[derive(Default)]
pub struct NamespaceFolioWriter {
    metrics: IngestionMetrics,
    namespaces: HashMap<NamespaceName, NamespaceFolioState>,
}

/// A folio's page with data for a single partition.
pub struct FolioPage {
    /// The topic name for the partition.
    pub topic_name: TopicName,
    /// The partition value for the partition.
    pub partition_value: Option<PartitionValue>,
    /// The serialized data.
    pub data: Vec<u8>,
    /// The batches that contributed to the folio.
    pub batches: Vec<WithReplyChannel<CommitBatchRequest>>,
}

/// A folio of data for a namespace.
#[derive(Debug)]
pub struct NamespaceFolio {
    /// The namespace.
    pub namespace: NamespaceRef,
    /// The timer key used to periodically flush the folio.
    pub timer_key: delay_queue::Key,
    /// The partitions' data.
    pub pages: Vec<FolioPage>,
}

struct NamespaceFolioState {
    /// The namespace
    pub namespace: NamespaceRef,
    /// Size threshold after which to flush the folio
    pub flush_size: u64,
    /// Map from (topic, partition) to the partition folio writer
    pub partitions: HashMap<(TopicName, Option<PartitionValue>), PartitionFolioWriter>,
    /// The current size of the folio
    pub current_size: u64,
    /// The timer key for the flush timer
    pub timer_key: delay_queue::Key,
}

impl NamespaceFolioWriter {
    /// Writes a batch to the namespace folio, returning the flushed folio if it's full.
    pub fn write_batch(
        &mut self,
        request: WithReplyChannel<WriteBatchRequest>,
        delay_queue: &mut DelayQueue<NamespaceName>,
    ) -> Result<Option<(NamespaceFolio, ReplyWithWriteBatchError)>, ReplyWithWriteBatchError> {
        let batch = &request.data;
        let namespace = batch.namespace.name.clone();

        let folio_state = self
            .namespaces
            .entry(batch.namespace.name.clone())
            .or_insert_with(|| {
                let flush_interval = batch.namespace.flush_interval;
                let timer_key = delay_queue.insert(namespace.clone(), flush_interval);
                NamespaceFolioState::new(batch.namespace.clone(), timer_key)
            });

        let folio_writer = match folio_state
            .partitions
            .entry((batch.topic.name.clone(), batch.partition.clone()))
        {
            Entry::Occupied(inner) => inner.into_mut(),
            Entry::Vacant(inner) => {
                match PartitionFolioWriter::new(
                    batch.topic.name.clone(),
                    batch.partition.clone(),
                    batch.topic.arrow_schema_without_partition_field(),
                ) {
                    Ok(writer) => inner.insert(writer),
                    Err(error) => {
                        return ReplyWithWriteBatchError::new_single(error, request.reply).into();
                    }
                }
            }
        };

        let written = folio_writer.write_batch(request, &self.metrics)?;
        folio_state.current_size += written as u64;

        if folio_state.current_size >= folio_state.flush_size {
            let Some(state) = self.namespaces.remove(&namespace) else {
                return Ok(None);
            };

            return Ok(state.finish(&self.metrics).into());
        }

        Ok(None)
    }

    /// Expires a namespace, flushing any pending data.
    pub fn expire_namespace(
        &mut self,
        namespace: NamespaceName,
    ) -> Option<(NamespaceFolio, ReplyWithWriteBatchError)> {
        let state = self.namespaces.remove(&namespace)?;

        state.finish(&self.metrics).into()
    }
}

impl NamespaceFolioState {
    /// Creates a new namespace folio writer with the given flush parameters.
    pub fn new(namespace: NamespaceRef, timer_key: delay_queue::Key) -> Self {
        let flush_size = namespace.flush_size.as_u64();
        Self {
            namespace,
            flush_size,
            partitions: HashMap::new(),
            current_size: 0,
            timer_key,
        }
    }

    /// Flushes the namespace folio, ready for the next step.
    pub fn finish(self, metrics: &IngestionMetrics) -> (NamespaceFolio, ReplyWithWriteBatchError) {
        let mut pages = Vec::with_capacity(self.partitions.len());
        let mut errors = ReplyWithWriteBatchError::new_empty();

        for (key, partition_writer) in self.partitions.into_iter() {
            let (topic_name, partition) = key;
            match partition_writer.finish(topic_name, partition, metrics) {
                Ok(folio) => pages.push(folio),
                Err(err) => errors.merge(err),
            }
        }

        let folio = NamespaceFolio {
            namespace: self.namespace,
            timer_key: self.timer_key,
            pages,
        };

        (folio, errors)
    }
}

impl Debug for FolioPage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let data_size = bytesize::ByteSize(self.data.len() as u64);
        f.debug_struct("PartitionBatch")
            .field("data", &format!("<{}>", data_size))
            .field("batches", &format!("<{} entries>", self.batches.len()))
            .finish()
    }
}
