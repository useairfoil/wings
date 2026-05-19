use std::{
    collections::{HashMap, hash_map::Entry},
    fmt::Debug,
};

use tokio_util::time::{DelayQueue, delay_queue};
use wings_resources::{NamespaceName, NamespaceRef, PartitionValue, TableName};

use crate::{
    metrics::IngestorMetrics,
    partition_writer::{FolioPage, PartitionPageWriter},
    reply::Reply,
    request::WriteBatchRequest,
};

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

/// Writes folio data grouped by namespace.
#[derive(Default)]
pub struct NamespaceFolioWriter {
    metrics: IngestorMetrics,
    namespaces: HashMap<NamespaceName, NamespaceFolioState>,
}

struct NamespaceFolioState {
    /// The namespace
    pub namespace: NamespaceRef,
    /// Size threshold after which to flush the folio
    pub flush_size: u64,
    /// Map from (table, partition) to the partition folio writer
    pub partitions: HashMap<(TableName, Option<PartitionValue>), PartitionPageWriter>,
    /// The current size of the folio
    pub current_size: u64,
    /// The timer key for the flush timer
    pub timer_key: delay_queue::Key,
}

impl NamespaceFolioWriter {
    /// Writes a batch to the namespace folio, returning the flushed folio if it's full.
    pub fn write_batch(
        &mut self,
        namespace: NamespaceRef,
        request: WriteBatchRequest,
        reply: Reply,
        delay_queue: &mut DelayQueue<NamespaceName>,
    ) -> Option<NamespaceFolio> {
        let namespace_name = namespace.name.clone();

        let folio_state = self
            .namespaces
            .entry(namespace_name.clone())
            .or_insert_with(|| {
                let flush_interval = namespace.flush_interval;
                let timer_key = delay_queue.insert(namespace_name.clone(), flush_interval);
                NamespaceFolioState::new(namespace.clone(), timer_key)
            });

        let folio_writer = match folio_state
            .partitions
            .entry((request.table.name.clone(), request.partition.clone()))
        {
            Entry::Occupied(inner) => inner.into_mut(),
            Entry::Vacant(inner) => {
                match PartitionPageWriter::new(
                    request.table.name.clone(),
                    request.partition.clone(),
                    request.table.arrow_schema_without_partition_field(),
                ) {
                    Ok(writer) => inner.insert(writer),
                    Err(error) => {
                        let _ = reply.send(Err(error));
                        return None;
                    }
                }
            }
        };

        let written = folio_writer.write_batch(request, reply, &self.metrics);
        folio_state.current_size += written as u64;

        if folio_state.current_size >= folio_state.flush_size {
            let state = self.namespaces.remove(&namespace_name)?;

            return state.finish(&self.metrics);
        }

        None
    }

    /// Expires a namespace, flushing any pending data.
    pub fn expire_namespace(&mut self, namespace: NamespaceName) -> Option<NamespaceFolio> {
        let state = self.namespaces.remove(&namespace)?;

        state.finish(&self.metrics)
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
    pub fn finish(self, metrics: &IngestorMetrics) -> Option<NamespaceFolio> {
        let mut pages = Vec::with_capacity(self.partitions.len());

        for (key, partition_writer) in self.partitions.into_iter() {
            let (table_name, partition) = key;
            if let Some(folio_page) = partition_writer.finish(table_name, partition, metrics) {
                pages.push(folio_page)
            };
        }

        if pages.is_empty() {
            return None;
        }

        NamespaceFolio {
            namespace: self.namespace,
            timer_key: self.timer_key,
            pages,
        }
        .into()
    }
}

#[cfg(test)]
mod tests {
    use bytesize::ByteSize;
    use tokio::sync::oneshot;

    use super::*;
    use crate::test_utils::{
        generate_test_batch_with_schema, generate_write_request_for,
        test_namespace_with_flush_size, test_table,
    };

    #[tokio::test]
    async fn write_batch_inserts_timer_on_first_namespace_write() {
        let namespace = test_namespace_with_flush_size(ByteSize::mb(8));
        let table = test_table();
        let mut writer = NamespaceFolioWriter::default();
        let mut delay_queue = DelayQueue::new();

        let (reply, _rx) = oneshot::channel();
        let request = write_request(table.clone(), 0, 1);

        let folio = writer.write_batch(namespace.clone(), request, reply, &mut delay_queue);

        assert!(folio.is_none());
        assert_eq!(delay_queue.len(), 1);
        assert!(writer.namespaces.contains_key(&namespace.name));

        let state = writer
            .namespaces
            .get(&namespace.name)
            .expect("namespace state not inserted");
        assert_eq!(state.partitions.len(), 1);
        assert!(state.current_size > 0);

        let (reply, _rx) = oneshot::channel();
        let request = write_request(table, 0, 1);

        let folio = writer.write_batch(namespace.clone(), request, reply, &mut delay_queue);

        assert!(folio.is_none());
        assert_eq!(delay_queue.len(), 1);
    }

    #[tokio::test]
    async fn write_batch_flushes_when_size_limit_is_hit() {
        let namespace = test_namespace_with_flush_size(ByteSize::b(32));
        let table = test_table();
        let table_name = table.name.clone();
        let mut writer = NamespaceFolioWriter::default();
        let mut delay_queue = DelayQueue::new();
        let (reply0, _rx) = oneshot::channel();

        let folio = writer.write_batch(
            namespace.clone(),
            write_request(table.clone(), 0, 3),
            reply0,
            &mut delay_queue,
        );
        assert!(folio.is_none());

        let (reply1, _rx) = oneshot::channel();

        let folio = writer
            .write_batch(
                namespace.clone(),
                write_request(table, 3, 3),
                reply1,
                &mut delay_queue,
            )
            .expect("expected size-triggered folio flush");

        assert_eq!(folio.namespace.name, namespace.name);
        assert_eq!(folio.pages.len(), 1);
        assert!(!writer.namespaces.contains_key(&namespace.name));

        let page = &folio.pages[0];
        assert_eq!(page.table_name, table_name);
        assert_eq!(page.partition_value, None);
        assert!(!page.data.is_empty());
        assert_eq!(page.replies.len(), 2);
        assert_eq!(page.replies[0].data.num_rows, 3);
        assert_eq!(page.replies[0].data.offset_rows, 0);
        assert_eq!(page.replies[1].data.num_rows, 3);
        assert_eq!(page.replies[1].data.offset_rows, 3);
    }

    fn write_request(
        table: wings_resources::TableRef,
        start_index: i32,
        num_rows: usize,
    ) -> WriteBatchRequest {
        let records = generate_test_batch_with_schema(
            start_index,
            num_rows,
            table.arrow_schema_without_partition_field(),
        );

        generate_write_request_for(table, None, records, None)
    }
}
