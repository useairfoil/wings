use std::{fmt::Debug, sync::Arc, time::SystemTime};

use datafusion::common::arrow::datatypes::SchemaRef;
use parquet::{
    arrow::ArrowWriter,
    file::{metadata::KeyValue, properties::WriterProperties},
};
use snafu::ResultExt;
use wings_resources::{PartitionValue, TopicName};

use crate::{
    error::{IngestorError, ParquetSnafu, Result, ValidationSnafu},
    metrics::IngestorMetrics,
    reply::{Reply, WithReply},
    request::WriteBatchRequest,
};

const DEFAULT_BUFFER_CAPACITY: usize = 8 * 1024 * 1024;

/// A folio's page with data for a single partition.
pub struct FolioPage {
    /// The topic name for the partition.
    pub topic_name: TopicName,
    /// The partition value for the partition.
    pub partition_value: Option<PartitionValue>,
    /// The serialized data.
    pub data: Vec<u8>,
    /// Reply channels for the batches that contributed to the page.
    pub replies: Vec<WithReply<WriteOutputMetadata>>,
}

/// The output metadata of a write operation.
#[derive(Debug, Clone)]
pub struct WriteOutputMetadata {
    pub batch_id: u32,
    pub num_rows: usize,
    pub offset_rows: usize,
    pub timestamp: Option<SystemTime>,
}

/// Combines multiple partition batches into a single folio's page.
pub struct PartitionPageWriter {
    writer: ArrowWriter<Vec<u8>>,
    num_rows: usize,
    schema: SchemaRef,
    replies: Vec<WithReply<WriteOutputMetadata>>,
    metrics_kv: Vec<wings_observability::KeyValue>,
}

impl PartitionPageWriter {
    /// Creates a new partition page writer with the given schema.
    pub fn new(
        topic_name: TopicName,
        partition_value: Option<PartitionValue>,
        schema: SchemaRef,
    ) -> Result<Self> {
        // Add topic name and partition key to the metadata to help with debugging and troubleshooting.
        let partition_value = partition_value.map(|v| v.to_string());
        let kv_metadata = vec![
            KeyValue::new("WINGS:topic-name".to_string(), topic_name.to_string()),
            KeyValue::new("WINGS:partition-value".to_string(), partition_value),
        ];

        let write_properties = WriterProperties::builder()
            .set_key_value_metadata(kv_metadata.into())
            .build();

        let metrics_kv = {
            use wings_observability::KeyValue as KV;

            let topic_id = topic_name.id().to_string();
            let namespace_id = topic_name.parent().id().to_string();
            let tenant_id = topic_name.parent().parent().id().to_string();
            vec![
                KV::new("tenant", tenant_id),
                KV::new("namespace", namespace_id),
                KV::new("topic", topic_id),
            ]
        };

        let buffer = Vec::with_capacity(DEFAULT_BUFFER_CAPACITY);
        // The writer will only fail if the schema is unsupported
        let writer = ArrowWriter::try_new(buffer, schema.clone(), write_properties.into())
            .context(ParquetSnafu {
                message: "failed to create parquet writer".to_string(),
            })?;

        Ok(Self {
            writer,
            num_rows: 0,
            schema,
            metrics_kv,
            replies: Vec::new(),
        })
    }

    /// Writes a batch to the partition batcher.
    /// Returns the number of bytes written to the parquet buffer.
    ///
    /// On error, forwards the error to the reply and returns 0.
    pub fn write_batch(
        &mut self,
        batch: WriteBatchRequest,
        reply: Reply,
        metrics: &IngestorMetrics,
    ) -> usize {
        // The writer will error if the schema does not match (which is good!), but it will also become poisoned.
        if self.schema != batch.records.schema() {
            let _ = reply.send(
                ValidationSnafu {
                    message: "batch schema does not match writer's schema".to_string(),
                }
                .fail(),
            );
            return 0;
        }

        let initial_size = self.estimate_bytes();
        let offset_rows = self.num_rows;

        if let Err(error) = self.writer.write(&batch.records).context(ParquetSnafu {
            message: "failed to write batch".to_string(),
        }) {
            let _ = reply.send(Err(error));
            return 0;
        }

        let num_rows = batch.records.num_rows();
        self.num_rows += num_rows;

        let output_meta = WriteOutputMetadata {
            batch_id: batch.batch_id,
            num_rows,
            offset_rows,
            timestamp: batch.timestamp,
        };

        self.replies.push(WithReply {
            reply,
            data: output_meta,
        });

        let bytes_written = self.estimate_bytes() - initial_size;
        metrics.written_rows.add(num_rows as _, &self.metrics_kv);

        bytes_written
    }

    /// Returns the estimated size of the buffer in bytes.
    pub fn estimate_bytes(&self) -> usize {
        self.writer.bytes_written() + self.writer.in_progress_size()
    }

    /// Closes the writer and returns the final parquet data with metadata.
    ///
    /// On error, forwards the error to all reply channels in the page and returns `None`.
    pub fn finish(
        self,
        topic_name: TopicName,
        partition_value: Option<PartitionValue>,
        metrics: &IngestorMetrics,
    ) -> Option<FolioPage> {
        match self.writer.into_inner() {
            Ok(data) => {
                let bytes_written = data.len();
                metrics
                    .written_bytes
                    .add(bytes_written as _, &self.metrics_kv);

                FolioPage {
                    topic_name,
                    partition_value,
                    data,
                    replies: self.replies,
                }
                .into()
            }
            Err(err) => {
                let error = IngestorError::Parquet {
                    message: "failed to finalize batch".to_string(),
                    source: Arc::new(err),
                };

                for notifier in self.replies.into_iter() {
                    let _ = notifier.reply.send(Err(error.clone()));
                }

                None
            }
        }
    }
}

impl Debug for FolioPage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let data_size = bytesize::ByteSize(self.data.len() as u64);
        f.debug_struct("PartitionBatch")
            .field("data", &format!("<{}>", data_size))
            .field("batches", &format!("<{} entries>", self.replies.len()))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::SystemTime};

    use datafusion::common::arrow::{
        array::Int32Array,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use tokio::sync::oneshot;
    use wings_resources::PartitionValue;

    use super::*;
    use crate::{
        error::IngestorError,
        metrics::IngestorMetrics,
        test_utils::{generate_test_batch, generate_write_request, schema, test_topic_name},
    };

    #[test]
    fn write_batch_tracks_offsets_and_finish_returns_page() {
        let metrics = IngestorMetrics::default();
        let topic_name = test_topic_name();
        let partition_value = Some(PartitionValue::Int64(42));
        let mut writer =
            PartitionPageWriter::new(topic_name.clone(), partition_value.clone(), schema())
                .expect("failed to create partition page writer");

        let first_timestamp = SystemTime::UNIX_EPOCH;
        let (first_reply, _first_rx) = oneshot::channel();
        let first_bytes = writer.write_batch(
            generate_write_request(generate_test_batch(2), Some(first_timestamp)),
            first_reply,
            &metrics,
        );

        let (second_reply, _second_rx) = oneshot::channel();
        let second_bytes = writer.write_batch(
            generate_write_request(generate_test_batch(3), None),
            second_reply,
            &metrics,
        );

        assert!(first_bytes > 0);
        assert!(second_bytes > 0);

        let page = writer
            .finish(topic_name.clone(), partition_value.clone(), &metrics)
            .expect("failed to finish partition page");

        assert_eq!(page.topic_name, topic_name);
        assert_eq!(page.partition_value, partition_value);
        assert!(!page.data.is_empty());
        assert_eq!(page.replies.len(), 2);

        assert_eq!(page.replies[0].data.batch_id, 0);
        assert_eq!(page.replies[0].data.num_rows, 2);
        assert_eq!(page.replies[0].data.offset_rows, 0);
        assert_eq!(page.replies[0].data.timestamp, Some(first_timestamp));
        assert_eq!(page.replies[1].data.batch_id, 0);
        assert_eq!(page.replies[1].data.num_rows, 3);
        assert_eq!(page.replies[1].data.offset_rows, 2);
        assert_eq!(page.replies[1].data.timestamp, None);
    }

    #[tokio::test]
    async fn write_batch_rejects_schema_mismatch_and_notifies_reply() {
        let metrics = IngestorMetrics::default();
        let mut writer = PartitionPageWriter::new(test_topic_name(), None, schema())
            .expect("failed to create partition page writer");
        let (reply, rx) = oneshot::channel();

        let bytes_written = writer.write_batch(
            generate_write_request(generate_mismatched_batch(), None),
            reply,
            &metrics,
        );

        assert_eq!(bytes_written, 0);

        let result = rx.await.expect("reply channel dropped");
        match result {
            Err(IngestorError::Validation { message }) => {
                assert_eq!(message, "batch schema does not match writer's schema");
            }
            other => panic!("expected validation error, got {other:?}"),
        }
    }

    fn generate_mismatched_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "other",
            DataType::Int32,
            false,
        )]));
        let values = Arc::new(Int32Array::from(vec![1, 2, 3]));

        RecordBatch::try_new(schema, vec![values]).expect("failed to create mismatched batch")
    }
}
