use std::sync::Arc;

use datafusion::common::arrow::datatypes::SchemaRef;
use parquet::{
    arrow::ArrowWriter,
    file::{metadata::KeyValue, properties::WriterProperties},
};
use wings_control_plane::{
    log_metadata::CommitBatchRequest,
    resources::{PartitionValue, TopicName},
};

use crate::{
    WriteBatchError, WriteBatchRequest,
    write::{ReplyWithWriteBatchError, WithReplyChannel},
};

use super::FolioPage;

const DEFAULT_BUFFER_CAPACITY: usize = 8 * 1024 * 1024;

/// Combines multiple partition batches into a single folio.
pub struct PartitionFolioWriter {
    writer: ArrowWriter<Vec<u8>>,
    schema: SchemaRef,
    batches: Vec<WithReplyChannel<CommitBatchRequest>>,
}

impl PartitionFolioWriter {
    /// Creates a new partition folio writer with the given schema.
    pub fn new(
        topic_name: TopicName,
        partition_value: Option<PartitionValue>,
        schema: SchemaRef,
    ) -> Result<Self, WriteBatchError> {
        // Add topic name and partition key to the metadata to help with debugging and troubleshooting.
        let partition_value = partition_value.map(|v| v.to_string());
        let kv_metadata = vec![
            KeyValue::new("WINGS:topic-name".to_string(), topic_name.to_string()),
            KeyValue::new("WINGS:partition-value".to_string(), partition_value),
        ];
        let write_properties = WriterProperties::builder()
            .set_key_value_metadata(kv_metadata.into())
            .build();

        let buffer = Vec::with_capacity(DEFAULT_BUFFER_CAPACITY);
        // The writer will only fail if the schema is unsupported
        let writer = ArrowWriter::try_new(buffer, schema.clone(), write_properties.into())
            .map_err(|err| WriteBatchError::Parquet {
                message: "failed to create parquet writer".to_string(),
                source: Arc::new(err),
            })?;

        Ok(Self {
            writer,
            schema,
            batches: Vec::new(),
        })
    }

    /// Writes a batch to the partition batcher.
    /// Returns the number of bytes written to the parquet buffer.
    ///
    /// On error, returns the reply channel and the error.
    pub fn write_batch(
        &mut self,
        request: WithReplyChannel<WriteBatchRequest>,
    ) -> std::result::Result<usize, ReplyWithWriteBatchError> {
        let batch = request.data;
        // The writer will error if the schema does not match (which is good!), but it will also become poisoned.
        if self.schema != batch.records.schema() {
            let error = WriteBatchError::Validation {
                message: "batch schema does not match writer's schema".to_string(),
            };
            return ReplyWithWriteBatchError::new_single(error, request.reply).into();
        }

        let initial_size = self.buffer_size();

        if let Err(err) = self.writer.write(&batch.records) {
            let error = WriteBatchError::Parquet {
                message: "failed to write batch".to_string(),
                source: Arc::new(err),
            };
            return ReplyWithWriteBatchError::new_single(error, request.reply).into();
        };

        self.batches.push(WithReplyChannel {
            reply: request.reply,
            data: CommitBatchRequest {
                num_messages: batch.records.num_rows() as _,
                timestamp: batch.timestamp,
            },
        });

        let bytes_written = self.buffer_size() - initial_size;

        Ok(bytes_written)
    }

    /// Returns the current size of the buffer in bytes.
    pub fn buffer_size(&self) -> usize {
        self.writer.inner().len()
    }

    /// Closes the writer and returns the final parquet data with metadata.
    ///
    /// On error, returns all the reply channels in the batch, together with the error.
    pub fn finish(
        self,
        topic_name: TopicName,
        partition_value: Option<PartitionValue>,
    ) -> Result<FolioPage, ReplyWithWriteBatchError> {
        match self.writer.into_inner() {
            Ok(data) => Ok(FolioPage {
                topic_name,
                partition_value,
                data,
                batches: self.batches,
            }),
            Err(err) => {
                let error = WriteBatchError::Parquet {
                    message: "failed to finalize batch".to_string(),
                    source: Arc::new(err),
                };

                let replies = self.batches.into_iter().map(|batch| batch.reply).collect();
                ReplyWithWriteBatchError::new_fanout(error, replies).into()
            }
        }
    }
}
