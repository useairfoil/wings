use std::{sync::Arc, time::SystemTime};

use datafusion::common::arrow::datatypes::SchemaRef;
use datafusion::common::arrow::record_batch::RecordBatch;
use parquet::{
    arrow::ArrowWriter,
    file::{metadata::KeyValue, properties::WriterProperties},
};
use snafu::ResultExt;
use wings_metadata_core::{admin::TopicName, partition::PartitionValue};

use crate::{
    batch::WriteReplySender,
    error::{IngestorError, ParquetSnafu, Result},
    types::{BatchContext, PartitionFolio, ReplyWithError},
};

const DEFAULT_BUFFER_CAPACITY: usize = 8 * 1024 * 1024;

/// Combines multiple partition batches into a single folio.
pub struct PartitionFolioWriter {
    writer: ArrowWriter<Vec<u8>>,
    schema: SchemaRef,
    batches: Vec<BatchContext>,
}

impl PartitionFolioWriter {
    /// Creates a new partition folio writer with the given schema.
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

        let buffer = Vec::with_capacity(DEFAULT_BUFFER_CAPACITY);
        // The writer will only fail if the schema is unsupported
        let writer = ArrowWriter::try_new(buffer, schema.clone(), write_properties.into())
            .context(ParquetSnafu {
                message: "failed to initialize writer",
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
        batch: &RecordBatch,
        timestamp: Option<SystemTime>,
        reply: WriteReplySender,
    ) -> std::result::Result<usize, ReplyWithError> {
        // The writer will error if the schema does not match (which is good!), but it will also become poisoned.
        if self.schema != batch.schema() {
            let error = IngestorError::Schema {
                message: "batch schema does not match writer's schema".to_string(),
            };
            return Err(ReplyWithError { reply, error });
        }

        let initial_size = self.buffer_size();

        if let Err(err) = self.writer.write(batch) {
            let error = IngestorError::Parquet {
                message: "failed to write batch",
                source: Arc::new(err),
            };
            return Err(ReplyWithError { reply, error });
        };

        self.batches.push(BatchContext {
            reply,
            num_messages: batch.num_rows() as _,
            timestamp,
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
    ) -> std::result::Result<PartitionFolio, Vec<ReplyWithError>> {
        match self.writer.into_inner() {
            Ok(data) => Ok(PartitionFolio {
                topic_name,
                partition_value,
                data,
                batches: self.batches,
            }),
            Err(err) => {
                let error = IngestorError::Parquet {
                    message: "failed to finalize batch",
                    source: Arc::new(err),
                };

                let replies = self
                    .batches
                    .into_iter()
                    .map(|m| ReplyWithError {
                        reply: m.reply,
                        error: error.clone(),
                    })
                    .collect();
                Err(replies)
            }
        }
    }
}
