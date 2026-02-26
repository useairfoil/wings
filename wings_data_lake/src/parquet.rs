use std::{
    sync::{Arc, Mutex},
    time::SystemTime,
};

use bytesize::ByteSize;
use datafusion::arrow::record_batch::RecordBatch;
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload, path::Path};
use parquet::file::{metadata::KeyValue, properties::WriterProperties};
use snafu::ResultExt;
use tracing::debug;
use ulid::Ulid;
use wings_control_plane_core::log_metadata::{FileInfo, FileMetadata};
use wings_object_store::paths::{format_parquet_data_path, format_partitioned_parquet_data_path};
use wings_resources::{PartitionPosition, PartitionValue, TopicName, TopicRef};
use wings_schema::Field;

use super::error::Result;
use crate::{
    BatchWriter, DataLake,
    error::{InternalSnafu, InvalidSchemaSnafu},
    parquet_writer::ParquetWriter,
};

pub struct ParquetDataLake {
    object_store: Arc<dyn ObjectStore>,
}

pub struct ParquetBatchWriter {
    inner: Mutex<ParquetWriter>,
    object_store: Arc<dyn ObjectStore>,
    partition_value: Option<PartitionValue>,
    partition_field: Option<Field>,
    written: Vec<FileInfo>,
    target_file_size_bytes: u64,
    topic_name: TopicName,
    end_offset: u64,
    current_file_start_offset: u64,
}

impl ParquetDataLake {
    pub fn new(object_store: Arc<dyn ObjectStore>) -> Self {
        Self { object_store }
    }
}

#[async_trait::async_trait]
impl DataLake for ParquetDataLake {
    async fn create_table(&self, topic: TopicRef) -> Result<String> {
        Ok(topic.name.to_string())
    }

    async fn batch_writer(
        &self,
        topic: TopicRef,
        partition_value: Option<PartitionValue>,
        start_offset: u64,
        end_offset: u64,
        target_file_size: ByteSize,
    ) -> Result<Box<dyn BatchWriter>> {
        ParquetBatchWriter::new_boxed(
            self.object_store.clone(),
            topic,
            partition_value,
            start_offset,
            end_offset,
            target_file_size,
        )
    }

    async fn commit_data(&self, _topic: TopicRef, _new_files: &[FileInfo]) -> Result<String> {
        Ok("0".to_string())
    }
}

impl ParquetBatchWriter {
    pub fn new_boxed(
        object_store: Arc<dyn ObjectStore>,
        topic: TopicRef,
        partition_value: Option<PartitionValue>,
        start_offset: u64,
        end_offset: u64,
        target_file_size: ByteSize,
    ) -> Result<Box<dyn BatchWriter>> {
        let writer_properties = {
            let partition_value = partition_value.as_ref().map(|v| v.to_string());
            let kv_metadata = vec![
                KeyValue::new("WINGS:topic-name".to_string(), topic.name.to_string()),
                KeyValue::new("WINGS:partition-value".to_string(), partition_value),
                KeyValue::new("WINGS:start-offset".to_string(), start_offset.to_string()),
                KeyValue::new("WINGS:end-offset".to_string(), end_offset.to_string()),
            ];

            WriterProperties::builder()
                .set_key_value_metadata(kv_metadata.into())
                .set_created_by("wings dev build".to_string())
                .build()
        };

        let output_schema = topic
            .schema_with_metadata(PartitionPosition::Skip)
            .context(InvalidSchemaSnafu {})?;
        let inner = ParquetWriter::new(output_schema.into(), writer_properties);

        let writer = ParquetBatchWriter {
            inner: Mutex::new(inner),
            partition_value,
            partition_field: topic.partition_field().cloned(),
            object_store,
            written: Default::default(),
            target_file_size_bytes: target_file_size.as_u64(),
            topic_name: topic.name.clone(),
            current_file_start_offset: start_offset,
            end_offset,
        };

        Ok(Box::new(writer))
    }

    async fn upload_file(&mut self, data: Vec<u8>, metadata: FileMetadata) -> Result<()> {
        let payload = PutPayload::from_bytes(data.into());

        let file_id = Ulid::new().to_string();
        let file_ref = if let Some(ref field) = self.partition_field {
            format_partitioned_parquet_data_path(
                &self.topic_name,
                field.name(),
                &self.partition_value,
                &file_id,
            )
        } else {
            format_parquet_data_path(&self.topic_name, &file_id)
        };

        debug!(
            %file_ref,
            file_start_offset = self.current_file_start_offset,
            "Uploading parquet file to storage"
        );

        let path = Path::parse(&file_ref).unwrap();

        self.object_store
            .put_opts(
                &path,
                payload,
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await?;

        let num_rows = metadata.num_rows as u64;
        assert!(num_rows > 0, "Parquet file with zero rows was uploaded");

        let end_offset = self.current_file_start_offset + num_rows - 1;

        self.written.push(FileInfo {
            file_ref,
            partition_value: self.partition_value.clone(),
            start_offset: self.current_file_start_offset,
            end_offset,
            metadata,
            modification_time: SystemTime::now(),
        });

        self.current_file_start_offset = end_offset + 1;

        Ok(())
    }
}

#[async_trait::async_trait]
impl BatchWriter for ParquetBatchWriter {
    async fn write_batch(&mut self, data: RecordBatch) -> Result<()> {
        let (data, metadata) = {
            let mut inner = self.inner.lock().map_err(|_| {
                InternalSnafu {
                    message: "poisoned lock".to_string(),
                }
                .build()
            })?;

            inner.write(&data)?;

            if inner.current_file_size() < self.target_file_size_bytes {
                return Ok(());
            }

            let (data, metadata) = inner.finish()?;
            assert!(!data.is_empty(), "data should not be empty");
            (data, metadata)
        };

        self.upload_file(data, metadata).await
    }

    async fn finish(&mut self) -> Result<Vec<FileInfo>> {
        let (data, metadata) = {
            let mut inner = self.inner.lock().map_err(|_| {
                InternalSnafu {
                    message: "poisoned lock".to_string(),
                }
                .build()
            })?;

            let (data, metadata) = inner.finish()?;
            (data, metadata)
        };

        if !data.is_empty() {
            self.upload_file(data, metadata).await?;
        }

        assert!(
            self.current_file_start_offset == self.end_offset + 1,
            "Parquet offset accounting is off"
        );

        let written = std::mem::take(&mut self.written);

        Ok(written)
    }
}
