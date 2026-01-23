use std::sync::Arc;

use datafusion::arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload, path::Path};
use parquet::{
    arrow::ArrowWriter,
    file::{metadata::KeyValue, properties::WriterProperties},
};
use snafu::ResultExt;
use tokio::sync::Mutex;
use tracing::debug;

use crate::{
    data_lake::{
        BatchWriter, DataLake,
        error::{ObjectStoreSnafu, ParquetPathSnafu, ParquetSnafu},
    },
    paths::format_parquet_path,
    resources::{PartitionValue, TopicRef},
};

use super::error::Result;

const DEFAULT_BUFFER_CAPACITY: usize = 8 * 1024 * 1024;

pub struct ParquetDataLake {
    object_store: Arc<dyn ObjectStore>,
}

pub struct ParquetBatchWriter {
    // TODO: check if the mutex is held across async points. if not replace.
    inner: Mutex<ArrowWriter<Vec<u8>>>,
    file_ref: Path,
    object_store: Arc<dyn ObjectStore>,
}

impl ParquetDataLake {
    pub fn new(object_store: Arc<dyn ObjectStore>) -> Self {
        Self { object_store }
    }
}

#[async_trait::async_trait]
impl DataLake for ParquetDataLake {
    async fn batch_writer(
        &self,
        topic: TopicRef,
        schema: SchemaRef,
        partition_value: Option<PartitionValue>,
        start_offset: u64,
        end_offset: u64,
    ) -> Result<Box<dyn BatchWriter>> {
        let inner = {
            let partition_value = partition_value.as_ref().map(|v| v.to_string());
            let kv_metadata = vec![
                KeyValue::new("WINGS:topic-name".to_string(), topic.name.to_string()),
                KeyValue::new("WINGS:partition-value".to_string(), partition_value),
            ];

            let write_properties = WriterProperties::builder()
                .set_key_value_metadata(kv_metadata.into())
                .build();

            let buffer = Vec::with_capacity(DEFAULT_BUFFER_CAPACITY);
            ArrowWriter::try_new(buffer, schema, write_properties.into())
                .context(ParquetSnafu {})?
        };

        let partition_field = topic
            .partition_field()
            .cloned()
            .map(|field| field.to_arrow_field().into());
        let file_ref: Path = format_parquet_path(&topic.name.parent)
            .with_offset_range(start_offset, end_offset)
            .with_partition(partition_field.as_ref(), partition_value.as_ref())
            .build()
            .context(ParquetPathSnafu {})?
            .into();

        let writer = ParquetBatchWriter {
            inner: Mutex::new(inner),
            file_ref,
            object_store: self.object_store.clone(),
        };

        Ok(Box::new(writer))
    }
}

#[async_trait::async_trait]
impl BatchWriter for ParquetBatchWriter {
    async fn write_batch(&mut self, data: RecordBatch) -> Result<()> {
        let mut writer = self.inner.lock().await;
        writer.write(&data).context(ParquetSnafu {})?;
        Ok(())
    }

    async fn commit(&mut self) -> Result<()> {
        let mut writer = self.inner.lock().await;
        let _parquet_metadata = writer.finish().context(ParquetSnafu {})?;
        let output_bytes = std::mem::take(&mut *writer.inner_mut());
        let payload = PutPayload::from_bytes(output_bytes.into());
        debug!(
            file_ref = %self.file_ref,
            "Uploading parquet file to storage"
        );

        self.object_store
            .put_opts(
                &self.file_ref,
                payload,
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
            .context(ObjectStoreSnafu {})?;

        Ok(())
    }
}
