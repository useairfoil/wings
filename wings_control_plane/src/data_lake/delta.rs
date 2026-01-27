use std::sync::Arc;

use bytesize::ByteSize;
use deltalake_aws::logstore::default_s3_logstore;
use deltalake_core::{
    DeltaTable,
    logstore::{LogStore, StorageConfig},
    protocol::SaveMode,
};
use object_store::{ObjectStore, prefix::PrefixStore};
use snafu::ResultExt;
use tracing::info;

use crate::{
    data_lake::{BatchWriter, DataLake, error::InvalidSchemaSnafu},
    resources::{ObjectStoreName, PartitionValue, TopicName, TopicRef},
    schema::Field,
};

use super::{error::Result, parquet::ParquetBatchWriter};

pub struct DeltaDataLake {
    object_store: Arc<dyn ObjectStore>,
    object_store_name: ObjectStoreName,
}

impl DeltaDataLake {
    pub fn new(object_store_name: ObjectStoreName, object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            object_store,
            object_store_name,
        }
    }

    pub fn new_log_store(&self, topic_name: &TopicName) -> Result<Arc<dyn LogStore>> {
        let location = self.object_store_name.wings_object_store_url()?;

        // We prefix the object store with the topic's full path
        let topic_store: Arc<_> =
            PrefixStore::new(self.object_store.clone(), topic_name.name().as_str()).into();

        let log_store = default_s3_logstore(
            topic_store.clone(),
            topic_store,
            location.as_ref(),
            &StorageConfig::default(),
        );

        Ok(log_store)
    }
}

#[async_trait::async_trait]
impl DataLake for DeltaDataLake {
    async fn create_table(&self, topic: TopicRef) -> Result<String> {
        let log_store = self.new_log_store(&topic.name)?;
        let columns = topic
            .schema()
            .fields_iter()
            .map(convert_field)
            .collect::<Result<Vec<_>>>()?;

        let table = DeltaTable::new(log_store.clone(), Default::default())
            .create()
            .with_save_mode(SaveMode::ErrorIfExists)
            .with_columns(columns)
            .await
            .unwrap();

        info!(?table, "Delta table created");

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
}

fn convert_field(field: &Field) -> Result<deltalake_core::StructField> {
    field.try_into().context(InvalidSchemaSnafu {})
}
