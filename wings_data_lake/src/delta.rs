use std::{collections::HashMap, sync::Arc, time::SystemTime};

use bytesize::ByteSize;
use deltalake_aws::logstore::default_s3_logstore;
use deltalake_core::{
    DeltaTable,
    kernel::{
        Action, Add,
        transaction::{CommitBuilder, CommitProperties, TableReference},
    },
    logstore::{LogStore, StorageConfig},
    protocol::{DeltaOperation, SaveMode},
};
use object_store::{ObjectStore, prefix::PrefixStore};
use serde_json::Value;
use snafu::ResultExt;
use tracing::{debug, info, warn};
use wings_control_plane_core::log_metadata::{FileInfo, FileMetadata};
use wings_resources::{ObjectStoreName, PartitionPosition, PartitionValue, TopicName, TopicRef};
use wings_schema::Field;

use super::{error::Result, parquet::ParquetBatchWriter};
use crate::{BatchWriter, DataLake, error::InvalidSchemaSnafu};

pub struct DeltaDataLake {
    object_store: Arc<dyn ObjectStore>,
    object_store_name: ObjectStoreName,
}

#[derive(Debug)]
struct DeltaLogSerializablePartitionValue<'a>(&'a Option<PartitionValue>);

impl DeltaDataLake {
    pub fn new(object_store_name: ObjectStoreName, object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            object_store,
            object_store_name,
        }
    }

    pub fn delta_log_location(&self, topic_name: &TopicName) -> String {
        topic_name.name().to_string()
    }

    pub fn new_log_store(&self, topic_name: &TopicName) -> Result<Arc<dyn LogStore>> {
        let location = self.object_store_name.wings_object_store_url()?;

        // We prefix the object store with the topic's full path
        let topic_store: Arc<_> = PrefixStore::new(
            self.object_store.clone(),
            self.delta_log_location(topic_name).as_str(),
        )
        .into();

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
            .schema_with_metadata(PartitionPosition::Original)
            .context(InvalidSchemaSnafu {})?
            .fields_iter()
            .map(convert_field)
            .collect::<Result<Vec<_>>>()?;

        let partition_columns = if let Some(column) = topic.partition_field() {
            vec![column.name()]
        } else {
            vec![]
        };

        let table = DeltaTable::new(log_store.clone(), Default::default())
            .create()
            .with_save_mode(SaveMode::ErrorIfExists)
            .with_columns(columns)
            .with_partition_columns(partition_columns)
            .await?;

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

    async fn commit_data(&self, topic: TopicRef, new_files: &[FileInfo]) -> Result<String> {
        let mut actions: Vec<Action> = Vec::with_capacity(new_files.len());

        // Make sure to add the `/` at the end to strip it from the file reference,
        // making it a relative path.
        let root_location = format!("{}/", self.delta_log_location(&topic.name));

        let partition_field = topic.partition_field();

        for file in new_files {
            let partition_value = DeltaLogSerializablePartitionValue(&file.partition_value);
            let partition_values = partition_field
                .map(|field| {
                    HashMap::<String, Option<String>>::from([(
                        field.name().to_string(),
                        partition_value.to_string(),
                    )])
                })
                .unwrap_or_default();

            let stats = serde_json::to_string(&convert_statistics_to_delta(&file.metadata))?;

            // Remove string prefix and create a new file ref relative to the topic's root dir.
            if let Some(file_ref) = file.file_ref.strip_prefix(&root_location) {
                let modification_time = file
                    .modification_time
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .map(|duration| duration.as_millis() as i64)
                    .unwrap_or_default();

                let add = Add {
                    path: file_ref.to_string(),
                    partition_values,
                    size: file.metadata.file_size.as_u64() as _,
                    modification_time,
                    data_change: true,
                    base_row_id: Some(file.start_offset as _),
                    stats: Some(stats),
                    ..Default::default()
                };
                actions.push(Action::Add(add));
            } else {
                warn!(
                    "File {} does not belong to topic {}",
                    file.file_ref, topic.name
                );
            }
        }

        debug!(topic = %topic.name, "Loading Delta table to commit");
        let log_store = self.new_log_store(&topic.name)?;
        let mut table = DeltaTable::new(log_store.clone(), Default::default());
        table.load().await?;

        let operation = DeltaOperation::Write {
            mode: SaveMode::Append,
            partition_by: partition_field.map(|field| vec![field.name().to_string()]),
            predicate: None,
        };

        let commit_properties = CommitProperties::default();
        let commit = CommitBuilder::from(commit_properties)
            .with_actions(actions)
            .build(
                table.state.as_ref().map(|f| f as &dyn TableReference),
                log_store,
                operation,
            )
            .await?;

        info!(topic = %topic.name, version = commit.version, "Delta table committed");
        Ok(commit.version().to_string())
    }
}

fn convert_field(field: &Field) -> Result<deltalake_core::StructField> {
    field.try_into().context(InvalidSchemaSnafu {})
}

impl DeltaLogSerializablePartitionValue<'_> {
    // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#partition-value-serialization
    fn to_string(&self) -> Option<String> {
        match self.0.as_ref()? {
            PartitionValue::Null => None,
            PartitionValue::Boolean(true) => Some("true".to_string()),
            PartitionValue::Boolean(false) => Some("false".to_string()),
            PartitionValue::Int8(n) => Some(n.to_string()),
            PartitionValue::Int16(n) => Some(n.to_string()),
            PartitionValue::Int32(n) => Some(n.to_string()),
            PartitionValue::Int64(n) => Some(n.to_string()),
            PartitionValue::UInt8(n) => Some(n.to_string()),
            PartitionValue::UInt16(n) => Some(n.to_string()),
            PartitionValue::UInt32(n) => Some(n.to_string()),
            PartitionValue::UInt64(n) => Some(n.to_string()),
            PartitionValue::String(s) => Some(s.clone()),
            PartitionValue::Bytes(b) => Some(
                b.iter()
                    .map(|b| format!("\\u{:04x}", b))
                    .collect::<String>(),
            ),
        }
    }
}

fn convert_statistics_to_delta(stats: &FileMetadata) -> Value {
    let mut inner = serde_json::Map::new();
    // The number of records in this data file.
    inner.insert("numRecords".to_string(), stats.num_rows.into());
    // Whether per-column statistics are currently tight or wide
    inner.insert("tightBounds".to_string(), true.into());
    // TODO: convert wings flat statistics into the nested delta format
    Value::Object(inner)
}
