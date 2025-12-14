mod error;
mod parquet;

use datafusion::arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use snafu::ResultExt;
use std::sync::Arc;

use crate::{
    cluster_metadata::ClusterMetadata,
    data_lake::{
        error::{ClusterMetadataSnafu, ObjectStoreSnafu},
        parquet::ParquetDataLake,
    },
    object_store::ObjectStoreFactory,
    resources::{DataLakeConfiguration, NamespaceRef, PartitionValue, TopicRef},
};

pub use self::error::DataLakeError;

use self::error::Result;

#[async_trait::async_trait]
pub trait BatchWriter: Send + Sync {
    async fn write_batch(&mut self, data: RecordBatch) -> Result<()>;
    async fn commit(&mut self) -> Result<()>;
}

#[async_trait::async_trait]
pub trait DataLake: Send + Sync {
    /// Append data to a topic's table in the data lake.
    async fn batch_writer(
        &self,
        topic: TopicRef,
        schema: SchemaRef,
        partition_value: Option<PartitionValue>,
        start_offset: u64,
        end_offset: u64,
    ) -> Result<Box<dyn BatchWriter>>;
}

/// Factory for creating data lake instances.
#[derive(Clone)]
pub struct DataLakeFactory {
    cluster_meta: Arc<dyn ClusterMetadata>,
    object_store_factory: Arc<dyn ObjectStoreFactory>,
}

impl DataLakeFactory {
    /// Creates a new data lake factory.
    pub fn new(
        cluster_meta: Arc<dyn ClusterMetadata>,
        object_store_factory: Arc<dyn ObjectStoreFactory>,
    ) -> Self {
        Self {
            cluster_meta,
            object_store_factory,
        }
    }

    /// Creates a new data lake client.
    pub async fn create_data_lake(&self, namespace: NamespaceRef) -> Result<Arc<dyn DataLake>> {
        let object_store = self
            .object_store_factory
            .create_object_store(namespace.object_store.clone())
            .await
            .context(ObjectStoreSnafu {})?;

        let data_lake = self
            .cluster_meta
            .get_data_lake(namespace.data_lake.clone())
            .await
            .context(ClusterMetadataSnafu {
                operation: "get_data_lake",
            })?;

        match data_lake.data_lake {
            DataLakeConfiguration::Parquet(_config) => {
                let data_lake: Arc<_> = ParquetDataLake::new(object_store).into();
                Ok(data_lake)
            }
            DataLakeConfiguration::Iceberg(_config) => {
                todo!()
            }
        }
    }
}
