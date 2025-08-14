use std::time::SystemTime;

use clap::Parser;
use snafu::ResultExt;
use tokio_util::sync::CancellationToken;
use wings_metadata_core::{
    admin::{Admin, TopicName},
    offset_registry::{OffsetLocation, OffsetRegistry},
    partition::PartitionValue,
};
use wings_object_store::{LocalFileSystemFactory, ObjectStoreFactory};

use crate::{
    error::{
        AdminSnafu, InvalidResourceNameSnafu, IoSnafu, ObjectStoreSnafu, OffsetRegistrySnafu,
        PartitionValueParseSnafu, Result,
    },
    remote::RemoteArgs,
};

/// Debug the content of a topic
#[derive(Parser)]
pub struct DebugDataArgs {
    /// Topic to debug
    topic: String,

    /// Message offset.
    offset: u64,

    /// Partition value.
    #[arg(long)]
    partition: Option<String>,

    /// Output file path.
    #[arg(long)]
    output: String,

    /// Base path for the object storage.
    #[arg(long)]
    base_path: String,

    #[clap(flatten)]
    remote: RemoteArgs,
}

impl DebugDataArgs {
    pub async fn run(self, _ct: CancellationToken) -> Result<()> {
        let admin = self.remote.admin_client().await?;
        let offset_registry = self.remote.offset_registry_client().await?;

        let object_store_factory =
            LocalFileSystemFactory::new(self.base_path).context(ObjectStoreSnafu {})?;

        let topic_name = TopicName::parse(&self.topic)
            .context(InvalidResourceNameSnafu { resource: "topic" })?;

        let topic = admin
            .get_topic(topic_name.clone())
            .await
            .context(AdminSnafu {
                operation: "get_topic",
            })?;

        let namespace = admin
            .get_namespace(topic_name.parent.clone())
            .await
            .context(AdminSnafu {
                operation: "get_namespace",
            })?;

        let object_store = object_store_factory
            .create_object_store(namespace.default_object_store_config)
            .await
            .context(ObjectStoreSnafu {})?;

        let partition_value = PartitionValue::parse_with_datatype_option(
            topic.partition_column_data_type(),
            self.partition.as_ref().map(String::as_str),
        )
        .context(PartitionValueParseSnafu {})?;

        let Some(offset_location) = offset_registry
            .offset_location(topic_name, partition_value, self.offset, SystemTime::now())
            .await
            .context(OffsetRegistrySnafu {
                operation: "offset_location",
            })?
        else {
            return Ok(());
        };

        match offset_location {
            OffsetLocation::Folio(location) => {
                println!("Fetching {}", location.file_ref);
                let response = object_store
                    .get(&location.file_ref.into())
                    .await
                    .context(ObjectStoreSnafu {})?;

                let bytes = response.bytes().await.context(ObjectStoreSnafu {})?;

                println!(
                    "Writing data for offset range {}-{}",
                    location.start_offset, location.end_offset
                );

                let parquet_start = location.offset_bytes as usize;
                let parquet_end = parquet_start + location.size_bytes as usize;

                println!("Offset in file {}-{}", parquet_start, parquet_end);

                std::fs::write(&self.output, &bytes[parquet_start..parquet_end])
                    .context(IoSnafu {})?;

                println!("Data written to {}", self.output);
            }
        }

        Ok(())
    }
}
