use arrow::util::pretty::pretty_format_batches;
use clap::Parser;
use snafu::ResultExt;
use tokio_util::sync::CancellationToken;
use wings_control_plane::resources::{PartitionValue, TopicName};

use crate::{
    error::{ArrowSnafu, ClientSnafu, InvalidResourceNameSnafu, PartitionValueParseSnafu, Result},
    remote::RemoteArgs,
};

/// Push messages to Wings topics
#[derive(Parser)]
pub struct FetchArgs {
    /// Topic name
    topic: String,
    /// The first message to fetch
    #[arg(long, default_value = "0")]
    offset: u64,
    /// The partition value
    #[arg(long)]
    partition: Option<String>,
    /// Change the maximum batch size
    #[arg(long)]
    max_batch_size: Option<usize>,
    /// Change the minimum batch size
    #[arg(long)]
    min_batch_size: Option<usize>,
    #[clap(flatten)]
    remote: RemoteArgs,
}

impl FetchArgs {
    pub async fn run(self, _ct: CancellationToken) -> Result<()> {
        let client = self.remote.wings_client().await?;

        let topic_name = TopicName::parse(&self.topic)
            .context(InvalidResourceNameSnafu { resource: "topic" })?;

        let mut topic_client = client
            .fetch_client(topic_name)
            .await
            .context(ClientSnafu {})?
            .with_offset(self.offset);

        if let Some(partition) = self.partition.as_ref() {
            let partition = PartitionValue::parse_with_datatype_option(
                topic_client.partition_data_type().as_ref(),
                Some(partition),
            )
            .context(PartitionValueParseSnafu {})?;
            topic_client = topic_client.with_partition(partition);
        }

        if let Some(max_batch_size) = self.max_batch_size {
            topic_client = topic_client.with_max_batch_size(max_batch_size);
        }

        if let Some(min_batch_size) = self.min_batch_size {
            topic_client = topic_client.with_min_batch_size(min_batch_size);
        }

        loop {
            let batches = topic_client.fetch_next().await.context(ClientSnafu {})?;

            if batches.is_empty() {
                break;
            }

            let out = pretty_format_batches(&batches).context(ArrowSnafu {})?;
            println!("{out}");
        }

        Ok(())
    }
}
