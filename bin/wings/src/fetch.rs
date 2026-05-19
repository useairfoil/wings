use arrow::util::pretty::pretty_format_batches;
use clap::Parser;
use snafu::ResultExt;
use tokio_util::sync::CancellationToken;
use wings_resources::{PartitionValue, TableName};

use crate::{
    error::{
        ArrowSnafu, ClientSnafu, InvalidArgumentSnafu, InvalidResourceNameSnafu,
        PartitionValueParseSnafu, Result,
    },
    remote::RemoteArgs,
};

/// Push messages to Wings tables
#[derive(Parser)]
pub struct FetchArgs {
    /// Table name
    table: String,
    /// The first message to fetch
    #[arg(long, default_value = "0")]
    seqnum: u64,
    /// The partition value
    #[arg(long)]
    partition: Option<String>,
    /// Change the maximum batch size
    #[arg(long)]
    max_batch_size: Option<usize>,
    /// Change the minimum batch size
    #[arg(long)]
    min_batch_size: Option<usize>,
    /// Change the timeout, e.g. 250ms or 1s
    #[arg(long)]
    timeout: Option<String>,
    #[clap(flatten)]
    remote: RemoteArgs,
}

impl FetchArgs {
    pub async fn run(self, ct: CancellationToken) -> Result<()> {
        let client = self.remote.wings_client().await?;

        let table_name = TableName::parse(&self.table)
            .context(InvalidResourceNameSnafu { resource: "table" })?;

        let mut table_client = client
            .fetch_client(table_name)
            .await
            .context(ClientSnafu {})?
            .with_seqnum(self.seqnum);

        if let Some(partition) = self.partition.as_ref() {
            let partition = PartitionValue::parse_with_datatype_option(
                table_client.partition_data_type().as_ref(),
                Some(partition),
            )
            .context(PartitionValueParseSnafu {})?;
            table_client = table_client.with_partition(partition);
        }

        if let Some(max_batch_size) = self.max_batch_size {
            table_client = table_client.with_max_batch_size(max_batch_size);
        }

        if let Some(min_batch_size) = self.min_batch_size {
            table_client = table_client.with_min_batch_size(min_batch_size);
        }

        if let Some(timeout) = self.timeout {
            let timeout = duration_str::parse(timeout).map_err(|message| {
                InvalidArgumentSnafu {
                    message,
                    name: "timeout",
                }
                .build()
            })?;
            table_client = table_client.with_timeout(timeout);
        }

        loop {
            let batches = match ct.run_until_cancelled(table_client.fetch_next()).await {
                Some(fut) => fut.context(ClientSnafu {})?,
                None => return Ok(()),
            };

            if batches.is_empty() {
                println!("...");
                continue;
            }

            let out = pretty_format_batches(&batches).context(ArrowSnafu {})?;
            println!("{out}");
        }
    }
}
