use std::time::Duration;

use clap::{Args, Parser};
use snafu::ResultExt;
use tokio::{sync::mpsc, task::JoinSet};
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use wings_client::WingsClient;
use wings_control_plane_core::cluster_metadata::{
    ClusterMetadata, TopicView, tonic::ClusterMetadataClient,
};
use wings_resources::{CompactionConfiguration, NamespaceName, Topic, TopicName, TopicOptions};
use wings_schema::{DataType, Field, Schema, SchemaBuilder};

use crate::{
    error::{ClusterMetadataSnafu, InvalidRemoteUrlSnafu, InvalidResourceNameSnafu, Result},
    log::{Event, run_log_loop},
    run::{RunContext, run_test},
};

mod error;
mod log;
mod run;

#[derive(Parser)]
#[command(name = "wings-stress")]
#[command(about = "Wings stress testing CLI")]
#[command(version)]
struct Cli {
    /// The number of concurrent clients
    #[arg(long, default_value = "10")]
    concurrency: u64,
    /// The number of iterations for each client
    #[arg(long, default_value = "1000")]
    iterations: usize,
    /// The batch size for each push request.
    #[arg(long, default_value = "773")]
    batch_size: usize,
    /// The number of partitions for each topic.
    ///
    /// If unspecified, the topic won't have a partition column.
    #[arg(long)]
    num_partitions: Option<usize>,
    /// The topic's namespace.
    #[arg(long, default_value = "tenants/default/namespaces/default")]
    namespace: String,
    /// If specified, use an existing topic.
    #[arg(long)]
    topic_id: Option<String>,
    #[clap(flatten)]
    remote: RemoteArgs,
}

/// Arguments for configuring the remote server connection.
#[derive(Args, Debug, Clone)]
pub struct RemoteArgs {
    /// The address of the remote Wings admin server
    #[arg(long, default_value = "http://localhost:7777")]
    pub remote_address: String,
}

#[tokio::main]
#[snafu::report]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let ct = CancellationToken::new();

    tokio::spawn({
        let ct = ct.clone();
        async move {
            let _ = tokio::signal::ctrl_c().await;
            ct.cancel();
        }
    });

    let namespace = NamespaceName::parse(&cli.namespace).context(InvalidResourceNameSnafu {
        resource: "namespace",
    })?;

    let cluster_meta = cli.remote.cluster_metadata_client().await?;
    let ingestion_client = cli.remote.ingestion_client().await?;

    let topic_id = cli.topic_id.unwrap_or_else(new_random_topic_id);

    let topic_name = TopicName::new(topic_id, namespace)
        .context(InvalidResourceNameSnafu { resource: "topic" })?;

    let topic = ensure_topic_exists(
        &cluster_meta,
        topic_name.clone(),
        cli.num_partitions.is_some(),
    )
    .await?;

    let (tx, rx) = mpsc::channel::<Event>(1024);

    let logger_handle = tokio::spawn(async move { run_log_loop(rx).await });

    let mut tasks = JoinSet::new();

    let ctx = RunContext::new(cli.batch_size, cli.iterations, cli.num_partitions);

    for client_id in 0..cli.concurrency {
        let ctx = ctx.clone();
        let tx = tx.clone();
        let client = ingestion_client.clone();
        let topic = topic.clone();
        let ct = ct.clone();
        tasks.spawn(async move { run_test(ctx, client_id, tx, client, topic, ct).await });
    }

    while let Some(result) = tasks.join_next().await.transpose()? {
        result?;
    }

    // Close the channel to stop the logger task
    drop(tx);

    logger_handle.await??;

    Ok(())
}

impl RemoteArgs {
    /// Create a new gRPC client for the admin service.
    pub async fn cluster_metadata_client(&self) -> Result<ClusterMetadataClient<Channel>> {
        let channel = self.channel().await?;
        Ok(ClusterMetadataClient::new(channel))
    }

    pub async fn ingestion_client(&self) -> Result<WingsClient> {
        let channel = self.channel().await?;
        Ok(WingsClient::new(channel))
    }

    async fn channel(&self) -> Result<Channel> {
        let channel = Channel::from_shared(self.remote_address.clone())
            .context(InvalidRemoteUrlSnafu {})?
            .connect()
            .await?;

        Ok(channel)
    }
}

async fn ensure_topic_exists(
    cluster_meta: &ClusterMetadataClient<Channel>,
    topic_name: TopicName,
    has_partition: bool,
) -> Result<Topic> {
    match cluster_meta
        .get_topic(topic_name.clone(), TopicView::Basic)
        .await
    {
        Ok(topic) => return Ok(topic),
        Err(err) if err.is_not_found() => {}
        Err(err) => {
            return Err(err).context(ClusterMetadataSnafu {
                operation: "get_topic",
            });
        }
    };

    let options = TopicOptions {
        schema: topic_schema(has_partition),
        compaction: CompactionConfiguration {
            freshness: Duration::from_secs(60),
            ..Default::default()
        },
        partition_key: if has_partition { Some(0) } else { None },
        description: Some("Linearizability test table with no partition".to_string()),
    };

    cluster_meta
        .create_topic(topic_name.clone(), options)
        .await
        .context(ClusterMetadataSnafu {
            operation: "create_topic",
        })
}

fn topic_schema(has_partition: bool) -> Schema {
    // We want the partition column to be the first column to test some internal
    // logic related to column reordering.
    let mut columns = Vec::default();
    if has_partition {
        columns.push(Field::new("part", 0, DataType::UInt64, false));
    }
    columns.push(Field::new("col", 1, DataType::UInt64, false));

    // PANIC: the schema is valid.
    SchemaBuilder::new(columns).build().expect("topic schema")
}

fn new_random_topic_id() -> String {
    let r = ulid::Ulid::new().to_string().to_lowercase();
    format!("t-{r}")
}
