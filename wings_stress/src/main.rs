use clap::{Args, Parser};
use error::{ClientSnafu, WriteSnafu};
use snafu::ResultExt;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use wings_client::{TopicClient, WingsClient};
use wings_control_plane::{
    cluster_metadata::{ClusterMetadata, tonic::ClusterMetadataClient},
    resources::{NamespaceName, TopicName},
};

use crate::{
    error::{
        ClusterMetadataSnafu, ConnectionSnafu, InvalidNamespaceNameSnafu, InvalidRangeSnafu,
        InvalidRemoteUrlSnafu, JoinSnafu, Result,
    },
    generators::{RequestGenerator, TopicType},
    helpers::parse_range,
};

mod conversions;
mod error;
mod generators;
mod helpers;

#[derive(Parser)]
#[command(name = "wings-stress")]
#[command(about = "Wings stress testing CLI")]
#[command(version)]
struct Cli {
    /// The type of topics to generate.
    ///
    /// Repeat this flag to generate multiple topic types.
    #[arg(long, value_enum)]
    topic: Vec<TopicType>,
    /// How many messages to send per second per partition.
    #[arg(long, default_value = "1000")]
    rate: u64,
    /// The number of partitions to use for each topic.
    #[arg(long, default_value = "1")]
    partitions: u64,
    /// The batch size for each partition.
    ///
    /// Either provide a number (e.g. 1000) or a range (e.g. 1000-2000).
    #[arg(long, default_value = "1000")]
    batch_size: String,
    /// Namespace name
    #[arg(long, default_value = "tenants/default/namespaces/default")]
    namespace: String,
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

    let batch_size_range = parse_range(&cli.batch_size).context(InvalidRangeSnafu {})?;
    let namespace = NamespaceName::parse(&cli.namespace).context(InvalidNamespaceNameSnafu {})?;

    println!("Running stress test");
    println!("  Namespace: {}", namespace);
    println!(
        "  Topics: {}",
        cli.topic
            .iter()
            .map(|t| format!("{:?}", t))
            .collect::<Vec<_>>()
            .join(", ")
    );
    println!("  Batch Size: {:?}", batch_size_range);
    println!("  Partitions: {:?}", cli.partitions);

    let cluster_meta = cli.remote.cluster_metadata_client().await?;
    let ingestion_client = cli.remote.ingestion_client().await?;

    let mut tasks = JoinSet::new();

    for topic in cli.topic.into_iter() {
        let topic_name = ensure_topic_exists(&cluster_meta, &namespace, &topic).await?;
        let topic_client = ingestion_client
            .topic(topic_name.clone())
            .await
            .context(ClientSnafu {})?;
        let batch_size_range = batch_size_range.clone();
        let ct = ct.clone();
        tasks.spawn(async move {
            let request_generator = RequestGenerator::new(topic, batch_size_range, cli.partitions);
            run_stress_test_for_topic(request_generator, topic_client, ct).await
        });
    }

    while let Some(result) = tasks.join_next().await.transpose().context(JoinSnafu {})? {
        ct.cancel();
        result?;
    }

    Ok(())
}

async fn run_stress_test_for_topic(
    mut generator: RequestGenerator,
    client: TopicClient,
    ct: CancellationToken,
) -> Result<()> {
    // TODO: this loop should send multiple requests in parallel
    println!("Starting stress test for topic");
    loop {
        if ct.is_cancelled() {
            println!("cancelled");
            break;
        }

        let request = generator.create_request();
        println!(
            "Sending request {:?} {}",
            request.partition_value,
            request.data.num_rows()
        );
        let response = client
            .push(request)
            .await
            .context(ClientSnafu {})?
            .wait_for_response()
            .await
            .context(WriteSnafu {})?;

        println!(
            "Batch written: {} -- {}",
            response.start_offset, response.end_offset
        );
    }

    println!("Done");

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
            .await
            .context(ConnectionSnafu {})?;

        Ok(channel)
    }
}

async fn ensure_topic_exists(
    admin: &ClusterMetadataClient<Channel>,
    namespace: &NamespaceName,
    topic: &TopicType,
) -> Result<TopicName> {
    let topic_name = TopicName::new_unchecked(topic.topic_name(), namespace.clone());
    match admin.get_topic(topic_name.clone()).await {
        Ok(_) => return Ok(topic_name),
        Err(err) if err.is_not_found() => {}
        Err(err) => {
            return Err(err).context(ClusterMetadataSnafu {
                operation: "get_topic",
            });
        }
    };

    admin
        .create_topic(topic_name.clone(), topic.topic_options())
        .await
        .context(ClusterMetadataSnafu {
            operation: "create_topic",
        })?;

    Ok(topic_name)
}
