use clap::{Args, Parser};
use snafu::ResultExt;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use wings_metadata_core::admin::{Admin, NamespaceName, RemoteAdminService, TopicName};
use wings_push_client::HttpPushClient;

use crate::{
    error::{
        AdminSnafu, ConnectionSnafu, HttpPushSnafu, InvalidNamespaceNameSnafu, InvalidRangeSnafu,
        InvalidRemoteUrlSnafu, Result,
    },
    generators::{RequestGenerator, TopicType},
    helpers::parse_range,
};

mod error;
mod generators;
mod helpers;

#[derive(Parser)]
#[command(name = "wings-stress")]
#[command(about = "Wings stress testing CLI")]
#[command(version)]
struct Cli {
    /// HTTP ingestor address.
    ///
    /// The address where the Wings HTTP ingestor is running. Should match
    /// the address used in the 'wings dev' command.
    #[arg(long, default_value = "http://127.0.0.1:7780")]
    http_address: String,
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

    let admin = cli.remote.admin_client().await?;
    let client = HttpPushClient::new(&cli.http_address, namespace.clone());

    for topic in cli.topic.iter() {
        ensure_topic_exists(&admin, &namespace, topic).await?;
    }

    let mut request_generator = RequestGenerator::new(cli.topic, batch_size_range, cli.partitions);

    loop {
        let push_req = request_generator.create_request(&client).send();

        tokio::select! {
            _ = ct.cancelled() => {
                break;
            }
            response = push_req => {
                let response = response.context(HttpPushSnafu {})?;
                let success_count = response.batches.iter().filter(|b| b.is_success()).count();
                let error_count = response.batches.iter().filter(|b| b.is_error()).count();
                let success_offsets = response
                    .batches
                    .iter()
                    .flat_map(|b| b.as_success())
                    .map(|(s, e)| format!("{}-{}", s, e))
                    .collect::<Vec<_>>()
                    .join(" ");
                println!("Success {} Error {}\t{}", success_count, error_count, success_offsets);
            }
        }
    }

    Ok(())
}

impl RemoteArgs {
    /// Create a new gRPC client for the admin service.
    pub async fn admin_client(&self) -> Result<RemoteAdminService<Channel>> {
        let channel = self.channel().await?;
        Ok(RemoteAdminService::new(channel))
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
    admin: &RemoteAdminService<Channel>,
    namespace: &NamespaceName,
    topic: &TopicType,
) -> Result<()> {
    let topic_name = TopicName::new_unchecked(topic.topic_name(), namespace.clone());
    match admin.get_topic(topic_name.clone()).await {
        Ok(_) => return Ok(()),
        Err(err) if err.is_not_found() => {}
        Err(err) => {
            return Err(err).context(AdminSnafu {
                operation: "get_topic",
            });
        }
    };

    admin
        .create_topic(topic_name, topic.topic_options())
        .await
        .context(AdminSnafu {
            operation: "create_topic",
        })?;

    Ok(())
}
