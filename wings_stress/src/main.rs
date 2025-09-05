// wings-stress CLI entry point
use clap::{Parser, Subcommand, ValueEnum};
use tokio_util::sync::CancellationToken;

use crate::error::Result;

mod error;

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
    /// The batch size for each topic and partition.
    ///
    /// Either provide a number (e.g. 1000) or a range (e.g. 1000-2000).
    #[arg(long)]
    batch_size: String,
    /// The number of partitions to use for each topic.
    ///
    /// Either provide a number (e.g. 1) or a range (e.g. 1-10).
    #[arg(long, default_value = "1")]
    partitions: String,
    /// How many partitions to use per request.
    ///
    /// Either provide a number (e.g. 1) or a range (e.g. 1-10).
    #[arg(long, default_value = "1")]
    partitions_per_request: String,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
enum TopicType {
    /// A small topic with just a few columns.
    Small,
}

#[tokio::main]
#[snafu::report]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let ct = CancellationToken::new();
    let ct_clone = ct.clone();

    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        ct_clone.cancel();
    });

    println!("Running with topics: {:?}", cli.topic);

    Ok(())
}
