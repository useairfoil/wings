use clap::{Parser, Subcommand};
use error::ObservabilitySnafu;
use flight::FlightCommands;
use snafu::ResultExt;
use tokio_util::sync::CancellationToken;
use wings_observability::init_observability;

use crate::{
    cluster::ClusterMetadataCommands, dev::DevArgs, error::Result, fetch::FetchArgs,
    push::PushArgs, sql::SqlArgs,
};

mod cluster;
mod dev;
mod error;
mod fetch;
mod flight;
mod push;
mod remote;
mod sql;

#[derive(Parser)]
#[command(name = "wings")]
#[command(about = "Wings CLI")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the Wings service in development mode
    Dev {
        #[clap(flatten)]
        inner: DevArgs,
    },
    /// Interact with the cluster metadata API
    Cluster {
        #[command(subcommand)]
        inner: ClusterMetadataCommands,
    },
    /// Interact with the Apache Flight SQL API
    Flight {
        #[command(subcommand)]
        inner: FlightCommands,
    },
    /// Push messages to Wings topics
    Push {
        #[clap(flatten)]
        inner: PushArgs,
    },
    Fetch {
        #[clap(flatten)]
        inner: FetchArgs,
    },
    /// Run SQL queries against a namespace
    Sql {
        #[clap(flatten)]
        inner: SqlArgs,
    },
}

#[tokio::main]
#[snafu::report]
async fn main() -> Result<()> {
    init_observability(env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"))
        .context(ObservabilitySnafu {})?;

    let cli = Cli::parse();

    let ct = CancellationToken::new();

    let ct_clone = ct.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        ct_clone.cancel();
    });

    match cli.command {
        Commands::Dev { inner } => inner.run(ct).await,
        Commands::Cluster { inner } => inner.run(ct).await,
        Commands::Flight { inner } => inner.run(ct).await,
        Commands::Push { inner } => inner.run(ct).await,
        Commands::Fetch { inner } => inner.run(ct).await,
        Commands::Sql { inner } => inner.run(ct).await,
    }
}
