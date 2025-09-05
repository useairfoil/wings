use clap::{Parser, Subcommand};
use observability::init_observability;
use tokio_util::sync::CancellationToken;

use crate::{
    admin::AdminCommands, dev::DevArgs, error::Result, fetch::FetchArgs, push::PushArgs,
    sql::SqlArgs,
};

mod admin;
mod dev;
mod error;
mod fetch;
mod observability;
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
    /// Interact with the admin API
    Admin {
        #[command(subcommand)]
        inner: AdminCommands,
    },
    /// Push messages to Wings topics
    Push {
        #[clap(flatten)]
        inner: PushArgs,
    },
    /// Run SQL queries against a namespace
    Sql {
        #[clap(flatten)]
        inner: SqlArgs,
    },
    /// Fetch messages from Wings topics
    Fetch {
        #[clap(flatten)]
        inner: FetchArgs,
    },
}

#[tokio::main]
#[snafu::report]
async fn main() -> Result<()> {
    init_observability(env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));

    let cli = Cli::parse();

    let ct = CancellationToken::new();

    let ct_clone = ct.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        ct_clone.cancel();
    });

    match cli.command {
        Commands::Dev { inner } => inner.run(ct).await,
        Commands::Admin { inner } => inner.run(ct).await,
        Commands::Push { inner } => inner.run(ct).await,
        Commands::Sql { inner } => inner.run(ct).await,
        Commands::Fetch { inner } => inner.run(ct).await,
    }
}
