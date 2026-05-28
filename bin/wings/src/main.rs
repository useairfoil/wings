use clap::{Parser, Subcommand};
use error::ObservabilitySnafu;
use snafu::ResultExt;
use tokio_util::sync::CancellationToken;
use wings_observability::{MetricsExporter, init_observability};

use crate::{dev::DevArgs, error::Result};

mod dev;
mod error;

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
}

#[tokio::main]
#[snafu::report]
async fn main() -> Result<()> {
    init_observability(
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION"),
        MetricsExporter::default(),
    )
    .context(ObservabilitySnafu {})?;

    let cli = Cli::parse();

    let ct = CancellationToken::new();

    tokio::spawn({
        let ct = ct.clone();
        async move {
            let _ = tokio::signal::ctrl_c().await;
            ct.cancel();
        }
    });

    match cli.command {
        Commands::Dev { inner } => inner.run(ct).await,
    }
}
