use std::{error::Error, sync::Arc};

use clap::{Parser, Subcommand};
use tokio::signal::unix::SignalKind;
use tokio_util::sync::CancellationToken;
use tracing::info;
use wings::cmd::BrokerArgs;
use wings_common::clock::DefaultSystemClock;

#[derive(Parser)]
#[command(name = "wings")]
#[command(about = "Wings CLI")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Start the task queue broker.
    Broker(BrokerArgs),
}

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let clock: Arc<_> = DefaultSystemClock::default().into();

    wings_observability::init_observability(
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION"),
        clock,
    )?;

    let ct = CancellationToken::new();

    tokio::spawn({
        let ct = ct.clone();
        handle_shutdown_signal(ct)
    });

    let cli = Cli::parse();

    match cli.command {
        Command::Broker(args) => args.run(ct).await?,
    };

    Ok(())
}

async fn handle_shutdown_signal(ct: CancellationToken) {
    let mut sigterm = tokio::signal::unix::signal(SignalKind::terminate())
        .expect("failed to create terminate signal handler");

    let sig_type = tokio::select! {
        _ = sigterm.recv() => {
            "sigterm"
        }
        _ = tokio::signal::ctrl_c() => {
            "ctrl-c"
        }
    };

    ct.cancel();

    info!(signal = sig_type, "signal received. shutting down.")
}
