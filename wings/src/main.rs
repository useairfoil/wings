use std::{error::Error, sync::Arc};

use clap::{Parser, Subcommand};
use tokio_util::sync::CancellationToken;
use wings::{cmd::BrokerArgs, handle_shutdown_signal};
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
        clock.clone(),
    )?;

    let ct = CancellationToken::new();

    tokio::spawn({
        let ct = ct.clone();
        handle_shutdown_signal(ct)
    });

    let cli = Cli::parse();

    match cli.command {
        Command::Broker(args) => args.run(clock, ct).await?,
    };

    Ok(())
}
