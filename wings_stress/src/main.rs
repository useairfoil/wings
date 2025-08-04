use std::{error::Error, sync::Arc};

use clap::{Parser, Subcommand};
use tokio_util::sync::CancellationToken;
use wings::handle_shutdown_signal;
use wings_common::clock::DefaultSystemClock;
use wings_observability::init_observability;
use wings_stress::cmd::QueueArgs;

#[derive(Parser)]
#[command(name = "wings-stress")]
#[command(about = "Wings stress testing CLI")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Stress test the task queue.
    Queue(QueueArgs),
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let clock: Arc<_> = DefaultSystemClock::default().into();

    init_observability(
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
        Command::Queue(args) => args.run(clock, ct).await?,
    };

    Ok(())
}
