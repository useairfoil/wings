use clap::{CommandFactory, Parser};

#[derive(Parser)]
#[command(name = "wings-stress")]
#[command(about = "Wings stress testing CLI")]
#[command(version)]
struct Cli {}

fn main() -> std::io::Result<()> {
    Cli::parse();
    Cli::command().print_help()
}
