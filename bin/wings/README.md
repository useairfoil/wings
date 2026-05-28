# Wings CLI

The Wings CLI provides a command-line interface for running and managing Wings.

## Installation

From the project root:

```bash
cargo build --release -p wings
```

The binary will be available at `target/release/wings`.

## Usage

### Development Mode

Run the development-mode stub:

```bash
wings dev
```

## Development

This CLI follows the Wings project style guide and uses:

- `clap` with derive API for command-line parsing
- `tokio` for async runtime
- `snafu` for error handling
- `tokio-util::sync::CancellationToken` for graceful shutdown

The CLI is structured with:
- `main.rs`: Entry point and command routing
- `error.rs`: Error types and result type aliases
- `dev.rs`: Development mode command
