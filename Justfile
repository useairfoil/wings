# Show available recipes.
default:
	@just --list

# Check every workspace crate.
check:
	cargo check --workspace --all-targets --all-features

# Build every workspace crate.
build:
	cargo build --workspace --all-targets --all-features

# Test every workspace crate.
test:
	cargo test --workspace --all-targets --all-features

# Run Clippy for every workspace crate, target, and feature.
clippy:
	cargo clippy --workspace --all-targets --all-features

# Format the workspace using the nightly toolchain from the Nix shell.
format:
	nix develop .#nightly -c cargo fmt

dev:
    cargo run --bin wings -- dev

alias fmt := format
