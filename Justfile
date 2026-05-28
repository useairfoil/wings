# Show available recipes.
default:
	@just --list

# Check every workspace crate.
check:
	cargo check --workspace

# Build every workspace crate.
build:
	cargo build --workspace

# Test every workspace crate.
test:
	cargo test --workspace

# Run Clippy for every workspace crate, target, and feature.
clippy:
		cargo clippy --workspace --all-targets --all-features

# Format the workspace using the nightly toolchain from the Nix shell.
format:
	nix develop .#nightly -c cargo fmt

alias fmt := format
