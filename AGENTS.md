# Agents

## Commands

If available, use `just`. If not, read the `justfile` to determine the equivalent command.
By default, commands run on all crates in the project. Pass the `-p <crate>` option to run on a specific crate.

The `format` command requires a nightly toolchain to run.

## Guidelines

 - Use `thiserror` for error management.
 - Ask the user before adding a new dependency.
 - If creating a `Result` alias, use the `type Result<T, E = Error> = std::result::Result<T, E>;` pattern.
