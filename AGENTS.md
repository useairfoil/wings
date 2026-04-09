# Airfoil Wings: commands and style guide

## Project overview

Airfoil Wings (from now, Wings) is a data ingestion service and framework to ingest data from any source directly into the data lakehouse.
Once the data is in the data lakehouse, it can be streamed _out_ of it too.
It's designed to be lightweight, efficient, and easy to use.

**Open standards**: Wings uses Parquet files for long-term storage. Topics are compacted and partitioned so that any system that can read Parquet and Iceberg can consume the data.

**Multi tenant**: Wings supports multi-tenancy, allowing multiple teams to share the same instance of Wings.

**Built-in schema registry**: Data ingestion forces data to conform to the topic's schema. No misshaped data can enter the system.

**Partitioning**: Topics are partitioned by key, with one partition for each unique value. Messages within the same partition are ordered, while no ordering is guaranteed between partitions.

**Decoupled data from metadata**: Wings separates data from metadata. The metadata store is the only stateful component of Wings. It stores metadata information about the cluster (tenants, namespaces, topics, etc.) and the log (offsets).

**Decouple storage from compute**: Data is stored on object storage. The data layer (ingestor and query server) are stateless components that can be scaled horizontally.

## The stack

Wings builds on top of existing technologies:

- Rust
- Apache DataFusion
- Apache Arrow
- Apache Parquet
- Apache Iceberg

When possible, Wings uses open standards to ensure compatibility with other systems.

## Components

At a high-level, Wings consists of the following components:

**Ingestor**: Receives messages from producers and writes them to object storage. To limit S3 PUT requests, the ingestor batches messages and writes them in bulk. Roughly speaking, this is done every 250ms or 8MiB (configurable, use this to get a ballpark idea).

**Query**: Receives queries from consumers and serves them data. Queries are powered by DataFusion.

**Metadata Store**: The metadata store is a stateful component that stores information about tenants, namespaces, topics, and partitions. This component is also used to assign offsets (sequence numbers) to messages.

**Compaction**: This component is responsible for compacting data in Parquet files. It starts by fetching "live" data from the object store and generate the first generation of compacted Parquet files. Files are later compacted further into larger and larger files, until they reach a certain target file size (around 250MiB).

## Data

As we mentioned before, long-term storage is handled by Parquet files uploaded on object storage, with one Parquet file per partition.

Storing "live" data in Parquet would be too inefficient (all data in a single Parquet file must have the same schema). Wings stores live data from multiple topics and partitions in the same binary file to reduce S3 operations.

Partition columns are not physically stored in the Parquet files, but they are encoded in the file path. As such, topics can only be partitioned by a single column.

## Vocabulary

**Batch**: a batch is a group of rows (same topic and partition) pushed at the same time.

**Folio**: a folio is a collection of rows by namespace. Data in a folio is grouped and sorted by topic and partition and contains data from multiple batches.

**Segment**: a segment file is a Parquet file containing compacted data from a single topic and partition.

**Segment's generation**: a segment's generation is the number of times it has been compacted.

## Commands

- `cargo check`: Check the project for errors.
- `cargo build`: Build the project.
- `cargo test`: Run the unit tests.
- `nix develop .#nightly -c cargo fmt`: Run cargo fmt. Nightly Rust is required.
- `nix develop .#nightly -c code-coverage`: Run code coverage. Only works if the user has nix installed.

Always use the `-p <crate-name>` flag to restrict commands to the crate you're currently working on.

## Style guide

We follow the standard Rust style guide.

### Error handling

We use the `snafu` crate for error handling. We create a custom error type per module (usually, in the `error.rs` file). Both the error and result types are exported by the module.

The project defines a `StatusCode` used to signal the error type to the user with a unique numerical code. 
Errors at the boundary between the control plane and the broker should implement the `ErrorExt` trait defined in `wings_observability/src/error.rs` to allow error propagation over gRPC.

### Task cancellation

All long-running async operations must be cancellable. We use `tokio_util::sync::CancellationToken` to signal cancellation to the task.

There is one root cancellation token, usually created by the main entry point. We create child tokens using either of the following methods:

- `child_token`: this creates a new child token that is cancelled when the current token is cancelled. Unlike a cloned token, cancelling a child token does not cancel the parent token.
- `clone`: this creates a new token that is cancelled when the current token is cancelled. Unlike a child token, cancelling a cloned token cancels the parent token.

Cancellation is signaled by calling `cancel` on the cancellation token. This is usually done by the main entry point when the user requests cancellation (e.g. by pressing ctrl-c).

If a service spawns a number of child tasks, it should also cancel them on exit. This is usually done by 1) creating a `child_token` for the current service, 2) creating a `drop_guard` for this new token, and 3) `clone`ing the token for each child task.

When cloning a cancellation token, we use a scoped shadowed variable to avoid naming the token `ct_clone`.

```rust
pub async fn do_something(ct: CancellationToken) {
    tokio::spawn({
        let ct = ct.clone();
        async move {
            do_something_else(ct).await;
        }
    })
}
```
