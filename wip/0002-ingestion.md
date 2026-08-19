# 0002: Ingestion

**Status**: idea

**Authors**

 - Francesco Ceccon <https://github.com/franceccon>

## Overview

This WIP describes the Wings ingestion process in detail. We introduce the Wings WAL, an extension of the OSWALD design, and describe the append, tail, and garbage collection operations.

## Background and motivation

Wings is an Apache Iceberg storage engine for real-time data. The goal is to handle writes and row deduplication efficiently, with time-to-iceberg latency measured in seconds, not minutes.

Wings decouples ingestion from Iceberg commits, allowing end users to tune ingestion and Iceberg commit latency independently.

 - Ingested data is durable after it's written to the WAL on object storage.
   + Wings achieves sub-second ingestion latency.
 - Iceberg commit frequency is decided by business needs and is tuned per-table.
 - Tables must have a primary key and version column defined. These columns are used for deduplication and ordering.

## Goals and non goals

### Goals

- Describe the ingestion process and how it works.
- Describe the WAL and its operations.
- Describe the WAL file encoding.

### Non goals

 - Describe the data layout in detail, that is available in WIP-0001.

## Detailed Design & Data Model


The goal of ingestion is to strike a balance between write latency and file size. We adopt the well known pattern of buffering data in memory (configurable, up to 1 second or 8MB) before writing to the WAL.

We follow the [OSWALD design](https://nvartolomei.com/oswald/) to implement the WAL, with a few modifications because of partitioned data.

 - We call log sequence numbers (LSNs) "WAL File IDs", or "FIDs" for short.
 - The "Snapshot LSN" is not a single value in the manifest, but the minimum WAL FID of all partition manifests.
   + Listing all partitions is infeasible, the number of partitions is unbounded.
 - The table's metadata stores a "last seen FID" to help the initialization process find the most recent WAL file.
   + This FID is always greater than or equal to the GC watermark.
 - The Garbage Collector implementation must be partition-aware and is described later.
 - The table's metadata contains the GC watermark, and this watermark is used to detect writer-GC conflicts.

Note that our design supports multiple concurrent writers. In practice, this topology is sub-optimal and production environments should have a routing layer to coalesce table writes, for example using [rendezvous hashing](https://en.wikipedia.org/wiki/Rendezvous_hashing) on the table id to assign write requests to ingestion nodes.

### Data Validation

The first step of the ingestion process is to validate the incoming data against the table's schema.

 - Wings supports partial updates, where only the specified columns are updated.
 - The nullability column constraints are relaxed, all top-level fields become nullable.
 - The incoming data is validated against the relaxed schema. If validation fails, the write is rejected immediately.
 - The Parquet files in the WAL use the relaxed schema.

### Initialization

Write operations are stateless, there is no need to initialize complex state by replaying the WAL.

On initialization, clients read the table's metadata to find the GC watermark and last seen FID, then scan the WAL to find the WAL head.

 - Starting from the last known FID as the lower bound, the client issues object store `HEAD` requests for larger WAL FIDs.
 - Probe exponentially at offsets 1, 2, 4, 8, 16, ...
 - Once it finds a missing WAL file, perform a binary search in the range.

### Appending

Write operations require two object store operations: a `PUT` request to create the WAL file, and a `Get If-None-Match` request on the manifest to detect writer-GC conflicts.

 - Use optimistic concurrency control to create the WAL file using a `PUT If-None-Match` request.
   + On conflict, tail the WAL head to find the latest FID and try again.
 - Fetch the table's manifest with `GET If-None-Match` to detect writer-GC conflicts.
   + On precondition failure, download and inspect the manifest to determine whether the newly created WAL file is behind the garbage collector.
 - Periodically (for example, every 100 files or every minute), update the manifest with the latest WAL file FID.
   + Use a `PUT If-Match` request to update the manifest to detect and resolve conflicts.

### Tailing

Tailing is performed by optimistically calling `GET` on the WAL files.

 - The client detects it reached the head of the WAL when it receives a `Not Found` response.
 - The client must check the table's manifest to detect reader-GC conflicts.

### Garbage Collection

The original OSWALD garbage collector design requires a single "snapshot LSN" to exist. In Wings, this is not possible because we have multiple (unbounded) partitions that are published independently.

The Wings WAL garbage collector works by replaying the WAL files to detect which partitions have been affected and then compute the equivalent "snapshot LSN" from this information.

 - Replay a range of WAL files and compute the set of partitions touched by them.
 - For each partition, check their manifest to determine up to which WAL FID they have been published.
 - Compute the minimum WAL FID across all partitions. It's safe to delete WAL files up to this FID.
 - Update the table's manifest GC watermark with a CAS update.
 - Delete the now obsolete WAL files.

If the garbage collector crashes between updating the manifest and deleting WAL files, the WAL files will become orphaned and will not be cleaned up. In the future, we can add a background task to clean up orphaned WAL files.

### Delivery Semantics

Ingestion provides at-least-once delivery. A batch of updates may appear in the WAL more than once, for example when an object-store write succeeds but its response is lost or when an ingestor restarts before acknowledging the request.
Retries preserve each row's primary key, version, and value. Publishing deduplicates these rows so processing multiple copies produces the same logical table state.

### WAL File Format

[TODO: describe the WAL file binary format]

## Conclusion and next step

This WIP describes the ingestion process for Wings. After data is ingested, it needs to be published to object storage (WIP-0003) and then committed to Apache Iceberg (WIP-0004) before it becomes visible to readers.

## History

| Date       | Changes          |
|------------|------------------|
| 2026-08-19 | Initial revision |
