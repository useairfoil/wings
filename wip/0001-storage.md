# 0001: Storage Layout

**Status**: idea

**Authors**

 - Francesco Ceccon <https://github.com/franceccon>

## Overview

This WIP describes the storage layout used by Wings. We describe how a table's data is organized and what metadata we need to manage it.

## Background and motivation

Wings is an Apache Iceberg storage engine for real-time data. The goal is to handle writes and row deduplication efficiently, with time-to-iceberg latency measured in seconds, not minutes.

## Goals and non goals

### Goals

- Provide the object store layout for data and metadata files.
- Describe how data and metadata are encoded.
- Describe which data files are shared with Apache Iceberg.
- Provide extension points for future work on sharding.

### Non goals

- Describe how the data is computed.
- Correctness and durability guarantees are not explored.
- Describe how data is committed to Apache Iceberg.
- Describe how schema and partition changes are handled.
- Describe sharding in detail.
- Garbage Collection will be described in a separate WIP.

## Detailed Design & Data Model

In this section we describe the data model for a single Apache Iceberg table. The storage layout is inspired by the [Apache Hudi project](https://hudi.apache.org/docs/storage_layouts/):

 - All tables are merge-on-read.
 - All tables must have a primary key (possibly composite) and a version column.
   + The version column is used to merge rows for the same primary key. The row is always updated with the latest version.
 - Data files are stored under the table's base location. The location comes from the Iceberg's table metadata.
 - Metadata files are stored in a separate object store (the metadata store), shared among all catalogs tracked by Wings.
 - Wings follows any partitioning scheme defined by the Iceberg table. Partitions data is stored separately, under different prefixes.
 - Partition data is split into _shards_ (equivalent to file groups in Hudi).
 - Shard operations bump the partition's manifest revision (similar but not fully equivalent to file slices in Hudi).
 - All data files referenced by an Iceberg snapshot must be available.


As an example, the metadata store for a table with two partitions might look like this (only the files described in this WIP are shown):

[TODO: update with definitive file extension once format is finalized]

```txt
/{catalog_id}/tables/{table_uuid}/
└── partitions
    └── 0
        ├── m-aaaa
        │   └── manifest
        │       ├── 00000000000000000001.bin
        │       ├── 00000000000000000002.bin
        │       ├── 00000000000000000003.bin
        │       └── ...
        └── m-bbbb
            └── manifest
                ├── 00000000000000000001.bin
                ├── 00000000000000000002.bin
                ├── 00000000000000000003.bin
                └── ...
```

Note that Apache Iceberg doesn't require a specific naming convention for data files, all partition information is stored in Iceberg manifests.

### Partitioning

Wings generates the partition manifest lazily, the first time data is written to a partition.

For each partition, Wings stores the following metadata. Additional metadata used for sharding is also stored and it's described in the corresponding section.

| Field   | Type     | Description                             |
|---------|----------|-----------------------------------------|
| Spec ID | int      | The partition spec ID.                  |
| Fields  | object[] | The partition fields.                   |
| Values  | object[] | The partition values.                   |
| Digest  | string   | The hash of the typed partition values. |

Note that the digest is computed by hashing the typed partition values _after they have been transformed_. For example, a table with the following partition spec:

```json
{
  "spec-id": 0,
  "fields": [
    {
      "source-id": 2,
      "field-id": 1000,
      "name": "event_day",
      "transform": "day"
    },
    {
      "source-id": 1,
      "field-id": 1001,
      "name": "id_bucket",
      "transform": "bucket[32]"
    },
    {
      "source-id": 3,
      "field-id": 1002,
      "name": "region",
      "transform": "identity"
    }
  ]
}
```

Its typed partition value is the following:

```ts
{
  1000: date("2026-08-18"),
  1001: int(7),
  1002: string("eu-west")
}
```

And the digest is computed by hashing the spec ID (`0`) and the typed partition values using a deterministic hashing algorithm like [xxhash](https://github.com/cyan4973/xxhash).
The details of the hashing algorithm are implementation-specific.

The partition's manifest is stored under the `partitions/{spec-id}/m-{digest}/manifest` prefix (`partitions/{spec-id}/u/manifest` for unpartitioned tables).
This directory contains a list of immutable files, one for each manifest revision.

[TODO: decide the manifest encoding and provide the schema]
[TODO: describe the immutable manifest protocol. Describe how to append manifests and tail changes at minimum.]
[TODO: show a full manifest content.]

### Base File, Data Files, and Deletion Vectors

Base files store the most recent snapshot of the partition's data in the Parquet format.
Rows in the base file are sorted and deduplicated by the primary key.
As we will see later, all files described in this section belong to a partition's shard. For the remainder of this section, we consider the trivial single-shard case.

The files described in this section (with the exception of tombstones) are published to the table's store, under the table's location prefix.
These files are shared with Apache Iceberg and are referenced by Iceberg manifests. This WIP does not describe how the Iceberg manifests are published or updated.

Rewriting the base file every time a row is updated is not efficient. Instead, Wings uses additional data files and deletion vectors to track changes.

 - On each write, Wings appends a new data file with the updated rows, and a Puffin file with the deletion vectors.
 - The Puffin file contains zero or one deletion vectors per data file, including the base file.
 - There is always only one (or zero) Puffin file.
   + Each shard revision references at most one Puffin file. Iceberg permits at most one deletion vector for each data file, Wings additionally stores all deletion vectors for a shard revision in one Puffin container to simplify publishing. A revision without deletion vectors has no Puffin file.
   + Deletion vectors are used to implement "delete" row operations.
   + To prevent resurrecting deleted rows, Wings stores their primary key and version in a tombstone file. Details about the tombstone file format are explained in the publishing WIP
 - Deletion vectors track rows that have been deleted or updated (in a more recent data file).
 - Data is periodically compacted into new base files. Old files are not deleted immediately because they are needed by old Iceberg snapshots.

For example, let's consider the following base file with primary key `id` and version column `ts`.

```
┌────────────────────────┐
│       Base File        │
├────┬────────┬─────┬────┤
│ id │ name   │ age │ ts │
├────┼────────┼─────┼────┤
│ 1  │ Alice  │ 34  │ 1  │
│ 2  │ Bob    │ 28  │ 1  │
│ 3  │ Carol  │ 79  │ 1  │
└────┴────────┴─────┴────┘
```

The ingester writes new values for rows with `id=2` and `id=4`, both with `ts=2`. This generates a new data file and a Puffin file with deletion vectors for the base file.
The deletion vectors are needed to "mask out" the row with `id=2` from the base file.
Note that deletion vectors contain the set of _rows_ deleted in a file, in this example row at index `1` (the row with `id=2`).

```
                              ┌────────────────────────┐
                              │      Data File 1       │
┌────────────────────────┐    ├────┬────────┬─────┬────┤
│       Base File        │    │ id │ name   │ age │ ts │
├────┬────────┬─────┬────┤    ├────┼────────┼─────┼────┤
│ id │ name   │ age │ ts │    │ 2  │ Bob    │ 29  │ 2  │
├────┼────────┼─────┼────┤    │ 4  │ Dave   │ 42  │ 2  │
│ 1  │ Alice  │ 34  │ 1  │    └────┴────────┴─────┴────┘
│ 2  │ Bob    │ 28  │ 1  │    ┌────────────────────────┐
│ 3  │ Carol  │ 79  │ 1  │    │    Deletion Vectors    │
└────┴────────┴─────┴────┘    ├───────────┬────────────┤
                              │ Base File │ {1}        │
                              └───────────┴────────────┘
```

Finally, the ingester updates rows with `id=2` again. In this case, the Puffin file contains the deletion vectors for both the base file and the previous data file.

```
                              ┌────────────────────────┐    ┌────────────────────────┐
                              │      Data File 1       │    │      Data File 2       │
┌────────────────────────┐    ├────┬────────┬─────┬────┤    ├────┬────────┬─────┬────┤
│       Base File        │    │ id │ name   │ age │ ts │    │ id │ name   │ age │ ts │
├────┬────────┬─────┬────┤    ├────┼────────┼─────┼────┤    ├────┼────────┼─────┼────┤
│ id │ name   │ age │ ts │    │ 2  │ Bob    │ 29  │ 2  │    │ 2  │ Bob    │ 30  │ 3  │
├────┼────────┼─────┼────┤    │ 4  │ Dave   │ 42  │ 2  │    └────┴────────┴─────┴────┘
│ 1  │ Alice  │ 34  │ 1  │    └────┴────────┴─────┴────┘    ┌────────────────────────┐
│ 2  │ Bob    │ 28  │ 1  │                                  │    Deletion Vectors    │
│ 3  │ Carol  │ 79  │ 1  │                                  ├───────────┬────────────┤
└────┴────────┴─────┴────┘                                  │ Base File │ {1}        │
                                                            │ D. F. 1   │ {0}        │
                                                            └───────────┴────────────┘
```

### Sharding

So far, we described how data for a single partition is stored. We always assumed that the base file is reasonably sized and can be compacted quickly and efficiently.
This assumption is not true for large partitions, where the base file may contain terabytes of data.

For this reason, Wings shards large partitions into smaller chunks, each using the same data layout described in the previous section.

 - Sharding is performed on primary key values.
   + For example, we may have a fixed number of shards (e.g. 2) and each shard is assigned ranges of hash values or modulo buckets.
 - In practice, Wings will adopt a dynamic sharding strategy that adjusts the number of shards based on the size of the partition.
 - This document assumes a single shard per partition.
 - Sharding is a mechanism to reduce data transfers, not to reduce contention on the manifest's partition.
   + Writers (publishers and compactors) must be able to recover from a failed manifest write without recomputing data, if the shard has not changed since the writer operation started.
   + Every time a shard is updated, its revision is bumped.

Sharding metadata is stored in the partition's manifest.

| Field        | Type     | Description                         |
|--------------|----------|-------------------------------------|
| Shard Type   | string   | The type of sharding strategy used. |
| Shard State  | object[] | The shard state, including params.  |

The shard state must specify at least the shard id and revision, the base and Puffin file locations, and a list of additional data files.

To continue the example above, we may assign all even `id` to shard `0` and all odd `id` to shard `1`.

## Alternatives considered

**Single Mutable Manifest Object**

This alternative uses a single manifest per partition. This makes discovering the latest partition's state easier, but it's harder to implement multiversion concurrency control (MVCC).

**Copy-on-Write Tables**

This alternative uses copy-on-write to publish new data. File and Iceberg manifest management becomes easier, but write amplification is higher. Not ideal for streaming use cases.

## Conclusion and next step

This WIP describes how data is organized and stored in Wings. The following WIPs are needed before Wings can be fully implemented:

 - (Required) Ingestion: describe how clients push data into Wings and how it becomes durable.
 - (Required) Publishing: describe how to compute the data and deletion vectors.
 - (Required) Iceberg Commit: describe how to commit changes to Apache Iceberg.
 - (Required) Reconciliation Loop: describe how to schedule and execute the steps required to publish and commit data.
 - (Optional) Compaction: describe how to optimize the storage layout.
 - (Optional) Garbage Collection: describe how to delete unused data and metadata files.
 - (Optional) Sharding: describe how to split large partitions into smaller shards.

## History

| Date       | Changes          |
|------------|------------------|
| 2026-08-18 | Initial revision |
