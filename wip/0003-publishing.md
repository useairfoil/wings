# 0003: Publishing

**Status**: idea

**Authors**

 - Francesco Ceccon <https://github.com/franceccon>

## Overview

[a short summary about this RFC] [ delete all text in the square brackets and replace it with your own]

## Background and motivation

Wings is a real-time storage engine for Apache Iceberg with row deduplication.
Data flows from ingestion (WIP-0002) to Apache Iceberg (WIP-0004), going through a publishing stage (this WIP).

Publishing is the process of reducing a contiguous WAL prefix into a complete, immutable set of data and metadata objects for each partition.
In other words, publishing reads WAL files and applies the changes to produce a new revision of the partition's data. These changes are considered published only after the partition's manifest (WIP-0001) is updated.
Note that the partition's manifest is immutable, so with "updated" we mean a new manifest revision is created on object storage. See WIP-0001 for more details.

After publishing, data is not yet visible in Apache Iceberg consumers. Data becomes visible after the commit stage.

## Goals and non goals

### Goals

- Describe the publishing process from WAL files to a new partition state.
- Describe the shape of the partition state and how it tracks the data files described in WIP-0001.
- Describe how conflicts between publishers and compactors are resolved.

### Non goals

- Describe how publishing is scheduled. Here we assume the service knows it needs to publish data for a table's partition.
- Describe how table data is deduplicated.
- Describe sharding.
- Describe how the partition state is used to commit data to Apache Iceberg.
- Handle partition spec changes. Partition spec is considered immutable.

## Detailed Design

The publisher's job is to update a table's partition with new data.

 - Tables managed by Wings require a primary key and a version column.
 - The version column is used to decide which value to keep when deduplicating data.
 - Partial updates are supported. In this case, only non-null values are updated.
 - Row deletions are not supported. Users should rely on soft deletes instead.

### Data model

[how data is modeled and how it flows through the system]

### Error boundaries

[what and where errors can occur, how they are (or not are) handled]

## Implementation strategy

[how you plan to implement this work]

## Alternatives considered

[alternative designs considered and why they were not used]

## Conclusion and next step

[if any additional work is required, write it here]

## History

| Date       | Changes          |
|------------|------------------|
| 2026-08-19 | Initial revision |
