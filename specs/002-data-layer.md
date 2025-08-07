# Data Layer: Query and Storage

**Revision**: 1
**Created**: <2025-08-07>
**Authors**: Francesco Ceccon
**Status**: Draft

## Overview

> Start with a brief summary of the project's purpose and scope.
> This should be one or two sentences to make the reader understand why this work is needed.

**Goals**

 - Provide a `query` module in `wings_server_core` to query data from the storage layer.
 - The query module implements all the required traits in Datafusion.

## Background and context

Datafusion provides a set of traits and conventions for building data processing pipelines. We are leveraging these features to:

 - Use well-known patterns that have been proved to work in production.
 - Simplify onboarding new contributors.
 - Integrate with the wider Datafusion ecosystem.

For now, we are only focusing on the read path.

## Design and architecture

> Provide a detailed description of the system's architecture.
> The goal of this section is to provide the AI all the necessary information to
> implement this work.
> It's unlikely you will provide _too much_ detail here.
> **Important**: make sure to keep this section up-to-date as you change the design
> during the implementation process.

**CatalogProvider**

The catalog provider is a per-namespace provider that exposes two schemas:

 - `public`: contains the public topics (tables).
 - `system`: contains the system tables, that is the metadata about the topics.

**SystemSchemaProvider**

The system schema provider contains tables that are helpful to understand the state of the system.

For now, we only include the `topics` table.

 - `topics`: contains information about the topics, such as their tenant, namespace, name, and partition column.

Later, we can add tables for partitions, file locations, iceberg catalog status, and more.

**PublicSchemaProvider**

The public schema provider exposes the topics in the namespace as tables. For each topic, it returns a `TopicTableProvider`.

**TopicTableProvider**

The topic table provider exposes the topic as a table. It provides methods to read the data from the topic.

 - The `schema` includes the partition column, if any.
 - The `schema` includes an `__offset__` column, which represents the offset of each row (record, message) in the topic.
 - If the topic is partitioned, the `scan` method returns an error if the `filter` condition does not include the partition column.

## Implementation plan

> This is a list of tasks that need to be completed to implement this work.
> Use checkboxes to track progress.

- [ ] Task 1
- [ ] Task 2
- [ ] Task 3

## Testing strategy

> Describe how to implement unit tests for the components described in the
> design and architecture section.
> Describe what type of integration tests to implement.
