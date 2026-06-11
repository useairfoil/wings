# Wings

Wings is the streaming storage engine for Apache Iceberg. It's designed to ingest high-volume data concurrently while keeping Iceberg's metadata up-to-date.

In a medalion architecture data lakehouse, Wings is used for bronze (raw) and silver (cleaned, deduplicated) data.

Wings supports two modes, configurable by table: 

 - append only: every write adds data to the table.
 - entities: row values are merged so that only the latest value is stored, with no duplicates.

In both modes, data is ingested through the Wings Arrow Flight service and committed to the write-ahead log (WAL) on object storage, after this data is considered durable. Concurrent writes are batched together and flushed every second.

At the moment, Wings _is not_ a general purpose storage engine. Data MUST go through the Wings ingestion service so that it can be validated and added to the table.

### Features

 - Multi tenant: manage multiple Iceberg catalogs with a single Wings cluster. Data from different tenants is never mixed together.
 - Separate cluster and catalog storage: operational data is stored separately from the catalogs data.
 - Operationally simple: there is no stateful dependency other than object storage. All components are stateless and can serve any tenant.
 - Apache Iceberg v3: support positional deletes, semi structured data, and row lineage.
 - Rust + Arrow + DataFusion (RAD) stack: Wings builds on a solid foundation.
 - Open Source: Apache 2.0 license. You can run it locally or in your cloud without any vendor lock-in.

### Roadmap

 - [ ] Entity tables with configurable primary key and version columns.
 - [ ] Append only tables.
 - [ ] Clustering and sorting by non-PK column.
 - [ ] Apache Iceberg v4.

## Architecture

A Wings cluster is made of the following components:

 - load balancers: reduce WAL write contention by always forwarding write requests for the same table/partition to the same ingestor.
 - ingestors: receive data from clients, validate it, and then flush it to the WAL.
 - task broker: tracks and assigns tasks such as compaction, updates, and pruning.
 - workers: perform the heavy-weight, async tasks.

The load balancer is the gateway between clients and the Wings cluster.
It exposes an Arrow Flight service and, after performing authentication and authorization, forwards the request to one ingestor.
This service tries to always forward requests for the same table and partition to the same ingestor to reduce contention when writing the WAL.

The ingestor is responsible for accumulating all writes (inserts, updates, and deletes) within a time window (usually, 1 second) in a single batch and then flush it to the WAL. The server responds to the client only after data has been durably written to storage.
The WAL is a collection of sequentially numbered files. Wings supports multiple concurrent writers by using the compare and swap (CAS) operation supported by modern object stores. 
At this stage data is durable but it's not yet visible in Iceberg. For this, we need to execute a background that materializes data from the WAL into the data lakehouse. The last step by the ingestor is to push a materialization task to the task broker.

The task broker is responsible for scheduling and assigning async jobs. It's based [on the turbopuffer's queue.json design](https://turbopuffer.com/blog/object-storage-queue). There is only one active broker at the time, this is enforced by fencing all writes. The queue file is also used for discovery: once the broker starts, it writes its IP address and port to this file. The task broker is stateless and does not perform any task, relying on a pool of workers.

Workers are the heavy-weight components in Wings. They perform all table maintenance tasks and are what moves tables state forward. They perform the following tasks:

 - materialize writes and deletes from the WAL to the Iceberg table.
 - compact data files if needed.
 - optimize the Iceberg manifests.
 - prune unused data and metadata files.
 - split and merge table slices.

All Wings data manipulation routines are implemented on top of DataFusion.

One last note: the catalog configuration is NOT on object storage because that's not secure.
For this (and only for this) data, Wings relies on a external secret stores such as [AWS Secrets Manager](https://aws.amazon.com/secrets-manager/), [Azure Key Vault](https://azure.microsoft.com/en-us/products/key-vault), or [Google Could Secret Manager](https://cloud.google.com/security/products/secret-manager).
In this case, Wings stores only a pointer to the remote secret.

```txt
           │                                                                                            
           │                                                                                            
           ▼                                                                                            
   ┌───────────────┐                                                                                    
╔══╡ load balancer ╞═════════════════════════════════════════════════════════════════════════════════╗  
║  └───────┬───────┘                                                                                 ║  
║          ├───────────────────┐                                                                     ║░░
║          ▼                   ▼                                                                     ║░░
║  ╔═ wings ingestors ═════════════════╗   ╔═════════════╗   ╔═ wings workers ═══════════════════╗   ║░░
║  ║ ┌────────────┐     ┌────────────┐ ║   ║             ║   ║ ┌────────────┐     ┌────────────┐ ║   ║░░
║  ║ │            │     │            │ ║   ║    task     ║   ║ │            │     │            │ ║   ║░░
║  ║ │   ing─0    │ ... │   ing─N    │ ║   ║   broker    ║   ║ │   wrk─0    │ ... │   wrk─M    │ ║   ║░░
║  ║ │            │     │            │ ║   ║             ║   ║ │            │     │            │ ║   ║░░
║  ║ └────────────┘     └────────────┘ ║   ║             ║   ║ └────────────┘     └────────────┘ ║   ║░░
║  ╚══════╤════════════════════════════╝   ╚═╤═══════════╝   ╚═══════════════════════════╤═══════╝   ║░░
║         │                                  │                       ▲                   │           ║░░
║         │                          ┌───────┘  ┌────────────────────┘                   │           ║░░
╚═════════╪══════════════════════════╪══════════╪════════════════════════════════════════╪═══════════╝░░
  ░░░░░░░░│░░░░░░░░░░░░░░░░░░░░░░░░░░│░░░░░░░░░░│░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░│░░░░░░░░░░░░░░
          │                          │          │                                        │              
          ▼                          ▼          ▼                                        ▼              
╔═ cluster object storage ═══════════════════════╗ ╔═ catalogs object storage ══════════════════════╗  
║  ┌ queue ───────────────────────────────────┐  ║░║                                                ║░░
║  │███████████████████                       │  ║░║ ┌────────────────────┐  ┌────────────────────┐ ║░░
║  └──────────────────────────────────────────┘  ║░║ │                    │  │                    │ ║░░
║                                                ║░║ │   manifest.avro    │  │   metadata.json    │ ║░░
║  ┌ {catalog}/{table} ───────────────────────┐  ║░║ │                    │  │                    │ ║░░
║  │ ┌ wal ──────────────┐┌ slices─meta ─────┐│  ║░║ └────────────────────┘  └────────────────────┘ ║░░
║  │ │                   ││                  ││  ║░║ ┌────────────────────┐  ┌────────────────────┐ ║░░
║  │ │ █████████░░░░░░░░ ││ ███ ███ ███ ███  ││  ║░║ │                    │  │                    │ ║░░
║  │ │                   ││                  ││  ║░║ │  file─00.parquet   │  │  file─01.parquet   │ ║░░
║  │ └───────────────────┘└──────────────────┘│  ║░║ │                    │  │                    │ ║░░
║  └──────────────────────────────────────────┘  ║░║ └────────────────────┘  └────────────────────┘ ║░░
╚════════════════════════════════════════════════╝░╚════════════════════════════════════════════════╝░░
  ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░ ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░
```

### Table Storage

Table storage is inspired by Apache Paimon and Apache Hudi. Tables are divided into _table slices_ that each contain a subset of the primary key space.
As the table grows, large slices are split into smaller ones.

This design reduces write amplification and increases concurrency when updating and deduplicating data since different workers can work on different slices of the same table.

The following diagrams shows the storage for an `orders` table partitioned by shop. Notice that partitions' table slices are independent of each other: in this case the table space for `shop-a` is divided in 4 slices, while `shop-b` in just 3. This is key to optimal file size.

```txt
╔═ shopify.orders ═════════════════════════════════════════════════════════════╗  
║                                                                              ║░░
║  ┏━ store=shop─a ━━━━━━━━━━━━━━━━━━━┓  ┏━ store=shop─b ━━━━━━━━━━━━━━━━━━━┓  ║░░
║  ┃ ┌──────────────┐┌──────────────┐ ┃  ┃ ┌──────────────┐┌──────────────┐ ┃  ║░░
║  ┃ │              ││              │ ┃  ┃ │              ││              │ ┃  ║░░
║  ┃ │  Key 00..20  ││  Key 20..40  │ ┃  ┃ │  Key 00..10  ││  Key 10..35  │ ┃  ║░░
║  ┃ │    LSM SR    ││    LSM SR    │ ┃  ┃ │    LSM SR    ││    LSM SR    │ ┃  ║░░
║  ┃ │              ││              │ ┃  ┃ │              ││              │ ┃  ║░░
║  ┃ │              ││              │ ┃  ┃ │              ││              │ ┃  ║░░
║  ┃ └──────────────┘└──────────────┘ ┃  ┃ └──────────────┘└──────────────┘ ┃  ║░░
║  ┃ ┌──────────────┐┌──────────────┐ ┃  ┃ ┌──────────────┐                 ┃  ║░░
║  ┃ │              ││              │ ┃  ┃ │              │                 ┃  ║░░
║  ┃ │  Key 40..60  ││  Key 60..99  │ ┃  ┃ │  Key 35..99  │                 ┃  ║░░
║  ┃ │    LSM SR    ││    LSM SR    │ ┃  ┃ │    LSM SR    │                 ┃  ║░░
║  ┃ │              ││              │ ┃  ┃ │              │                 ┃  ║░░
║  ┃ │              ││              │ ┃  ┃ │              │                 ┃  ║░░
║  ┃ └──────────────┘└──────────────┘ ┃  ┃ └──────────────┘                 ┃  ║░░
║  ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛  ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛  ║░░
║                                                                              ║░░
╚══════════════════════════════════════════════════════════════════════════════╝░░
  ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░
```

## Getting Started

TODO

## Development

TODO

## License

Copyright 2026 GNC Labs Limited

Licensed under the Apache License, Version 2.0 (the "License"); you may not use
this file except in compliance with the License. You may obtain a copy of the
License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
