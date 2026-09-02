# Wings

Wings is the streaming storage engine for Apache Iceberg. It's designed to ingest high-volume data concurrently while keeping Iceberg's metadata up-to-date.

In a medallion architecture data lakehouse, Wings is used for bronze (raw) and silver (cleaned, deduplicated) data.

Wings supports two modes, configurable by table: 

 - entities: row values are merged so that only the latest value is stored, with no duplicates. In this mode, partial updates are supported.
 - append only: every write adds data to the table.

In both modes, data is ingested through the Wings Arrow Flight service and committed to the write-ahead log (WAL) on object storage, after this data is considered durable. Concurrent writes are batched together and flushed every second.

At the moment, Wings _is not_ a general purpose storage engine. Data MUST go through the Wings ingestion service so that it can be validated and added to the table.

### Features

 - Multi tenant: manage multiple Iceberg catalogs with a single Wings cluster. Data from different tenants is never mixed together.
 - Separate cluster and catalog storage: operational data is stored separately from the catalogs data.
 - Operationally simple: there is no stateful dependency other than object storage. All components are stateless and can serve any tenant.
 - Apache Iceberg v3: support positional deletes with deletion vectors, semi structured data, and row lineage.
 - Rust + Arrow + DataFusion (RAD) stack: Wings builds on a solid foundation.
 - Open Source: Apache 2.0 license. You can run it locally or in your cloud without any vendor lock-in.

### Roadmap

 - [ ] Entity tables with configurable primary key and version columns.
 - [ ] Append only tables.
 - [ ] Clustering and sorting by non-PK column.
 - [ ] Apache Iceberg v4.

## Getting Started

TODO

```txt
wings
├── wings: the main binary.
├── wings_common: common utilities shared across crates, e.g. DST.
├── wings_grpc_server: gRPC server, including the Arrow Flight server for ingestion.
├── wings_meta_store: crate to interact with the metadata.
├── wings_observability: utilities to setup observability.
├── wings_secret_store: abstraction over secret stores (e.g. AWS Secrets Manager, HashiCorp Vault).
└── wings_stress: a stress testing tool.
```

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
