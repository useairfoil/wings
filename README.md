# Wings

Wings is a distributed event streaming platform built on top of Apache Datafusion and object storage.

The goal is to simplify the common use case of syncing streams to a data lake, while providing a streaming interface for applications.

## Roadmap

**MVP**

 - [x] Write and read data
 - [x] Built-in timestamp support
 - [ ] Arrow Flight (+ SQL) for reading data
 - [ ] Arrow Flight (+ SQL) for ingesting data
 - [ ] Compaction to Parquet files
 - [ ] Control plane persistence based on PostgreSQL
 - [ ] Schema management and evolution
 - [ ] Iceberg catalog support

**Version 1.0**

 - [ ] Data retention policies
 - [ ] Transactions with clearly defined semantics
 - [ ] Extensible authentication and authorization
 - [ ] Extensible secret management (for object store credentials)
 - [ ] ["Distributed mmap"](https://www.warpstream.com/blog/minimizing-s3-api-costs-with-distributed-mmap) based on [LiquidCache](https://github.com/XiangpengHao/liquid-cache)
 - [ ] Cloud-native control plane persistence (DynamoDB, Azure CosmosDB, Google Cloud Spanner)
 - [ ] Deterministic simulation testing
 - [ ] Read-only streams based on externally managed Iceberg catalogs

## License

Copyright 2025 GNC Labs Limited

Licensed under the Apache License, Version 2.0 (the "License"); you may not use
this file except in compliance with the License. You may obtain a copy of the
License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
