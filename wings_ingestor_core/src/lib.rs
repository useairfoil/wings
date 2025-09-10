//! Wings ingestion loop
//!
//! This module implements the core ingestion loop.
//!
//! The ingestor is a background process that receives batch ingestion requests from clients,
//! batches them together, uploads them to the object store, and finally commits them to the
//! log metadata service.
//!
//! When the requests come in, they are accumulated together (by namespace, topic, and partition).
//! into a single Parquet file (we call this "page"). Pages from multiple topics and partitions
//! (but the same namespace) are joined together into a "folio" (a.k.a. a collection of pages)
//! before being uploaded to the object store.
//! The folio is then committed to the log metadata service.
//!
//! The main challenge of this code is tracking which request has generated the batch.
//!
//! We have the following write failure cases.
//!
//! Single batch write failure cases:
//!
//!  - validation failure, for example the schema doesn't match the topic's schema.
//!  - commit failure, for example the requested timestamp is not valid.
//!
//! In some cases, multiple batches fail due to a single error:
//!
//!  - object store upload failures
//!  - log metadata service failures
//!
//! To simplify error handling, we wrap batch-related data into the [`write::WithReplyChannel`] struct.
//! The module provides a [`write::ReplyWithWriteBatchError`] struct to simplify sending one or multiple
//! errors to one or more clients.
mod batcher;
mod client;
mod error;
mod ingestor;
mod uploader;
mod write;

#[cfg(test)]
pub mod test_utils;

pub use ingestor::{BatchIngestor, run_background_ingestor};

pub use self::client::BatchIngestorClient;
pub use self::error::{IngestorError, Result};
pub use self::write::{WriteBatchError, WriteBatchRequest};
