//! HTTP ingestor server.
//!
//! This crate provides a server to ingest messages over HTTP.

pub mod error;
pub mod push;
pub mod types;

use axum::{Router, routing::post};
pub use error::{HttpIngestorError, Result};
pub use types::{Batch, PushRequest, PushResponse};
use wings_control_plane_core::cluster_metadata::cache::{NamespaceCache, TableCache};
use wings_ingestor_core::IngestorClient;

use crate::push::push_handler;

/// HTTP ingestor server that receives messages via HTTP POST requests.
pub struct HttpIngestor {
    state: HttpIngestorState,
}

#[derive(Clone)]
pub struct HttpIngestorState {
    table_cache: TableCache,
    namespace_cache: NamespaceCache,
    batch_ingestion: IngestorClient,
}

impl HttpIngestor {
    /// Create a new HTTP ingestor with the specified listen address and caches.
    pub fn new(
        table_cache: TableCache,
        namespace_cache: NamespaceCache,
        batch_ingestion: IngestorClient,
    ) -> Self {
        let state = HttpIngestorState {
            table_cache,
            namespace_cache,
            batch_ingestion,
        };

        Self { state }
    }

    pub fn into_router(self) -> Router {
        Router::new()
            .route("/v1/push", post(push_handler))
            .with_state(self.state)
    }
}
