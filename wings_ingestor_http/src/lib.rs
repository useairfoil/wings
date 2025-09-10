//! HTTP ingestor server.
//!
//! This crate provides a server to ingest messages over HTTP.

pub mod error;
pub mod push;
pub mod types;

pub use error::{HttpIngestorError, Result};
pub use types::{Batch, PushRequest, PushResponse};

use axum::{Router, routing::post};
use wings_control_plane::cluster_metadata::cache::{NamespaceCache, TopicCache};
use wings_ingestor_core::BatchIngestorClient;

use crate::push::push_handler;

/// HTTP ingestor server that receives messages via HTTP POST requests.
pub struct HttpIngestor {
    state: HttpIngestorState,
}

#[derive(Clone)]
pub struct HttpIngestorState {
    topic_cache: TopicCache,
    namespace_cache: NamespaceCache,
    batch_ingestion: BatchIngestorClient,
}

impl HttpIngestor {
    /// Create a new HTTP ingestor with the specified listen address and caches.
    pub fn new(
        topic_cache: TopicCache,
        namespace_cache: NamespaceCache,
        batch_ingestion: BatchIngestorClient,
    ) -> Self {
        let state = HttpIngestorState {
            topic_cache,
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
