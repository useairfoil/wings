pub mod error;
pub mod fetch;
pub mod types;

use axum::{Router, routing::post};
use fetch::fetch_handler;
use wings_server_core::Fetcher;

pub struct HttpServer {
    fetcher: Fetcher,
}

impl HttpServer {
    pub fn new(fetcher: Fetcher) -> Self {
        Self { fetcher }
    }

    pub fn into_router(self) -> Router {
        Router::new()
            .route("/v1/fetch", post(fetch_handler))
            .with_state(self.fetcher)
    }
}
