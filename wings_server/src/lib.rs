//! HTTP server for Wings catalog management.

mod catalog;
mod iceberg;

use axum::{
    Router,
    http::StatusCode,
    routing::{get, post},
};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use wings_meta_store::catalog::CatalogStore;

/// Builds the HTTP API. Catalog configurations are persisted in the secret store;
/// there is deliberately no catalog listing endpoint.
pub fn router(catalog_store: CatalogStore) -> Router {
    Router::new()
        .route("/health", get(|| async { StatusCode::OK }))
        .route("/catalogs", post(catalog::create))
        .route("/catalogs/{id}", get(catalog::get).delete(catalog::delete))
        .nest("/catalogs/{id}", iceberg::router())
        .with_state(catalog_store)
}

/// Runs the HTTP server until cancellation, draining in-flight requests on shutdown.
pub async fn run_http_server(
    listener: TcpListener,
    catalog_store: CatalogStore,
    ct: CancellationToken,
) -> std::io::Result<()> {
    tracing::info!(address = %listener.local_addr()?, "http server listening");
    axum::serve(listener, router(catalog_store))
        .with_graceful_shutdown(ct.cancelled_owned())
        .await
}
