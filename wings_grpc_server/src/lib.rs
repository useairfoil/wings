pub mod error;
mod service;

use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Server, server::TcpIncoming};
use tracing::info;
use wings_grpc_common::pb;
use wings_meta_store::catalog::CatalogStore;

pub use self::{
    error::{Error, Result},
    service::CatalogService,
};

/// Runs a gRPC server serving the catalog service until the given
/// cancellation token is cancelled.
pub async fn run_grpc_server(
    listener: TcpListener,
    catalog_store: CatalogStore,
    ct: CancellationToken,
) -> Result<()> {
    let catalog_service = CatalogService::new(catalog_store);

    let (health_reporter, health_service) = tonic_health::server::health_reporter();

    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(wings_grpc_common::file_descriptor_set())
        .register_encoded_file_descriptor_set(tonic_health::pb::FILE_DESCRIPTOR_SET)
        .build_v1()?;

    let address = listener.local_addr()?;
    info!(address = %address, "grpc server listening");

    health_reporter
        .set_serving::<pb::catalog_service_server::CatalogServiceServer<CatalogService>>()
        .await;

    Server::builder()
        .add_service(health_service)
        .add_service(reflection)
        .add_service(catalog_service.into_service())
        .serve_with_incoming_shutdown(TcpIncoming::from(listener), ct.cancelled())
        .await?;

    Ok(())
}
