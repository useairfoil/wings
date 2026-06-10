use std::net::IpAddr;

use clap::Args;
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Server, server::TcpIncoming};
use tracing::info;
use wings_queue::{QueueBrokerService, pb};

use crate::{object_store::ObjectStoreArgs, server::ServerArgs};

#[derive(Debug, Args)]
pub struct BrokerArgs {
    #[command(flatten)]
    pub server: ServerArgs,
    #[command(flatten)]
    pub object_store: ObjectStoreArgs,
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid ip address: ipv4 expected")]
    InvalidIpAddress,
    #[error("server transport error: {0}")]
    ServerTransport(#[from] tonic::transport::Error),
    #[error("reflection server error: {0}")]
    ReflectionServer(#[from] tonic_reflection::server::Error),
}

impl BrokerArgs {
    pub async fn run(self, ct: CancellationToken) -> Result<(), Error> {
        let broker = QueueBrokerService::new();

        let listener = self.server.bind_listener().await?;
        let local_addr = listener.local_addr()?;

        let port = local_addr.port();

        let IpAddr::V4(local_addr) = local_addr.ip() else {
            return Err(Error::InvalidIpAddress);
        };

        info!(address = %local_addr, port, "server listening");

        let broker = broker.into_service();
        let reflection = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(pb::queue_file_descriptor_set())
            .build_v1()?;

        let (health_reporter, health_service) = tonic_health::server::health_reporter();

        health_reporter
            .set_serving::<pb::broker_server::BrokerServer<QueueBrokerService>>()
            .await;

        Server::builder()
            .add_service(health_service)
            .add_service(reflection)
            .add_service(broker)
            .serve_with_incoming_shutdown(TcpIncoming::from(listener), ct.cancelled())
            .await?;

        Ok(())
    }
}
