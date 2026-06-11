use std::{net::IpAddr, sync::Arc};

use clap::Args;
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Server, server::TcpIncoming};
use tracing::info;
use wings_common::clock::SystemClock;
use wings_queue::{CompressionType, Header, QueueBrokerService, pb};

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
    #[error("object store error: {0}")]
    ObjectStore(#[from] object_store::Error),
}

impl BrokerArgs {
    pub async fn run(
        self,
        clock: Arc<dyn SystemClock>,
        ct: CancellationToken,
    ) -> Result<(), Error> {
        let object_store = self.object_store.create_object_store()?;

        let listener = self.server.bind_listener().await?;
        let local_addr = listener.local_addr()?;

        let port = local_addr.port();

        let IpAddr::V4(local_addr) = local_addr.ip() else {
            return Err(Error::InvalidIpAddress);
        };

        let header = Header {
            address: local_addr,
            compression: CompressionType::Zstd,
            port,
            epoch: 0,
        };
        let broker = QueueBrokerService::init(clock, object_store, header)
            .await
            .unwrap();

        info!(address = %local_addr, port, "server listening");

        let (health_reporter, health_service) = tonic_health::server::health_reporter();

        let broker = broker.into_service();
        let reflection = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(pb::queue_file_descriptor_set())
            .register_encoded_file_descriptor_set(tonic_health::pb::FILE_DESCRIPTOR_SET)
            .build_v1()?;

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
