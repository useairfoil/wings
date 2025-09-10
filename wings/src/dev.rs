use std::{net::SocketAddr, sync::Arc};

use axum::Router;
use clap::Args;
use snafu::ResultExt;
use tokio_util::sync::CancellationToken;
use tracing::info;
use wings_control_plane::{
    cluster_metadata::{
        ClusterMetadata, InMemoryClusterMetadata,
        cache::{NamespaceCache, TopicCache},
        tonic::ClusterMetadataServer,
    },
    log_metadata::{InMemoryLogMetadata, tonic::LogMetadataServer},
    resources::{NamespaceName, NamespaceOptions, SecretName, TenantName},
};
use wings_ingestor_core::{BatchIngestor, BatchIngestorClient, run_background_ingestor};
use wings_ingestor_http::HttpIngestor;
use wings_object_store::TemporaryFileSystemFactory;

use crate::error::{
    InvalidServerUrlSnafu, IoSnafu, ObjectStoreSnafu, Result, TonicReflectionSnafu,
    TonicServerSnafu,
};

#[derive(Debug, Args)]
pub struct DevArgs {
    /// The address of the gRPC metadata server.
    #[arg(long, default_value = "127.0.0.1:7777")]
    metadata_address: String,
    /// The address of the HTTP ingestor server.
    #[arg(long, default_value = "127.0.0.1:7780")]
    http_address: String,
}

impl DevArgs {
    pub async fn run(self, ct: CancellationToken) -> Result<()> {
        let (admin, default_namespace) = new_dev_cluster_metadata_service().await;

        let metadata_address = self
            .metadata_address
            .parse::<SocketAddr>()
            .context(InvalidServerUrlSnafu {})?;

        let http_address = self
            .http_address
            .parse::<SocketAddr>()
            .context(InvalidServerUrlSnafu {})?;

        info!("Starting Wings in development mode");
        info!("Default namespace: {}", default_namespace);
        info!("gRPC server listening on {}", metadata_address);
        info!("HTTP ingestor listening on {}", http_address);

        let _ct_guard = ct.child_token().drop_guard();
        let object_store_factory =
            TemporaryFileSystemFactory::new().context(ObjectStoreSnafu {})?;

        let log_metadata = Arc::new(InMemoryLogMetadata::default());
        let object_store_factory = Arc::new(object_store_factory);

        info!(
            "Object store root path: {}",
            object_store_factory.root_path().display()
        );

        let ingestor = BatchIngestor::new(object_store_factory.clone(), log_metadata.clone());

        let grpc_server_fut = run_grpc_server(
            admin.clone(),
            log_metadata,
            ingestor.client(),
            metadata_address,
            ct.clone(),
        );

        let http_ingestor_fut = run_http_server(admin, ingestor.client(), http_address, ct.clone());

        let ingestor_fut = run_background_ingestor(ingestor, ct);

        tokio::select! {
            res = grpc_server_fut => {
                info!("gRPC server exited with {:?}", res);
            },
            res = http_ingestor_fut => {
                info!("HTTP ingestor server exited with {:?}", res);
            },
            res = ingestor_fut => {
                info!("Background ingestor exited with {:?}", res);
            },
        }

        Ok(())
    }
}

async fn new_dev_cluster_metadata_service() -> (Arc<InMemoryClusterMetadata>, NamespaceName) {
    let cluster_meta = Arc::new(InMemoryClusterMetadata::default());

    let default_tenant = TenantName::new_unchecked("default");
    cluster_meta
        .create_tenant(default_tenant.clone())
        .await
        .expect("failed to create default tenant");

    let default_namespace = NamespaceName::new_unchecked("default", default_tenant);
    let default_namespace_options =
        NamespaceOptions::new(SecretName::new_unchecked("default-bucket"));
    cluster_meta
        .create_namespace(default_namespace.clone(), default_namespace_options)
        .await
        .expect("failed to create default namespace");

    (cluster_meta, default_namespace)
}

async fn run_grpc_server(
    cluster_meta: Arc<InMemoryClusterMetadata>,
    log_meta: Arc<InMemoryLogMetadata>,
    _batch_ingestor: BatchIngestorClient,
    address: SocketAddr,
    ct: CancellationToken,
) -> Result<()> {
    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(
            wings_control_plane::cluster_metadata::tonic::file_descriptor_set(),
        )
        .register_encoded_file_descriptor_set(
            wings_control_plane::log_metadata::tonic::file_descriptor_set(),
        )
        .build_v1()
        .context(TonicReflectionSnafu {})?;

    let admin_service = ClusterMetadataServer::new(cluster_meta).into_tonic_server();
    let offset_registry_service = LogMetadataServer::new(log_meta).into_tonic_server();

    let server = tonic::transport::Server::builder()
        .add_service(reflection_service)
        .add_service(admin_service)
        .add_service(offset_registry_service)
        .serve_with_shutdown(address, async move {
            ct.cancelled().await;
        });

    server.await.context(TonicServerSnafu {})
}

async fn run_http_server(
    cluster_meta: Arc<dyn ClusterMetadata>,
    batch_ingestor: BatchIngestorClient,
    address: SocketAddr,
    ct: CancellationToken,
) -> Result<()> {
    let topic_cache = TopicCache::new(cluster_meta.clone());
    let namespace_cache = NamespaceCache::new(cluster_meta.clone());

    let ingestor = HttpIngestor::new(topic_cache, namespace_cache, batch_ingestor);

    let app = Router::new().merge(ingestor.into_router());

    let listener = tokio::net::TcpListener::bind(&address)
        .await
        .context(IoSnafu {})?;

    let server = axum::serve(listener, app).with_graceful_shutdown(async move {
        ct.cancelled().await;
    });

    server.await.context(IoSnafu {})
}
