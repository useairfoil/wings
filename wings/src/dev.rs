use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use axum::Router;
use clap::{Args, ValueEnum};
use snafu::ResultExt;
use tokio_util::sync::CancellationToken;
use tracing::info;
use wings_control_plane_core::{
    cluster_metadata::{
        ClusterMetadata,
        cache::{NamespaceCache, TopicCache},
        tonic::ClusterMetadataServer,
    },
    log_metadata::tonic::LogMetadataServer,
};
use wings_control_plane_memory::{InMemoryClusterMetadata, InMemoryLogMetadata};
use wings_flight::WingsFlightSqlServer;
use wings_ingestor_core::{BatchIngestor, BatchIngestorClient, run_background_ingestor};
use wings_ingestor_http::HttpIngestor;
use wings_object_store::{
    CloudObjectStoreFactory, LocalFileSystemFactory, ObjectStoreFactory, TemporaryFileSystemFactory,
};
use wings_observability::MetricsExporter;
use wings_resources::{
    DataLakeConfiguration, DataLakeName, NamespaceName, NamespaceOptions, ObjectStoreConfiguration,
    ObjectStoreName, S3CompatibleConfiguration, TenantName,
};
use wings_server_core::query::NamespaceProviderFactory;
use wings_worker::{WorkerPool, WorkerPoolOptions, run_worker_pool};

use crate::error::{
    InvalidServerUrlSnafu, IoSnafu, Result, TonicReflectionSnafu, TonicServerSnafu,
};

#[derive(Debug, Args)]
pub struct DevArgs {
    /// The address of the gRPC metadata server.
    #[arg(
        long("metadata.address"),
        default_value = "127.0.0.1:7777",
        env = "WINGS_METADATA_ADDRESS"
    )]
    metadata_address: String,
    /// The address of the HTTP ingestor server.
    #[arg(
        long("http.address"),
        default_value = "127.0.0.1:7780",
        env = "WINGS_HTTP_ADDRESS"
    )]
    http_address: String,
    /// The type of object store to use.
    #[arg(long, default_value = "temp", env = "WINGS_OBJECT_STORE")]
    object_store: ObjectStoreType,
    /// The root path to use for local object storage.
    #[arg(
        long("object-store.local-path"),
        required_if_eq("object_store", "local"),
        env = "WINGS_OBJECT_STORE_LOCAL_PATH"
    )]
    object_store_local_path: Option<String>,
    /// The S3 bucket to use for cloud object storage.
    ///
    /// This value is used to create the object store for the default tenant.
    #[arg(
        long("default.object-store.cloud-bucket"),
        required_if_eq("object_store", "cloud"),
        env = "WINGS_DEFAULT_OBJECT_STORE_CLOUD_BUCKET"
    )]
    default_object_store_cloud_bucket: Option<String>,
    /// The S3 access key ID to use for cloud object storage.
    ///
    /// This value is used to create the object store for the default tenant.
    #[arg(
        long("default.object-store.cloud-access-key-id"),
        required_if_eq("object_store", "cloud"),
        env = "WINGS_DEFAULT_OBJECT_STORE_CLOUD_ACCESS_KEY_ID"
    )]
    default_object_store_cloud_access_key_id: Option<String>,
    /// The S3 secret access key ID to use for cloud object storage.
    ///
    /// This value is used to create the object store for the default tenant.
    #[arg(
        long("default.object-store.cloud-secret-access-key-id"),
        required_if_eq("object_store", "cloud"),
        env = "WINGS_DEFAULT_OBJECT_STORE_CLOUD_SECRET_ACCESS_KEY_ID"
    )]
    default_object_store_cloud_secret_access_key_id: Option<String>,
    /// The S3 endpoint to use for cloud object storage.
    ///
    /// This value is used to create the object store for the default tenant.
    #[arg(
        long("default.object-store.cloud-endpoint"),
        default_value = "http://localhost:9000",
        env = "WINGS_DEFAULT_OBJECT_STORE_CLOUD_ENDPOINT"
    )]
    default_object_store_cloud_endpoint: String,
    /// The S3 prefix to use for the default object store.
    #[arg(
        long("default.object-store.cloud-prefix"),
        env = "WINGS_DEFAULT_OBJECT_STORE_CLOUD_PREFIX"
    )]
    default_object_store_cloud_prefix: Option<String>,
    /// The data lake for the default namespace.
    #[arg(long, default_value = "parquet", env = "WINGS_DEFAULT_DATA_LAKE")]
    default_data_lake: DataLakeType,
}

#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum ObjectStoreType {
    /// Use a temporary directory for object storage.
    #[default]
    #[clap(name = "temp")]
    Temporary,
    /// Use a local directory for object storage.
    #[clap(name = "local")]
    Local,
    /// Use a cloud object store for object storage.
    ///
    /// By default, the endpoint is set to `http://localhost:9000` for use with RustFS and MinIO.
    #[clap(name = "cloud")]
    Cloud,
}

#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum DataLakeType {
    /// Store files as plain Parquet files.
    #[default]
    #[clap(name = "parquet")]
    Parquet,
    /// Store files in an Iceberg table.
    #[clap(name = "iceberg")]
    Iceberg,
    /// Store files in a Delta table.
    #[clap(name = "delta")]
    Delta,
}

impl DevArgs {
    pub async fn run(self, metrics_exporter: MetricsExporter, ct: CancellationToken) -> Result<()> {
        let (cluster_metadata, default_tenant) = new_dev_cluster_metadata_service().await;

        let (object_store_factory, default_object_store) = self
            .new_object_store_factory(cluster_metadata.clone(), &default_tenant)
            .await;

        let default_data_lake = self.new_data_lake(&cluster_metadata, &default_tenant).await;
        let default_namespace = self
            .new_namespace(
                &cluster_metadata,
                &default_tenant,
                &default_object_store,
                &default_data_lake,
            )
            .await;

        let metadata_address = self
            .metadata_address
            .parse::<SocketAddr>()
            .context(InvalidServerUrlSnafu {})?;

        let http_address = self
            .http_address
            .parse::<SocketAddr>()
            .context(InvalidServerUrlSnafu {})?;

        info!("Starting Wings in development mode");
        info!("gRPC server listening on {}", metadata_address);
        info!("HTTP ingestor listening on {}", http_address);
        info!("Default tenant: {}", default_tenant);
        info!("Default namespace: {}", default_namespace);
        info!("Default object store: {}", default_object_store);
        info!("Default data lake: {}", default_data_lake);
        info!(
            "You can create new tenants, namespaces, object stores, data lakes, and topics using the CLI"
        );

        let _ct_guard = ct.child_token().drop_guard();

        let log_metadata = Arc::new(InMemoryLogMetadata::new(cluster_metadata.clone()));

        let namespace_provider_factory = NamespaceProviderFactory::new(
            cluster_metadata.clone(),
            log_metadata.clone(),
            metrics_exporter.clone(),
            object_store_factory.clone(),
        );

        let ingestor = BatchIngestor::new(object_store_factory.clone(), log_metadata.clone());
        let worker_pool = {
            let topic_cache = TopicCache::new(cluster_metadata.clone());
            let namespace_cache = NamespaceCache::new(cluster_metadata.clone());
            WorkerPool::new(
                topic_cache,
                namespace_cache,
                log_metadata.clone(),
                cluster_metadata.clone(),
                object_store_factory.clone(),
                namespace_provider_factory.clone(),
                WorkerPoolOptions::default(),
            )
        };

        let grpc_server_fut = run_grpc_server(
            cluster_metadata.clone(),
            log_metadata,
            namespace_provider_factory,
            ingestor.client(),
            metadata_address,
            ct.clone(),
        );

        let http_ingestor_fut = run_http_server(
            cluster_metadata,
            ingestor.client(),
            http_address,
            ct.clone(),
        );

        let ingestor_fut = run_background_ingestor(ingestor, ct.clone());
        let worker_pool_fut = run_worker_pool(worker_pool, ct);

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
            res = worker_pool_fut => {
                info!("Worker pool exited with {:?}", res);
            },
        }

        Ok(())
    }

    async fn new_object_store_factory(
        &self,
        cluster: Arc<InMemoryClusterMetadata>,
        tenant: &TenantName,
    ) -> (Arc<dyn ObjectStoreFactory>, ObjectStoreName) {
        let object_store_name = ObjectStoreName::new_unchecked("default", tenant.clone());

        let backend: Arc<dyn ObjectStoreFactory> = match self.object_store {
            ObjectStoreType::Temporary => {
                let factory = TemporaryFileSystemFactory::new(cluster.clone())
                    .expect("TemporaryFileSystemFactory");
                info!(
                    "Using temporary directory for object store: {:?}",
                    factory.root_path()
                );
                Arc::new(factory)
            }
            ObjectStoreType::Local => {
                let path = self
                    .object_store_local_path
                    .clone()
                    .expect("object-store.local-path");
                let path: PathBuf = path.parse().expect("failed to parse path");
                let factory = LocalFileSystemFactory::new(path, cluster.clone())
                    .expect("LocalFileSystemFactory");
                info!(
                    "Using directory for object store: {:?}",
                    factory.root_path()
                );
                Arc::new(factory)
            }
            ObjectStoreType::Cloud => {
                let factory = CloudObjectStoreFactory::new(cluster.clone());
                info!("Using cloud object store",);
                Arc::new(factory)
            }
        };

        let aws_config = S3CompatibleConfiguration {
            bucket_name: self
                .default_object_store_cloud_bucket
                .clone()
                .unwrap_or_else(|| "wings-root".to_string()),
            access_key_id: self
                .default_object_store_cloud_access_key_id
                .clone()
                .unwrap_or_default(),
            secret_access_key: self
                .default_object_store_cloud_secret_access_key_id
                .clone()
                .unwrap_or_default(),
            endpoint: self.default_object_store_cloud_endpoint.clone(),
            prefix: self.default_object_store_cloud_prefix.clone(),
            allow_http: true,
            region: None,
        };

        cluster
            .create_object_store(
                object_store_name.clone(),
                ObjectStoreConfiguration::S3Compatible(aws_config),
            )
            .await
            .expect("failed to create default aws s3 object store");

        (backend, object_store_name)
    }

    async fn new_data_lake(
        &self,
        cluster: &Arc<InMemoryClusterMetadata>,
        tenant: &TenantName,
    ) -> DataLakeName {
        info!(
            "Creating default data lake of type {}",
            self.default_data_lake
        );
        let data_lake_name = DataLakeName::new_unchecked("default", tenant.clone());
        let data_lake_options = match self.default_data_lake {
            DataLakeType::Parquet => DataLakeConfiguration::Parquet(Default::default()),
            DataLakeType::Iceberg => todo!(),
            DataLakeType::Delta => DataLakeConfiguration::Delta(Default::default()),
        };

        cluster
            .create_data_lake(data_lake_name.clone(), data_lake_options)
            .await
            .expect("failed to create default data lake");

        data_lake_name
    }

    async fn new_namespace(
        &self,
        cluster: &Arc<InMemoryClusterMetadata>,
        tenant: &TenantName,
        object_store: &ObjectStoreName,
        data_lake: &DataLakeName,
    ) -> NamespaceName {
        let namespace_name = NamespaceName::new_unchecked("default", tenant.clone());
        let default_namespace_options =
            NamespaceOptions::new(object_store.clone(), data_lake.clone());

        cluster
            .create_namespace(namespace_name.clone(), default_namespace_options)
            .await
            .expect("failed to create default namespace");

        namespace_name
    }
}

async fn new_dev_cluster_metadata_service() -> (Arc<InMemoryClusterMetadata>, TenantName) {
    let cluster_meta = Arc::new(InMemoryClusterMetadata::default());

    let default_tenant = TenantName::new_unchecked("default");
    cluster_meta
        .create_tenant(default_tenant.clone())
        .await
        .expect("failed to create default tenant");

    (cluster_meta, default_tenant)
}

async fn run_grpc_server(
    cluster_meta: Arc<InMemoryClusterMetadata>,
    log_meta: Arc<InMemoryLogMetadata>,
    namespace_provider_factory: NamespaceProviderFactory,
    batch_ingestor: BatchIngestorClient,
    address: SocketAddr,
    ct: CancellationToken,
) -> Result<()> {
    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(
            wings_control_plane_core::pb::cluster_metadata_file_descriptor_set(),
        )
        .register_encoded_file_descriptor_set(
            wings_control_plane_core::pb::log_metadata_file_descriptor_set(),
        )
        .register_encoded_file_descriptor_set(WingsFlightSqlServer::file_descriptor_set())
        .build_v1()
        .context(TonicReflectionSnafu {})?;

    let topic_cache = TopicCache::new(cluster_meta.clone());
    let namespace_cache = NamespaceCache::new(cluster_meta.clone());

    let admin_service = ClusterMetadataServer::new(cluster_meta).into_tonic_server();
    let offset_registry_service = LogMetadataServer::new(log_meta).into_tonic_server();
    let sql_service = WingsFlightSqlServer::new(
        namespace_cache,
        topic_cache,
        batch_ingestor,
        namespace_provider_factory,
    )
    .into_tonic_server();

    let server = tonic::transport::Server::builder()
        .add_service(reflection_service)
        .add_service(admin_service)
        .add_service(offset_registry_service)
        .add_service(sql_service)
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

impl std::fmt::Display for DataLakeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataLakeType::Parquet => write!(f, "parquet"),
            DataLakeType::Iceberg => write!(f, "iceberg"),
            DataLakeType::Delta => write!(f, "delta"),
        }
    }
}
