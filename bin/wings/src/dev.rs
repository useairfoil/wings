use std::{net::SocketAddr, sync::Arc};

use clap::Args;
use object_store::{
    ObjectStore as DynObjectStore, aws::AmazonS3Builder, azure::MicrosoftAzureBuilder,
    gcp::GoogleCloudStorageBuilder,
};
use snafu::ResultExt;
use tokio_util::sync::CancellationToken;
use tracing::info;
use wings_dst_base::{Clock, ThreadRng};
use wings_meta_db::ClusterStore;
use wings_secret_manager::{SecretManager, UnsecureObjectStorageSecretManager};
use wings_server_cluster::ClusterService;

use crate::error::{
    InvalidGrpcAddressSnafu, ObjectStoreSnafu, Result, SecretManagerSnafu, TonicReflectionSnafu,
    TonicServerSnafu,
};

const AWS_ACCESS_KEY_ID_ENV: &str = "AWS_ACCESS_KEY_ID";
const AWS_SECRET_ACCESS_KEY_ENV: &str = "AWS_SECRET_ACCESS_KEY";
const SEAWEEDFS_ENDPOINT: &str = "http://localhost:8333";
const SEAWEEDFS_BUCKET: &str = "default-bucket";
const SEAWEEDFS_REGION: &str = "us-east-1";
const SEAWEEDFS_ACCESS_KEY_ID: &str = "wingsdevaccesskey";
const SEAWEEDFS_SECRET_ACCESS_KEY: &str = "wingsdevsecretkey";

#[derive(Debug, Args)]
pub struct DevArgs {
    /// The address of the gRPC server.
    #[arg(long = "grpc.address", default_value = "127.0.0.1:7253")]
    grpc_address: String,
    #[command(flatten)]
    object_store: ObjectStoreArgs,
}

#[derive(Debug, Args)]
struct ObjectStoreArgs {
    /// The type of object store to use for cluster metadata.
    #[arg(long = "object-store.type", default_value = "aws")]
    object_store_type: ObjectStoreType,
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum ObjectStoreType {
    Aws,
    Gcp,
    Azure,
}

struct ClusterObjectStore {
    object_store: Arc<dyn DynObjectStore>,
}

impl DevArgs {
    pub async fn run(
        self,
        ct: CancellationToken,
        clock: Arc<dyn Clock>,
        rng: Arc<ThreadRng>,
    ) -> Result<()> {
        let cluster_object_store = self.object_store.create_object_store()?;
        let secret_manager: Arc<dyn SecretManager> = Arc::new(
            UnsecureObjectStorageSecretManager::new(cluster_object_store.object_store())
                .await
                .context(SecretManagerSnafu {})?,
        );
        let cluster_store = ClusterStore::new(
            cluster_object_store.object_store(),
            secret_manager,
            clock,
            rng,
        );
        let grpc_address = self
            .grpc_address
            .parse::<SocketAddr>()
            .context(InvalidGrpcAddressSnafu {})?;

        info!(address = %grpc_address, "Starting gRPC server");

        run_grpc_server(cluster_store, grpc_address, ct).await
    }
}

impl ClusterObjectStore {
    fn object_store(&self) -> Arc<dyn DynObjectStore> {
        Arc::clone(&self.object_store)
    }
}

async fn run_grpc_server(
    cluster_store: ClusterStore,
    address: SocketAddr,
    ct: CancellationToken,
) -> Result<()> {
    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(
            wings_server_cluster::pb::cluster_file_descriptor_set(),
        )
        .build_v1()
        .context(TonicReflectionSnafu {})?;

    let cluster_service = ClusterService::new(cluster_store).into_tonic_server();

    tonic::transport::Server::builder()
        .add_service(reflection_service)
        .add_service(cluster_service)
        .serve_with_shutdown(address, async move {
            ct.cancelled().await;
        })
        .await
        .context(TonicServerSnafu {})
}

impl ObjectStoreArgs {
    fn create_object_store(&self) -> Result<ClusterObjectStore> {
        let object_store: Arc<dyn DynObjectStore> = match self.object_store_type {
            ObjectStoreType::Aws => Arc::new(self.create_aws_object_store()?),
            ObjectStoreType::Gcp => Arc::new(
                GoogleCloudStorageBuilder::from_env()
                    .build()
                    .context(ObjectStoreSnafu {})?,
            ),
            ObjectStoreType::Azure => Arc::new(
                MicrosoftAzureBuilder::from_env()
                    .build()
                    .context(ObjectStoreSnafu {})?,
            ),
        };

        Ok(ClusterObjectStore { object_store })
    }

    fn create_aws_object_store(&self) -> Result<impl DynObjectStore> {
        let mut builder = AmazonS3Builder::from_env();

        if aws_credentials_env_missing() {
            info!(
                endpoint = SEAWEEDFS_ENDPOINT,
                bucket = SEAWEEDFS_BUCKET,
                "Using SeaweedFS object store defaults"
            );

            builder = builder
                .with_endpoint(SEAWEEDFS_ENDPOINT)
                .with_bucket_name(SEAWEEDFS_BUCKET)
                .with_region(SEAWEEDFS_REGION)
                .with_access_key_id(SEAWEEDFS_ACCESS_KEY_ID)
                .with_secret_access_key(SEAWEEDFS_SECRET_ACCESS_KEY)
                .with_allow_http(true);
        }

        builder.build().context(ObjectStoreSnafu {})
    }
}

fn aws_credentials_env_missing() -> bool {
    std::env::var_os(AWS_ACCESS_KEY_ID_ENV).is_none()
        && std::env::var_os(AWS_SECRET_ACCESS_KEY_ENV).is_none()
}
