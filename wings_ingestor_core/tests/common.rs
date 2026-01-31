use std::{sync::Arc, time::Duration};

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use wings_control_plane_core::cluster_metadata::ClusterMetadata;
use wings_control_plane_memory::InMemoryControlPlane;
use wings_ingestor_core::{BatchIngestor, BatchIngestorClient};
use wings_object_store::TemporaryFileSystemFactory;
use wings_resources::{
    AwsConfiguration, DataLakeConfiguration, DataLakeName, Namespace, NamespaceName,
    NamespaceOptions, ObjectStoreConfiguration, ObjectStoreName, TenantName,
};

pub fn create_batch_ingestor() -> (
    JoinHandle<()>,
    BatchIngestorClient,
    Arc<dyn ClusterMetadata>,
    CancellationToken,
) {
    let control_plane: Arc<_> = InMemoryControlPlane::new().into();
    let object_store_factory: Arc<_> = TemporaryFileSystemFactory::new(control_plane.clone())
        .expect("object store factory")
        .into();
    let ingestor = BatchIngestor::new(object_store_factory, control_plane.clone());

    let client = ingestor.client();
    let ct = CancellationToken::new();
    let task = tokio::spawn({
        let ct = ct.clone();
        async move {
            ingestor.run(ct).await.expect("ingestor run");
        }
    });

    (task, client, control_plane, ct)
}

pub async fn initialize_test_namespace(cluster_meta: &Arc<dyn ClusterMetadata>) -> Arc<Namespace> {
    let tenant_name = TenantName::new_unchecked("test");
    let _tenant = cluster_meta
        .create_tenant(tenant_name.clone())
        .await
        .expect("create_tenant");

    let object_store = ObjectStoreName::new_unchecked("test-cred", tenant_name.clone());
    let aws_config = AwsConfiguration {
        bucket_name: "test".to_string(),
        access_key_id: Default::default(),
        secret_access_key: Default::default(),
        prefix: None,
        region: None,
    };
    cluster_meta
        .create_object_store(
            object_store.clone(),
            ObjectStoreConfiguration::Aws(aws_config),
        )
        .await
        .expect("create_object_store");

    let data_lake = DataLakeName::new_unchecked("test-data-lake", tenant_name.clone());
    cluster_meta
        .create_data_lake(
            data_lake.clone(),
            DataLakeConfiguration::Parquet(Default::default()),
        )
        .await
        .expect("create_data_lake");

    let namespace_name = NamespaceName::new_unchecked("test-ns", tenant_name);
    let namespace = cluster_meta
        .create_namespace(
            namespace_name.clone(),
            NamespaceOptions::new(object_store, data_lake)
                .with_flush_interval(default_flush_interval()),
        )
        .await
        .expect("create_namespace");

    namespace.into()
}

pub fn default_flush_interval() -> Duration {
    Duration::from_secs(1)
}
