use std::{sync::Arc, time::Duration};

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use wings_ingestor_core::{BatchIngestor, BatchIngestorClient};
use wings_metadata_core::{
    admin::{
        Admin, InMemoryAdminService, Namespace, NamespaceName, NamespaceOptions, SecretName,
        TenantName,
    },
    offset_registry::InMemoryOffsetRegistry,
};
use wings_object_store::TemporaryFileSystemFactory;

pub fn create_batch_ingestor() -> (
    JoinHandle<()>,
    BatchIngestorClient,
    Arc<dyn Admin>,
    CancellationToken,
) {
    let admin: Arc<_> = InMemoryAdminService::new().into();
    let object_store_factory: Arc<_> = TemporaryFileSystemFactory::new()
        .expect("object store factory")
        .into();
    let offset_registry: Arc<_> = InMemoryOffsetRegistry::new().into();
    let ingestor = BatchIngestor::new(object_store_factory, offset_registry);

    let client = ingestor.client();
    let ct = CancellationToken::new();
    let task = tokio::spawn({
        let ct = ct.clone();
        async move {
            ingestor.run(ct).await.expect("ingestor run");
        }
    });

    (task, client, admin, ct)
}

pub async fn initialize_test_namespace(admin: &Arc<dyn Admin>) -> Arc<Namespace> {
    let tenant_name = TenantName::new_unchecked("test");
    let _tenant = admin
        .create_tenant(tenant_name.clone())
        .await
        .expect("create_tenant");
    let namespace_name = NamespaceName::new_unchecked("test_ns", tenant_name);
    let namespace = admin
        .create_namespace(
            namespace_name.clone(),
            NamespaceOptions::new(SecretName::new_unchecked("my-secret"))
                .with_flush_interval(default_flush_interval()),
        )
        .await
        .expect("create_namespace");

    namespace.into()
}

pub fn default_flush_interval() -> Duration {
    Duration::from_secs(1)
}
