#![allow(dead_code)]
use std::{sync::Arc, time::Duration};

use datafusion::common::arrow::datatypes::{DataType, Field, Schema};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use wings_ingestor_core::{BatchIngestor, BatchIngestorClient};
use wings_control_plane::{
    admin::{
        Admin, InMemoryAdminService, Namespace, NamespaceName, NamespaceOptions, SecretName,
        TenantName, Topic, TopicName, TopicOptions,
    },
    offset_registry::InMemoryOffsetRegistry,
};
use wings_object_store::TemporaryFileSystemFactory;
use wings_server_core::query::NamespaceProviderFactory;

pub fn create_ingestor_and_provider() -> (
    JoinHandle<()>,
    BatchIngestorClient,
    NamespaceProviderFactory,
    Arc<dyn Admin>,
    CancellationToken,
) {
    let admin: Arc<_> = InMemoryAdminService::new().into();
    let object_store_factory: Arc<_> = TemporaryFileSystemFactory::new()
        .expect("object store factory")
        .into();
    let offset_registry: Arc<_> = InMemoryOffsetRegistry::new().into();
    let factory = NamespaceProviderFactory::new(
        admin.clone(),
        offset_registry.clone(),
        object_store_factory.clone(),
    );
    let ingestor = BatchIngestor::new(object_store_factory, offset_registry);

    let client = ingestor.client();
    let ct = CancellationToken::new();
    let task = tokio::spawn({
        let ct = ct.clone();
        async move {
            ingestor.run(ct).await.expect("ingestor run");
        }
    });

    (task, client, factory, admin, ct)
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

pub async fn initialize_test_partitioned_topic(
    admin: &Arc<dyn Admin>,
    namespace: &NamespaceName,
) -> Arc<Topic> {
    let topic_name = TopicName::new_unchecked("my_partitioned_topic", namespace.clone());
    let schema = schema_with_partition();
    let topic = admin
        .create_topic(
            topic_name,
            TopicOptions::new_with_partition_key(schema.fields, Some(0)),
        )
        .await
        .expect("create_topic");

    topic.into()
}

pub async fn initialize_test_topic(
    admin: &Arc<dyn Admin>,
    namespace: &NamespaceName,
) -> Arc<Topic> {
    let topic_name = TopicName::new_unchecked("my_topic", namespace.clone());
    let schema = schema_without_partition();
    let topic = admin
        .create_topic(topic_name, TopicOptions::new(schema.fields))
        .await
        .expect("create_topic");

    topic.into()
}

pub fn default_flush_interval() -> Duration {
    Duration::from_secs(1)
}

pub fn schema_without_partition() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
    ])
}

pub fn schema_with_partition() -> Schema {
    Schema::new(vec![
        Field::new("region_id", DataType::Int64, false),
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
    ])
}
