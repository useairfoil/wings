#![allow(dead_code)]
use std::{sync::Arc, time::Duration};

use datafusion::common::arrow::datatypes::{DataType, Field, Schema};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use wings_control_plane::{
    cluster_metadata::{ClusterMetadata, InMemoryClusterMetadata},
    log_metadata::InMemoryLogMetadata,
    resources::{
        Namespace, NamespaceName, NamespaceOptions, ObjectStoreName, TenantName, Topic, TopicName,
        TopicOptions,
    },
};
use wings_ingestor_core::{BatchIngestor, BatchIngestorClient};
use wings_object_store::TemporaryFileSystemFactory;
use wings_observability::MetricsExporter;
use wings_server_core::query::NamespaceProviderFactory;

pub fn create_ingestor_and_provider() -> (
    JoinHandle<()>,
    BatchIngestorClient,
    NamespaceProviderFactory,
    Arc<dyn ClusterMetadata>,
    CancellationToken,
) {
    let metrics_exporter = MetricsExporter::default();
    let cluster_meta: Arc<_> = InMemoryClusterMetadata::new().into();
    let object_store_factory: Arc<_> = TemporaryFileSystemFactory::new(cluster_meta.clone())
        .expect("object store factory")
        .into();
    let log_meta: Arc<_> = InMemoryLogMetadata::new(cluster_meta.clone()).into();
    let factory = NamespaceProviderFactory::new(
        cluster_meta.clone(),
        log_meta.clone(),
        metrics_exporter,
        object_store_factory.clone(),
    );
    let ingestor = BatchIngestor::new(object_store_factory, log_meta);

    let client = ingestor.client();
    let ct = CancellationToken::new();
    let task = tokio::spawn({
        let ct = ct.clone();
        async move {
            ingestor.run(ct).await.expect("ingestor run");
        }
    });

    (task, client, factory, cluster_meta, ct)
}

pub async fn initialize_test_namespace(cluster_meta: &Arc<dyn ClusterMetadata>) -> Arc<Namespace> {
    let tenant_name = TenantName::new_unchecked("test");
    let _tenant = cluster_meta
        .create_tenant(tenant_name.clone())
        .await
        .expect("create_tenant");
    let namespace_name = NamespaceName::new_unchecked("test-ns", tenant_name.clone());
    let creds = ObjectStoreName::new_unchecked("test-cred", tenant_name);
    let namespace = cluster_meta
        .create_namespace(
            namespace_name.clone(),
            NamespaceOptions::new(creds).with_flush_interval(default_flush_interval()),
        )
        .await
        .expect("create_namespace");

    namespace.into()
}

pub async fn initialize_test_partitioned_topic(
    cluster_meta: &Arc<dyn ClusterMetadata>,
    namespace: &NamespaceName,
) -> Arc<Topic> {
    let topic_name = TopicName::new_unchecked("my_partitioned_topic", namespace.clone());
    let schema = schema_with_partition();
    let topic = cluster_meta
        .create_topic(
            topic_name,
            TopicOptions::new_with_partition_key(schema.fields, Some(0)),
        )
        .await
        .expect("create_topic");

    topic.into()
}

pub async fn initialize_test_topic(
    cluster_meta: &Arc<dyn ClusterMetadata>,
    namespace: &NamespaceName,
) -> Arc<Topic> {
    let topic_name = TopicName::new_unchecked("my_topic", namespace.clone());
    let schema = schema_without_partition();
    let topic = cluster_meta
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
