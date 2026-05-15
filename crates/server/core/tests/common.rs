#![allow(dead_code)]
use std::{sync::Arc, time::Duration};

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use wings_control_plane_core::cluster_metadata::ClusterMetadata;
use wings_control_plane_sql::SqlControlPlane;
use wings_ingestor_core::{Ingestor, IngestorClient};
use wings_object_store::TemporaryFileSystemFactory;
use wings_observability::MetricsExporter;
use wings_resources::{
    AwsConfiguration, DataLakeConfiguration, DataLakeName, Namespace, NamespaceName,
    NamespaceOptions, ObjectStoreConfiguration, ObjectStoreName, TenantName, Table, TableName,
    TableOptions,
};
use wings_schema::{DataType, Field, Schema, SchemaBuilder};
use wings_server_core::query::NamespaceProviderFactory;

pub async fn create_ingestor_and_provider() -> (
    JoinHandle<()>,
    IngestorClient,
    NamespaceProviderFactory,
    Arc<dyn ClusterMetadata>,
    CancellationToken,
) {
    let metrics_exporter = MetricsExporter::default();
    let control_plane: Arc<_> = SqlControlPlane::new_in_memory().await.into();
    let object_store_factory: Arc<_> = TemporaryFileSystemFactory::new(control_plane.clone())
        .expect("object store factory")
        .into();
    let factory = NamespaceProviderFactory::new(
        control_plane.clone(),
        control_plane.clone(),
        metrics_exporter,
        object_store_factory.clone(),
    );
    let ingestor = Ingestor::new(object_store_factory, control_plane.clone());

    let client = ingestor.client();
    let ct = CancellationToken::new();
    let task = tokio::spawn({
        let ct = ct.clone();
        async move {
            ingestor.run(ct).await.expect("ingestor run");
        }
    });

    (task, client, factory, control_plane, ct)
}

pub async fn initialize_test_namespace(cluster_meta: &Arc<dyn ClusterMetadata>) -> Arc<Namespace> {
    let tenant_name = TenantName::new_unchecked("test");
    let _tenant = cluster_meta
        .create_tenant(tenant_name.clone())
        .await
        .expect("create_tenant");

    let object_store_name = ObjectStoreName::new_unchecked("test-cred", tenant_name.clone());
    let aws_config = AwsConfiguration {
        bucket_name: "test".to_string(),
        access_key_id: Default::default(),
        secret_access_key: Default::default(),
        prefix: None,
        region: None,
    };
    cluster_meta
        .create_object_store(
            object_store_name.clone(),
            ObjectStoreConfiguration::Aws(aws_config),
        )
        .await
        .expect("create_object_store");

    let data_lake_name = DataLakeName::new_unchecked("test-data-lake", tenant_name.clone());
    cluster_meta
        .create_data_lake(
            data_lake_name.clone(),
            DataLakeConfiguration::Parquet(Default::default()),
        )
        .await
        .expect("create_data_lake");

    let namespace_name = NamespaceName::new_unchecked("test-ns", tenant_name);
    let namespace = cluster_meta
        .create_namespace(
            namespace_name.clone(),
            NamespaceOptions::new(object_store_name, data_lake_name)
                .with_flush_interval(default_flush_interval()),
        )
        .await
        .expect("create_namespace");

    namespace.into()
}

pub async fn initialize_test_partitioned_table(
    cluster_meta: &Arc<dyn ClusterMetadata>,
    namespace: &NamespaceName,
) -> Arc<Table> {
    let table_name = TableName::new_unchecked("my_partitioned_table", namespace.clone());
    let schema = schema_with_partition();
    let table = cluster_meta
        .create_table(
            table_name,
            TableOptions::new_with_partition_key(schema, Some(0)),
        )
        .await
        .expect("create_table");

    table.into()
}

pub async fn initialize_test_table(
    cluster_meta: &Arc<dyn ClusterMetadata>,
    namespace: &NamespaceName,
) -> Arc<Table> {
    let table_name = TableName::new_unchecked("my_table", namespace.clone());
    let schema = schema_without_partition();
    let table = cluster_meta
        .create_table(table_name, TableOptions::new(schema))
        .await
        .expect("create_table");

    table.into()
}

pub fn default_flush_interval() -> Duration {
    Duration::from_secs(1)
}

pub fn schema_without_partition() -> Schema {
    SchemaBuilder::new(vec![
        Field::new("id", 1, DataType::Int32, false),
        Field::new("name", 2, DataType::Utf8, false),
        Field::new("age", 3, DataType::Int32, false),
    ])
    .build()
    .unwrap()
}

pub fn schema_with_partition() -> Schema {
    SchemaBuilder::new(vec![
        Field::new("region_id", 0, DataType::Int64, false),
        Field::new("id", 1, DataType::Int32, false),
        Field::new("name", 2, DataType::Utf8, false),
        Field::new("age", 3, DataType::Int32, false),
    ])
    .build()
    .unwrap()
}
