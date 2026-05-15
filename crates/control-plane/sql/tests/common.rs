#![allow(unused)]
use sea_orm::ConnectOptions;
use wings_control_plane_core::cluster_metadata::ClusterMetadata;
use wings_control_plane_sql::{Database, SqlControlPlane};
use wings_resources::{
    AwsConfiguration, DataLakeConfiguration, DataLakeName, NamespaceName, NamespaceOptions,
    ObjectStoreConfiguration, ObjectStoreName, ParquetConfiguration, TenantName,
};
use wings_schema::{DataType, Field, SchemaBuilder};

pub async fn new_test_db() -> Database {
    let options = ConnectOptions::new("sqlite::memory:");
    let pool = Database::new(options)
        .await
        .expect("failed to create test database");

    wings_control_plane_sql::migrate(&pool)
        .await
        .expect("failed to migrate database");

    pool
}

pub async fn seed_tenant(cp: &SqlControlPlane) {
    let name = TenantName::new_unchecked("abcd");
    cp.create_tenant(name.clone()).await.unwrap();
}

pub async fn seed_data_lake(cp: &SqlControlPlane) {
    let name = DataLakeName::parse("tenants/abcd/data-lakes/xyz").unwrap();
    let config = DataLakeConfiguration::Parquet(ParquetConfiguration::default());

    cp.create_data_lake(name.clone(), config.clone())
        .await
        .unwrap();
}

pub async fn seed_object_store(cp: &SqlControlPlane) {
    let name = ObjectStoreName::parse("tenants/abcd/object-stores/xyz").unwrap();
    let config = ObjectStoreConfiguration::Aws(AwsConfiguration {
        bucket_name: "my-bucket".to_string(),
        access_key_id: "my-access-key".to_string(),
        secret_access_key: "my-secret-key".to_string(),
        prefix: None,
        region: None,
    });

    cp.create_object_store(name.clone(), config.clone())
        .await
        .unwrap();
}

pub async fn seed_namespace(cp: &SqlControlPlane) {
    let name = NamespaceName::parse("tenants/abcd/namespaces/xyz").unwrap();
    let object_store = ObjectStoreName::parse("tenants/abcd/object-stores/xyz").unwrap();
    let data_lake = DataLakeName::parse("tenants/abcd/data-lakes/xyz").unwrap();
    let options = NamespaceOptions::new(object_store, data_lake);

    cp.create_namespace(name, options).await.unwrap();
}

pub async fn seed_table(cp: &SqlControlPlane) {
    use wings_resources::{TableName, TableOptions};

    let name = TableName::parse("tenants/abcd/namespaces/xyz/tables/my-table").unwrap();
    let schema = SchemaBuilder::new(vec![Field::new("message", 1, DataType::Utf8, false)])
        .build()
        .unwrap();
    let options = TableOptions::new(schema);

    cp.create_table(name, options).await.unwrap();
}
