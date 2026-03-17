#![allow(unused)]
use sea_orm::ConnectOptions;
use wings_control_plane_sql::Database;
use wings_resources::{
    AwsConfiguration, DataLakeConfiguration, DataLakeName, ObjectStoreConfiguration,
    ObjectStoreName, ParquetConfiguration, TenantName,
};

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

pub async fn seed_tenant(db: &Database) {
    let name = TenantName::new_unchecked("abcd");
    db.create_tenant(name.clone()).await.unwrap();
}

pub async fn seed_data_lake(db: &Database) {
    let name = DataLakeName::parse("tenants/abcd/data-lakes/xyz").unwrap();
    let config = DataLakeConfiguration::Parquet(ParquetConfiguration::default());

    db.create_data_lake(name.clone(), config.clone())
        .await
        .unwrap();
}

pub async fn seed_object_store(db: &Database) {
    let name = ObjectStoreName::parse("tenants/abcd/object-stores/xyz").unwrap();
    let config = ObjectStoreConfiguration::Aws(AwsConfiguration {
        bucket_name: "my-bucket".to_string(),
        access_key_id: "my-access-key".to_string(),
        secret_access_key: "my-secret-key".to_string(),
        prefix: None,
        region: None,
    });

    db.create_object_store(name.clone(), config.clone())
        .await
        .unwrap();
}
