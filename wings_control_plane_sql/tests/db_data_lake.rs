use wings_resources::{DataLakeConfiguration, DataLakeName, ParquetConfiguration};

mod common;

#[tokio::test]
async fn test_data_lake_roundtrip() {
    let db = common::new_test_db().await;

    common::seed_tenant(&db).await;

    let name = DataLakeName::parse("tenants/abcd/data-lakes/xyz").unwrap();
    let config = DataLakeConfiguration::Parquet(ParquetConfiguration::default());

    let back = db
        .create_data_lake(name.clone(), config.clone())
        .await
        .unwrap();

    assert_eq!(back.name, name);
    assert_eq!(back.data_lake, config);
}
