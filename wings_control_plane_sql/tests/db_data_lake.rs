use wings_control_plane_sql::db::Error;
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

#[tokio::test]
async fn test_get_data_lake() {
    let db = common::new_test_db().await;

    common::seed_tenant(&db).await;
    common::seed_data_lake(&db).await;

    let name = DataLakeName::parse("tenants/abcd/data-lakes/xyz").unwrap();

    let back = db.get_data_lake(name.clone()).await.unwrap();

    assert_eq!(back.name, name);
}

#[tokio::test]
async fn test_get_data_lake_fails_if_not_found() {
    let db = common::new_test_db().await;

    common::seed_tenant(&db).await;

    let name = DataLakeName::parse("tenants/abcd/data-lakes/nonexistent").unwrap();

    let result = db.get_data_lake(name).await;

    assert!(matches!(
        result,
        Err(Error::NotFound {
            resource: "data-lake",
            ..
        })
    ));
}

#[tokio::test]
async fn test_list_data_lakes() {
    let db = common::new_test_db().await;

    common::seed_tenant(&db).await;
    common::seed_data_lake(&db).await;

    let tenant_name = wings_resources::TenantName::parse("tenants/abcd").unwrap();
    let request =
        wings_control_plane_core::cluster_metadata::ListDataLakesRequest::new(tenant_name);

    let response = db.list_data_lakes(request).await.unwrap();

    assert_eq!(response.data_lakes.len(), 1);
    assert_eq!(response.data_lakes[0].name.id, "xyz");
    assert!(response.next_page_token.is_none());
}

#[tokio::test]
async fn test_list_data_lakes_empty() {
    let db = common::new_test_db().await;

    common::seed_tenant(&db).await;

    let tenant_name = wings_resources::TenantName::parse("tenants/abcd").unwrap();
    let request =
        wings_control_plane_core::cluster_metadata::ListDataLakesRequest::new(tenant_name);

    let response = db.list_data_lakes(request).await.unwrap();

    assert!(response.data_lakes.is_empty());
    assert!(response.next_page_token.is_none());
}

#[tokio::test]
async fn test_delete_data_lake() {
    let db = common::new_test_db().await;

    common::seed_tenant(&db).await;
    common::seed_data_lake(&db).await;

    let name = DataLakeName::parse("tenants/abcd/data-lakes/xyz").unwrap();

    db.delete_data_lake(name.clone()).await.unwrap();

    let result = db.get_data_lake(name).await;
    assert!(matches!(
        result,
        Err(Error::NotFound {
            resource: "data-lake",
            ..
        })
    ));
}

#[tokio::test]
async fn test_delete_data_lake_fails_if_not_found() {
    let db = common::new_test_db().await;

    common::seed_tenant(&db).await;

    let name = DataLakeName::parse("tenants/abcd/data-lakes/nonexistent").unwrap();

    let result = db.delete_data_lake(name).await;

    assert!(matches!(
        result,
        Err(Error::NotFound {
            resource: "data-lake",
            ..
        })
    ));
}

#[tokio::test]
async fn test_delete_data_lake_fails_if_used_by_namespace() {
    let db = common::new_test_db().await;

    common::seed_tenant(&db).await;
    common::seed_data_lake(&db).await;
    common::seed_object_store(&db).await;
    common::seed_namespace(&db).await;

    let data_lake_name = DataLakeName::parse("tenants/abcd/data-lakes/xyz").unwrap();

    let result = db.delete_data_lake(data_lake_name).await;

    assert!(matches!(
        result,
        Err(Error::InvalidArgument {
            resource: "data-lake",
            ..
        })
    ));
}

#[tokio::test]
async fn test_create_data_lake_fails_if_parent_tenant_doesnt_exist() {
    let db = common::new_test_db().await;

    let name = DataLakeName::parse("tenants/abcd/data-lakes/xyz").unwrap();
    let config = DataLakeConfiguration::Parquet(ParquetConfiguration::default());

    let result = db.create_data_lake(name.clone(), config.clone()).await;

    assert!(matches!(
        result,
        Err(Error::NotFound {
            resource: "tenant",
            ..
        })
    ));
}
