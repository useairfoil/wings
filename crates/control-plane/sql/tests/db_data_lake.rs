use wings_control_plane_core::cluster_metadata::{
    ClusterMetadata, ClusterMetadataError, ListDataLakesRequest,
};
use wings_control_plane_sql::SqlControlPlane;
use wings_resources::{DataLakeConfiguration, DataLakeName, ParquetConfiguration};

mod common;

#[tokio::test]
async fn test_data_lake_roundtrip() {
    let cp = SqlControlPlane::new_in_memory().await;

    common::seed_tenant(&cp).await;

    let name = DataLakeName::parse("tenants/abcd/data-lakes/xyz").unwrap();
    let config = DataLakeConfiguration::Parquet(ParquetConfiguration::default());

    let back = cp
        .create_data_lake(name.clone(), config.clone())
        .await
        .unwrap();

    assert_eq!(back.name, name);
    assert_eq!(back.data_lake, config);
}

#[tokio::test]
async fn test_get_data_lake() {
    let cp = SqlControlPlane::new_in_memory().await;

    common::seed_tenant(&cp).await;
    common::seed_data_lake(&cp).await;

    let name = DataLakeName::parse("tenants/abcd/data-lakes/xyz").unwrap();

    let back = cp.get_data_lake(name.clone()).await.unwrap();

    assert_eq!(back.name, name);
}

#[tokio::test]
async fn test_get_data_lake_fails_if_not_found() {
    let cp = SqlControlPlane::new_in_memory().await;

    common::seed_tenant(&cp).await;

    let name = DataLakeName::parse("tenants/abcd/data-lakes/nonexistent").unwrap();

    let result = cp.get_data_lake(name).await;

    assert!(matches!(result, Err(ClusterMetadataError::NotFound { .. })));
}

#[tokio::test]
async fn test_list_data_lakes() {
    let cp = SqlControlPlane::new_in_memory().await;

    common::seed_tenant(&cp).await;
    common::seed_data_lake(&cp).await;

    let tenant_name = wings_resources::TenantName::parse("tenants/abcd").unwrap();
    let request = ListDataLakesRequest::new(tenant_name);

    let response = cp.list_data_lakes(request).await.unwrap();

    assert_eq!(response.data_lakes.len(), 1);
    assert_eq!(response.data_lakes[0].name.id, "xyz");
    assert!(response.next_page_token.is_none());
}

#[tokio::test]
async fn test_list_data_lakes_empty() {
    let cp = SqlControlPlane::new_in_memory().await;

    common::seed_tenant(&cp).await;

    let tenant_name = wings_resources::TenantName::parse("tenants/abcd").unwrap();
    let request = ListDataLakesRequest::new(tenant_name);

    let response = cp.list_data_lakes(request).await.unwrap();

    assert!(response.data_lakes.is_empty());
    assert!(response.next_page_token.is_none());
}

#[tokio::test]
async fn test_delete_data_lake() {
    let cp = SqlControlPlane::new_in_memory().await;

    common::seed_tenant(&cp).await;
    common::seed_data_lake(&cp).await;

    let name = DataLakeName::parse("tenants/abcd/data-lakes/xyz").unwrap();

    cp.delete_data_lake(name.clone()).await.unwrap();

    let result = cp.get_data_lake(name).await;
    assert!(matches!(result, Err(ClusterMetadataError::NotFound { .. })));
}

#[tokio::test]
async fn test_delete_data_lake_fails_if_not_found() {
    let cp = SqlControlPlane::new_in_memory().await;

    common::seed_tenant(&cp).await;

    let name = DataLakeName::parse("tenants/abcd/data-lakes/nonexistent").unwrap();

    let result = cp.delete_data_lake(name).await;

    assert!(matches!(result, Err(ClusterMetadataError::NotFound { .. })));
}

#[tokio::test]
async fn test_delete_data_lake_fails_if_used_by_namespace() {
    let cp = SqlControlPlane::new_in_memory().await;

    common::seed_tenant(&cp).await;
    common::seed_data_lake(&cp).await;
    common::seed_object_store(&cp).await;
    common::seed_namespace(&cp).await;

    let data_lake_name = DataLakeName::parse("tenants/abcd/data-lakes/xyz").unwrap();

    let result = cp.delete_data_lake(data_lake_name).await;

    assert!(matches!(
        result,
        Err(ClusterMetadataError::FailedPrecondition { .. })
    ));
}

#[tokio::test]
async fn test_create_data_lake_fails_if_parent_tenant_doesnt_exist() {
    let cp = SqlControlPlane::new_in_memory().await;

    let name = DataLakeName::parse("tenants/abcd/data-lakes/xyz").unwrap();
    let config = DataLakeConfiguration::Parquet(ParquetConfiguration::default());

    let result = cp.create_data_lake(name.clone(), config.clone()).await;

    assert!(matches!(result, Err(ClusterMetadataError::NotFound { .. })));
}
