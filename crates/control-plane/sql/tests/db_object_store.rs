use wings_control_plane_core::cluster_metadata::{
    ClusterMetadata, ClusterMetadataError, ListObjectStoresRequest,
};
use wings_control_plane_sql::SqlControlPlane;
use wings_resources::{AwsConfiguration, ObjectStoreConfiguration, ObjectStoreName};

mod common;

#[tokio::test]
async fn test_object_store_roundtrip() {
    let cp = SqlControlPlane::new_in_memory().await;

    common::seed_tenant(&cp).await;

    let name = ObjectStoreName::parse("tenants/abcd/object-stores/xyz").unwrap();
    let config = ObjectStoreConfiguration::Aws(AwsConfiguration {
        bucket_name: "my-bucket".to_string(),
        access_key_id: "my-access-key".to_string(),
        secret_access_key: "my-secret-key".to_string(),
        prefix: None,
        region: None,
    });

    let back = cp
        .create_object_store(name.clone(), config.clone())
        .await
        .unwrap();

    assert_eq!(back.name, name);
    assert_eq!(back.object_store, config);
}

#[tokio::test]
async fn test_get_object_store() {
    let cp = SqlControlPlane::new_in_memory().await;

    common::seed_tenant(&cp).await;
    common::seed_object_store(&cp).await;

    let name = ObjectStoreName::parse("tenants/abcd/object-stores/xyz").unwrap();

    let back = cp.get_object_store(name.clone()).await.unwrap();

    assert_eq!(back.name, name);
}

#[tokio::test]
async fn test_get_object_store_fails_if_not_found() {
    let cp = SqlControlPlane::new_in_memory().await;

    common::seed_tenant(&cp).await;

    let name = ObjectStoreName::parse("tenants/abcd/object-stores/nonexistent").unwrap();

    let result = cp.get_object_store(name).await;

    assert!(matches!(result, Err(ClusterMetadataError::NotFound { .. })));
}

#[tokio::test]
async fn test_list_object_stores() {
    let cp = SqlControlPlane::new_in_memory().await;

    common::seed_tenant(&cp).await;
    common::seed_object_store(&cp).await;

    let tenant_name = wings_resources::TenantName::parse("tenants/abcd").unwrap();
    let request = ListObjectStoresRequest::new(tenant_name);

    let response = cp.list_object_stores(request).await.unwrap();

    assert_eq!(response.object_stores.len(), 1);
    assert_eq!(response.object_stores[0].name.id, "xyz");
    assert!(response.next_page_token.is_none());
}

#[tokio::test]
async fn test_list_object_stores_empty() {
    let cp = SqlControlPlane::new_in_memory().await;

    common::seed_tenant(&cp).await;

    let tenant_name = wings_resources::TenantName::parse("tenants/abcd").unwrap();
    let request = ListObjectStoresRequest::new(tenant_name);

    let response = cp.list_object_stores(request).await.unwrap();

    assert!(response.object_stores.is_empty());
    assert!(response.next_page_token.is_none());
}

#[tokio::test]
async fn test_delete_object_store() {
    let cp = SqlControlPlane::new_in_memory().await;

    common::seed_tenant(&cp).await;
    common::seed_object_store(&cp).await;

    let name = ObjectStoreName::parse("tenants/abcd/object-stores/xyz").unwrap();

    cp.delete_object_store(name.clone()).await.unwrap();

    let result = cp.get_object_store(name).await;
    assert!(matches!(result, Err(ClusterMetadataError::NotFound { .. })));
}

#[tokio::test]
async fn test_delete_object_store_fails_if_not_found() {
    let cp = SqlControlPlane::new_in_memory().await;

    common::seed_tenant(&cp).await;

    let name = ObjectStoreName::parse("tenants/abcd/object-stores/nonexistent").unwrap();

    let result = cp.delete_object_store(name).await;

    assert!(matches!(result, Err(ClusterMetadataError::NotFound { .. })));
}

#[tokio::test]
async fn test_delete_object_store_fails_if_used_by_namespace() {
    let cp = SqlControlPlane::new_in_memory().await;

    common::seed_tenant(&cp).await;
    common::seed_data_lake(&cp).await;
    common::seed_object_store(&cp).await;
    common::seed_namespace(&cp).await;

    let object_store_name = ObjectStoreName::parse("tenants/abcd/object-stores/xyz").unwrap();

    let result = cp.delete_object_store(object_store_name).await;

    assert!(matches!(
        result,
        Err(ClusterMetadataError::FailedPrecondition { .. })
    ));
}

#[tokio::test]
async fn test_create_object_store_fails_if_parent_tenant_doesnt_exist() {
    let cp = SqlControlPlane::new_in_memory().await;

    let name = ObjectStoreName::parse("tenants/abcd/object-stores/xyz").unwrap();
    let config = ObjectStoreConfiguration::Aws(AwsConfiguration {
        bucket_name: "my-bucket".to_string(),
        access_key_id: "my-access-key".to_string(),
        secret_access_key: "my-secret-key".to_string(),
        prefix: None,
        region: None,
    });

    let result = cp.create_object_store(name, config).await;

    assert!(matches!(result, Err(ClusterMetadataError::NotFound { .. })));
}
