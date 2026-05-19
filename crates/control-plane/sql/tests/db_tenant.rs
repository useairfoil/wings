use wings_control_plane_core::cluster_metadata::{
    ClusterMetadata, ClusterMetadataError, ListTenantsRequest,
};
use wings_control_plane_sql::SqlControlPlane;
use wings_resources::TenantName;

mod common;

#[tokio::test]
async fn test_tenant_roundtrip() {
    let cp = SqlControlPlane::new_in_memory().await;

    let name = TenantName::new_unchecked("abcd");
    let back = cp.create_tenant(name.clone()).await.unwrap();
    assert_eq!(back.name, name);

    // Test get_tenant
    let retrieved = cp.get_tenant(name.clone()).await.unwrap();
    assert_eq!(retrieved.name, name);

    // Test list_tenants
    let list_result = cp
        .list_tenants(ListTenantsRequest::default())
        .await
        .unwrap();
    assert_eq!(list_result.tenants.len(), 1);
    assert_eq!(list_result.tenants[0].name, name);
    assert!(list_result.next_page_token.is_none());
}

#[tokio::test]
async fn test_get_tenant_not_found() {
    let cp = SqlControlPlane::new_in_memory().await;

    let name = TenantName::new_unchecked("nonexistent");
    let result = cp.get_tenant(name).await;

    assert!(matches!(result, Err(ClusterMetadataError::NotFound { .. })));
}

#[tokio::test]
async fn test_list_tenants_pagination() {
    let cp = SqlControlPlane::new_in_memory().await;

    // Create multiple tenants
    for i in 0..5 {
        let name = TenantName::new_unchecked(format!("tenant{i}"));
        cp.create_tenant(name).await.unwrap();
    }

    // Test with page_size=2
    let request = ListTenantsRequest {
        page_size: Some(2),
        page_token: None,
    };
    let result = cp.list_tenants(request).await.unwrap();
    assert_eq!(result.tenants.len(), 2);
    assert!(result.next_page_token.is_some());

    // Get next page
    let request = ListTenantsRequest {
        page_size: Some(2),
        page_token: result.next_page_token,
    };
    let result = cp.list_tenants(request).await.unwrap();
    assert_eq!(result.tenants.len(), 2);
    assert!(result.next_page_token.is_some());

    // Get final page
    let request = ListTenantsRequest {
        page_size: Some(2),
        page_token: result.next_page_token,
    };
    let result = cp.list_tenants(request).await.unwrap();
    assert_eq!(result.tenants.len(), 1);
    assert!(result.next_page_token.is_none());
}

#[tokio::test]
async fn test_delete_tenant() {
    let cp = SqlControlPlane::new_in_memory().await;

    let name = TenantName::new_unchecked("delete-me");
    cp.create_tenant(name.clone()).await.unwrap();

    // Verify it exists
    let list_result = cp
        .list_tenants(ListTenantsRequest::default())
        .await
        .unwrap();
    assert_eq!(list_result.tenants.len(), 1);

    // Delete it
    cp.delete_tenant(name.clone()).await.unwrap();

    // Verify it's gone
    let list_result = cp
        .list_tenants(ListTenantsRequest::default())
        .await
        .unwrap();
    assert_eq!(list_result.tenants.len(), 0);

    // Verify get fails
    let result = cp.get_tenant(name).await;

    assert!(matches!(result, Err(ClusterMetadataError::NotFound { .. })));
}

#[tokio::test]
async fn test_delete_tenant_not_found() {
    let cp = SqlControlPlane::new_in_memory().await;

    let name = TenantName::new_unchecked("nonexistent");
    let result = cp.delete_tenant(name).await;

    assert!(matches!(result, Err(ClusterMetadataError::NotFound { .. })));
}

#[tokio::test]
async fn test_delete_tenant_fails_if_has_namespaces() {
    let cp = SqlControlPlane::new_in_memory().await;

    common::seed_tenant(&cp).await;
    common::seed_data_lake(&cp).await;
    common::seed_object_store(&cp).await;
    common::seed_namespace(&cp).await;

    let name = TenantName::new_unchecked("abcd");

    let result = cp.delete_tenant(name).await;

    assert!(matches!(
        result,
        Err(ClusterMetadataError::FailedPrecondition { .. })
    ));
}

#[tokio::test]
async fn test_delete_tenant_cascades_to_object_stores_and_data_lakes() {
    let cp = SqlControlPlane::new_in_memory().await;

    common::seed_tenant(&cp).await;
    common::seed_data_lake(&cp).await;
    common::seed_object_store(&cp).await;

    let tenant_name = TenantName::new_unchecked("abcd");
    let _data_lake_name =
        wings_resources::DataLakeName::parse("tenants/abcd/data-lakes/xyz").unwrap();
    let _object_store_name =
        wings_resources::ObjectStoreName::parse("tenants/abcd/object-stores/xyz").unwrap();

    // Verify they exist
    let lakes_response = cp
        .list_data_lakes(
            wings_control_plane_core::cluster_metadata::ListDataLakesRequest::new(
                tenant_name.clone(),
            ),
        )
        .await
        .unwrap();
    assert_eq!(lakes_response.data_lakes.len(), 1);

    let stores_response = cp
        .list_object_stores(
            wings_control_plane_core::cluster_metadata::ListObjectStoresRequest::new(
                tenant_name.clone(),
            ),
        )
        .await
        .unwrap();
    assert_eq!(stores_response.object_stores.len(), 1);

    // Delete tenant - should cascade to data_lakes and object_stores
    cp.delete_tenant(tenant_name.clone()).await.unwrap();

    // Verify tenant is gone
    let result = cp.get_tenant(tenant_name.clone()).await;
    assert!(result.is_err());

    // Verify data_lakes are gone (cascaded)
    let lakes_response = cp
        .list_data_lakes(
            wings_control_plane_core::cluster_metadata::ListDataLakesRequest::new(
                tenant_name.clone(),
            ),
        )
        .await
        .unwrap();
    assert!(lakes_response.data_lakes.is_empty());

    // Verify object_stores are gone (cascaded)
    let stores_response = cp
        .list_object_stores(
            wings_control_plane_core::cluster_metadata::ListObjectStoresRequest::new(
                tenant_name.clone(),
            ),
        )
        .await
        .unwrap();
    assert!(stores_response.object_stores.is_empty());
}
