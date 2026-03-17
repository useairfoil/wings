use wings_control_plane_core::cluster_metadata::ListTenantsRequest;
use wings_control_plane_sql::db::Error;
use wings_resources::TenantName;

mod common;

#[tokio::test]
async fn test_tenant_roundtrip() {
    let db = common::new_test_db().await;

    let name = TenantName::new_unchecked("abcd");
    let back = db.create_tenant(name.clone()).await.unwrap();
    assert_eq!(back.name, name);

    // Test get_tenant
    let retrieved = db.get_tenant(name.clone()).await.unwrap();
    assert_eq!(retrieved.name, name);

    // Test list_tenants
    let list_result = db
        .list_tenants(ListTenantsRequest::default())
        .await
        .unwrap();
    assert_eq!(list_result.tenants.len(), 1);
    assert_eq!(list_result.tenants[0].name, name);
    assert!(list_result.next_page_token.is_none());
}

#[tokio::test]
async fn test_get_tenant_not_found() {
    let db = common::new_test_db().await;

    let name = TenantName::new_unchecked("nonexistent");
    let result = db.get_tenant(name).await;

    assert!(matches!(
        result,
        Err(Error::NotFound {
            resource: "tenant",
            ..
        })
    ));
}

#[tokio::test]
async fn test_list_tenants_pagination() {
    let db = common::new_test_db().await;

    // Create multiple tenants
    for i in 0..5 {
        let name = TenantName::new_unchecked(&format!("tenant{i}"));
        db.create_tenant(name).await.unwrap();
    }

    // Test with page_size=2
    let request = ListTenantsRequest {
        page_size: Some(2),
        page_token: None,
    };
    let result = db.list_tenants(request).await.unwrap();
    assert_eq!(result.tenants.len(), 2);
    assert!(result.next_page_token.is_some());

    // Get next page
    let request = ListTenantsRequest {
        page_size: Some(2),
        page_token: result.next_page_token,
    };
    let result = db.list_tenants(request).await.unwrap();
    assert_eq!(result.tenants.len(), 2);
    assert!(result.next_page_token.is_some());

    // Get final page
    let request = ListTenantsRequest {
        page_size: Some(2),
        page_token: result.next_page_token,
    };
    let result = db.list_tenants(request).await.unwrap();
    assert_eq!(result.tenants.len(), 1);
    assert!(result.next_page_token.is_none());
}

#[tokio::test]
async fn test_delete_tenant() {
    let db = common::new_test_db().await;

    let name = TenantName::new_unchecked("delete-me");
    db.create_tenant(name.clone()).await.unwrap();

    // Verify it exists
    let list_result = db
        .list_tenants(ListTenantsRequest::default())
        .await
        .unwrap();
    assert_eq!(list_result.tenants.len(), 1);

    // Delete it
    db.delete_tenant(name.clone()).await.unwrap();

    // Verify it's gone
    let list_result = db
        .list_tenants(ListTenantsRequest::default())
        .await
        .unwrap();
    assert_eq!(list_result.tenants.len(), 0);

    // Verify get fails
    let result = db.get_tenant(name).await;

    assert!(matches!(
        result,
        Err(Error::NotFound {
            resource: "tenant",
            ..
        })
    ));
}

#[tokio::test]
async fn test_delete_tenant_not_found() {
    let db = common::new_test_db().await;

    let name = TenantName::new_unchecked("nonexistent");
    let result = db.delete_tenant(name).await;

    assert!(matches!(
        result,
        Err(Error::NotFound {
            resource: "tenant",
            ..
        })
    ));
}

#[tokio::test]
async fn test_delete_tenant_fails_if_has_namespaces() {
    let db = common::new_test_db().await;

    common::seed_tenant(&db).await;
    common::seed_data_lake(&db).await;
    common::seed_object_store(&db).await;
    common::seed_namespace(&db).await;

    let name = TenantName::new_unchecked("abcd");

    let result = db.delete_tenant(name).await;

    assert!(matches!(
        result,
        Err(Error::InvalidArgument {
            resource: "tenant",
            ..
        })
    ));
}

#[tokio::test]
async fn test_delete_tenant_cascades_to_object_stores_and_data_lakes() {
    let db = common::new_test_db().await;

    common::seed_tenant(&db).await;
    common::seed_data_lake(&db).await;
    common::seed_object_store(&db).await;

    let tenant_name = TenantName::new_unchecked("abcd");
    let _data_lake_name =
        wings_resources::DataLakeName::parse("tenants/abcd/data-lakes/xyz").unwrap();
    let _object_store_name =
        wings_resources::ObjectStoreName::parse("tenants/abcd/object-stores/xyz").unwrap();

    // Verify they exist
    let lakes_response = db
        .list_data_lakes(
            wings_control_plane_core::cluster_metadata::ListDataLakesRequest::new(
                tenant_name.clone(),
            ),
        )
        .await
        .unwrap();
    assert_eq!(lakes_response.data_lakes.len(), 1);

    let stores_response = db
        .list_object_stores(
            wings_control_plane_core::cluster_metadata::ListObjectStoresRequest::new(
                tenant_name.clone(),
            ),
        )
        .await
        .unwrap();
    assert_eq!(stores_response.object_stores.len(), 1);

    // Delete tenant - should cascade to data_lakes and object_stores
    db.delete_tenant(tenant_name.clone()).await.unwrap();

    // Verify tenant is gone
    let result = db.get_tenant(tenant_name.clone()).await;
    assert!(result.is_err());

    // Verify data_lakes are gone (cascaded)
    let lakes_response = db
        .list_data_lakes(
            wings_control_plane_core::cluster_metadata::ListDataLakesRequest::new(
                tenant_name.clone(),
            ),
        )
        .await
        .unwrap();
    assert!(lakes_response.data_lakes.is_empty());

    // Verify object_stores are gone (cascaded)
    let stores_response = db
        .list_object_stores(
            wings_control_plane_core::cluster_metadata::ListObjectStoresRequest::new(
                tenant_name.clone(),
            ),
        )
        .await
        .unwrap();
    assert!(stores_response.object_stores.is_empty());
}
