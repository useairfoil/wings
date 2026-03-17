use wings_control_plane_core::cluster_metadata::ListTenantsRequest;
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
    assert!(result.is_err());
}

#[tokio::test]
async fn test_debug_list_tenants() {
    let db = common::new_test_db().await;

    // Create multiple tenants with delay to ensure different timestamps
    for i in 0..3 {
        let name = TenantName::new_unchecked(&format!("tenant{i}"));
        db.create_tenant(name).await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }

    // List all tenants first
    let all_tenants = db
        .list_tenants(ListTenantsRequest::default())
        .await
        .unwrap();
    println!("All tenants: {} tenants", all_tenants.tenants.len());
    for t in &all_tenants.tenants {
        println!("  - {}", t.name.id());
    }

    // Test with page_size=1
    let request = ListTenantsRequest {
        page_size: Some(1),
        page_token: None,
    };
    let result = db.list_tenants(request).await.unwrap();
    println!("\nFirst page: {} tenants", result.tenants.len());
    for t in &result.tenants {
        println!("  - {}", t.name.id());
    }
    println!("Next page token: {:?}", result.next_page_token);

    // Get next page
    let request = ListTenantsRequest {
        page_size: Some(1),
        page_token: result.next_page_token,
    };
    let result = db.list_tenants(request).await.unwrap();
    println!("\nSecond page: {} tenants", result.tenants.len());
    for t in &result.tenants {
        println!("  - {}", t.name.id());
    }
    println!("Next page token: {:?}", result.next_page_token);
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
    assert!(result.is_err());
}

#[tokio::test]
async fn test_delete_tenant_not_found() {
    let db = common::new_test_db().await;

    let name = TenantName::new_unchecked("nonexistent");
    let result = db.delete_tenant(name).await;
    assert!(result.is_err());
}
