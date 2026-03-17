use wings_control_plane_sql::db::Error;
use wings_resources::{AwsConfiguration, ObjectStoreConfiguration, ObjectStoreName};

mod common;

#[tokio::test]
async fn test_object_store_roundtrip() {
    let db = common::new_test_db().await;

    common::seed_tenant(&db).await;

    let name = ObjectStoreName::parse("tenants/abcd/object-stores/xyz").unwrap();
    let config = ObjectStoreConfiguration::Aws(AwsConfiguration {
        bucket_name: "my-bucket".to_string(),
        access_key_id: "my-access-key".to_string(),
        secret_access_key: "my-secret-key".to_string(),
        prefix: None,
        region: None,
    });

    let back = db
        .create_object_store(name.clone(), config.clone())
        .await
        .unwrap();

    assert_eq!(back.name, name);
    assert_eq!(back.object_store, config);
}

#[tokio::test]
async fn test_get_object_store() {
    let db = common::new_test_db().await;

    common::seed_tenant(&db).await;
    common::seed_object_store(&db).await;

    let name = ObjectStoreName::parse("tenants/abcd/object-stores/xyz").unwrap();

    let back = db.get_object_store(name.clone()).await.unwrap();

    assert_eq!(back.name, name);
}

#[tokio::test]
async fn test_get_object_store_fails_if_not_found() {
    let db = common::new_test_db().await;

    common::seed_tenant(&db).await;

    let name = ObjectStoreName::parse("tenants/abcd/object-stores/nonexistent").unwrap();

    let result = db.get_object_store(name).await;

    assert!(matches!(
        result,
        Err(Error::NotFound {
            resource: "object-store",
            ..
        })
    ));
}

#[tokio::test]
async fn test_list_object_stores() {
    let db = common::new_test_db().await;

    common::seed_tenant(&db).await;
    common::seed_object_store(&db).await;

    let tenant_name = wings_resources::TenantName::parse("tenants/abcd").unwrap();
    let request =
        wings_control_plane_core::cluster_metadata::ListObjectStoresRequest::new(tenant_name);

    let response = db.list_object_stores(request).await.unwrap();

    assert_eq!(response.object_stores.len(), 1);
    assert_eq!(response.object_stores[0].name.id, "xyz");
    assert!(response.next_page_token.is_none());
}

#[tokio::test]
async fn test_list_object_stores_empty() {
    let db = common::new_test_db().await;

    common::seed_tenant(&db).await;

    let tenant_name = wings_resources::TenantName::parse("tenants/abcd").unwrap();
    let request =
        wings_control_plane_core::cluster_metadata::ListObjectStoresRequest::new(tenant_name);

    let response = db.list_object_stores(request).await.unwrap();

    assert!(response.object_stores.is_empty());
    assert!(response.next_page_token.is_none());
}

#[tokio::test]
async fn test_delete_object_store() {
    let db = common::new_test_db().await;

    common::seed_tenant(&db).await;
    common::seed_object_store(&db).await;

    let name = ObjectStoreName::parse("tenants/abcd/object-stores/xyz").unwrap();

    db.delete_object_store(name.clone()).await.unwrap();

    let result = db.get_object_store(name).await;
    assert!(matches!(
        result,
        Err(Error::NotFound {
            resource: "object-store",
            ..
        })
    ));
}

#[tokio::test]
async fn test_delete_object_store_fails_if_not_found() {
    let db = common::new_test_db().await;

    common::seed_tenant(&db).await;

    let name = ObjectStoreName::parse("tenants/abcd/object-stores/nonexistent").unwrap();

    let result = db.delete_object_store(name).await;

    assert!(matches!(
        result,
        Err(Error::NotFound {
            resource: "object-store",
            ..
        })
    ));
}

#[tokio::test]
async fn test_delete_object_store_fails_if_used_by_namespace() {
    let db = common::new_test_db().await;

    common::seed_tenant(&db).await;
    common::seed_data_lake(&db).await;
    common::seed_object_store(&db).await;
    common::seed_namespace(&db).await;

    let object_store_name = ObjectStoreName::parse("tenants/abcd/object-stores/xyz").unwrap();

    let result = db.delete_object_store(object_store_name).await;

    assert!(matches!(
        result,
        Err(Error::InvalidArgument {
            resource: "object-store",
            ..
        })
    ));
}

#[tokio::test]
async fn test_create_object_store_fails_if_parent_tenant_doesnt_exist() {
    let db = common::new_test_db().await;

    let name = ObjectStoreName::parse("tenants/abcd/object-stores/xyz").unwrap();
    let config = ObjectStoreConfiguration::Aws(AwsConfiguration {
        bucket_name: "my-bucket".to_string(),
        access_key_id: "my-access-key".to_string(),
        secret_access_key: "my-secret-key".to_string(),
        prefix: None,
        region: None,
    });

    let result = db.create_object_store(name, config).await;

    assert!(matches!(
        result,
        Err(Error::NotFound {
            resource: "tenant",
            ..
        })
    ));
}
