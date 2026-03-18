use wings_control_plane_sql::db::Error;
use wings_resources::{DataLakeName, NamespaceName, NamespaceOptions, ObjectStoreName};

mod common;

#[tokio::test]
async fn test_namespace_roundtrip() {
    let db = common::new_test_db().await;

    common::seed_tenant(&db).await;
    common::seed_data_lake(&db).await;
    common::seed_object_store(&db).await;

    let name = NamespaceName::parse("tenants/abcd/namespaces/xyz").unwrap();
    let object_store = ObjectStoreName::parse("tenants/abcd/object-stores/xyz").unwrap();
    let data_lake = DataLakeName::parse("tenants/abcd/data-lakes/xyz").unwrap();
    let options = NamespaceOptions::new(object_store, data_lake);

    let back = db
        .create_namespace(name.clone(), options.clone())
        .await
        .unwrap();

    assert_eq!(back.name, name);
    assert_eq!(back.data_lake, options.data_lake);
    assert_eq!(back.object_store, options.object_store);
    assert_eq!(back.flush_interval, options.flush_interval);
    assert_eq!(back.flush_size, options.flush_size);
}

#[tokio::test]
async fn test_get_namespace() {
    let db = common::new_test_db().await;

    common::seed_tenant(&db).await;
    common::seed_data_lake(&db).await;
    common::seed_object_store(&db).await;

    let name = NamespaceName::parse("tenants/abcd/namespaces/xyz").unwrap();
    let object_store = ObjectStoreName::parse("tenants/abcd/object-stores/xyz").unwrap();
    let data_lake = DataLakeName::parse("tenants/abcd/data-lakes/xyz").unwrap();
    let options = NamespaceOptions::new(object_store, data_lake);

    db.create_namespace(name.clone(), options).await.unwrap();

    let back = db.get_namespace(name.clone()).await.unwrap();

    assert_eq!(back.name, name);
}

#[tokio::test]
async fn test_get_namespace_fails_if_not_found() {
    let db = common::new_test_db().await;

    common::seed_tenant(&db).await;

    let name = NamespaceName::parse("tenants/abcd/namespaces/nonexistent").unwrap();

    let result = db.get_namespace(name).await;

    assert!(matches!(
        result,
        Err(Error::NotFound {
            resource: "namespace",
            ..
        })
    ));
}

#[tokio::test]
async fn test_list_namespaces() {
    let db = common::new_test_db().await;

    common::seed_tenant(&db).await;
    common::seed_data_lake(&db).await;
    common::seed_object_store(&db).await;

    let name = NamespaceName::parse("tenants/abcd/namespaces/xyz").unwrap();
    let object_store = ObjectStoreName::parse("tenants/abcd/object-stores/xyz").unwrap();
    let data_lake = DataLakeName::parse("tenants/abcd/data-lakes/xyz").unwrap();
    let options = NamespaceOptions::new(object_store, data_lake);

    db.create_namespace(name, options).await.unwrap();

    let tenant_name = wings_resources::TenantName::parse("tenants/abcd").unwrap();
    let request =
        wings_control_plane_core::cluster_metadata::ListNamespacesRequest::new(tenant_name);

    let response = db.list_namespaces(request).await.unwrap();

    assert_eq!(response.namespaces.len(), 1);
    assert_eq!(response.namespaces[0].name.id, "xyz");
    assert!(response.next_page_token.is_none());
}

#[tokio::test]
async fn test_list_namespaces_empty() {
    let db = common::new_test_db().await;

    common::seed_tenant(&db).await;

    let tenant_name = wings_resources::TenantName::parse("tenants/abcd").unwrap();
    let request =
        wings_control_plane_core::cluster_metadata::ListNamespacesRequest::new(tenant_name);

    let response = db.list_namespaces(request).await.unwrap();

    assert!(response.namespaces.is_empty());
    assert!(response.next_page_token.is_none());
}

#[tokio::test]
async fn test_delete_namespace() {
    let db = common::new_test_db().await;

    common::seed_tenant(&db).await;
    common::seed_data_lake(&db).await;
    common::seed_object_store(&db).await;

    let name = NamespaceName::parse("tenants/abcd/namespaces/xyz").unwrap();
    let object_store = ObjectStoreName::parse("tenants/abcd/object-stores/xyz").unwrap();
    let data_lake = DataLakeName::parse("tenants/abcd/data-lakes/xyz").unwrap();
    let options = NamespaceOptions::new(object_store, data_lake);

    db.create_namespace(name.clone(), options).await.unwrap();

    db.delete_namespace(name.clone()).await.unwrap();

    let result = db.get_namespace(name).await;
    assert!(matches!(
        result,
        Err(Error::NotFound {
            resource: "namespace",
            ..
        })
    ));
}

#[tokio::test]
async fn test_delete_namespace_fails_if_not_found() {
    let db = common::new_test_db().await;

    common::seed_tenant(&db).await;

    let name = NamespaceName::parse("tenants/abcd/namespaces/nonexistent").unwrap();

    let result = db.delete_namespace(name).await;

    assert!(matches!(
        result,
        Err(Error::NotFound {
            resource: "namespace",
            ..
        })
    ));
}

#[tokio::test]
async fn test_create_namespace_fails_if_parent_tenant_doesnt_exist() {
    let db = common::new_test_db().await;

    let name = NamespaceName::parse("tenants/abcd/namespaces/xyz").unwrap();
    let object_store = ObjectStoreName::parse("tenants/abcd/object-stores/xyz").unwrap();
    let data_lake = DataLakeName::parse("tenants/abcd/data-lakes/xyz").unwrap();
    let options = NamespaceOptions::new(object_store, data_lake);

    let result = db.create_namespace(name.clone(), options.clone()).await;

    assert!(matches!(
        result,
        Err(Error::NotFound {
            resource: "tenant",
            ..
        })
    ));
}

#[tokio::test]
async fn test_create_namespace_fails_if_object_store_doesnt_exist() {
    let db = common::new_test_db().await;

    common::seed_tenant(&db).await;
    common::seed_data_lake(&db).await;

    let name = NamespaceName::parse("tenants/abcd/namespaces/xyz").unwrap();
    let object_store = ObjectStoreName::parse("tenants/abcd/object-stores/xyz").unwrap();
    let data_lake = DataLakeName::parse("tenants/abcd/data-lakes/xyz").unwrap();
    let options = NamespaceOptions::new(object_store, data_lake);

    let result = db.create_namespace(name.clone(), options.clone()).await;

    assert!(matches!(
        result,
        Err(Error::NotFound {
            resource: "object-store",
            ..
        })
    ));
}

#[tokio::test]
async fn test_create_namespace_fails_if_data_lake_doesnt_exist() {
    let db = common::new_test_db().await;

    common::seed_tenant(&db).await;
    common::seed_object_store(&db).await;

    let name = NamespaceName::parse("tenants/abcd/namespaces/xyz").unwrap();
    let object_store = ObjectStoreName::parse("tenants/abcd/object-stores/xyz").unwrap();
    let data_lake = DataLakeName::parse("tenants/abcd/data-lakes/xyz").unwrap();
    let options = NamespaceOptions::new(object_store, data_lake);

    let result = db.create_namespace(name.clone(), options.clone()).await;

    assert!(matches!(
        result,
        Err(Error::NotFound {
            resource: "data-lake",
            ..
        })
    ));
}

#[tokio::test]
async fn test_create_namespace_fails_if_object_store_parent_mismatch() {
    let db = common::new_test_db().await;

    common::seed_tenant(&db).await;
    common::seed_data_lake(&db).await;
    common::seed_object_store(&db).await;

    let name = NamespaceName::parse("tenants/abcd/namespaces/xyz").unwrap();
    let object_store = ObjectStoreName::parse("tenants/missing/object-stores/xyz").unwrap();
    let data_lake = DataLakeName::parse("tenants/abcd/data-lakes/xyz").unwrap();
    let options = NamespaceOptions::new(object_store, data_lake);

    let result = db.create_namespace(name.clone(), options.clone()).await;

    assert!(matches!(
        result,
        Err(Error::InvalidArgument {
            resource: "namespace",
            ..
        })
    ));
}

#[tokio::test]
async fn test_create_namespace_fails_if_data_lake_parent_mismatch() {
    let db = common::new_test_db().await;

    common::seed_tenant(&db).await;
    common::seed_data_lake(&db).await;
    common::seed_object_store(&db).await;

    let name = NamespaceName::parse("tenants/abcd/namespaces/xyz").unwrap();
    let object_store = ObjectStoreName::parse("tenants/abcd/object-stores/xyz").unwrap();
    let data_lake = DataLakeName::parse("tenants/missing/data-lakes/xyz").unwrap();
    let options = NamespaceOptions::new(object_store, data_lake);

    let result = db.create_namespace(name.clone(), options.clone()).await;

    assert!(matches!(
        result,
        Err(Error::InvalidArgument {
            resource: "namespace",
            ..
        })
    ));
}

#[tokio::test]
async fn test_delete_namespace_fails_if_has_topics() {
    let db = common::new_test_db().await;

    common::seed_tenant(&db).await;
    common::seed_data_lake(&db).await;
    common::seed_object_store(&db).await;
    common::seed_namespace(&db).await;
    common::seed_topic(&db).await;

    let name = NamespaceName::parse("tenants/abcd/namespaces/xyz").unwrap();

    let result = db.delete_namespace(name).await;

    assert!(matches!(
        result,
        Err(Error::InvalidArgument {
            resource: "namespace",
            ..
        })
    ));
}
