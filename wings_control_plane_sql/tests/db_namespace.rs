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
