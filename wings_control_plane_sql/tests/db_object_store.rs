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
