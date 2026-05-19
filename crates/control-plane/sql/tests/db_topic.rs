use wings_control_plane_core::cluster_metadata::{
    ClusterMetadata, ClusterMetadataError, ListTablesRequest, TableView,
};
use wings_control_plane_sql::SqlControlPlane;
use wings_resources::{NamespaceName, TableName, TableOptions};
use wings_schema::{DataType, Field, SchemaBuilder};

mod common;

#[tokio::test]
async fn test_table_roundtrip() {
    let cp = SqlControlPlane::new_in_memory().await;

    common::seed_tenant(&cp).await;
    common::seed_data_lake(&cp).await;
    common::seed_object_store(&cp).await;
    common::seed_namespace(&cp).await;

    let name = TableName::parse("tenants/abcd/namespaces/xyz/tables/my-table").unwrap();
    let schema = SchemaBuilder::new(vec![
        Field::new("id", 1, DataType::Int64, false),
        Field::new("message", 2, DataType::Utf8, false),
    ])
    .build()
    .unwrap();
    let options = TableOptions::new(schema.clone());

    let back = cp
        .create_table(name.clone(), options.clone())
        .await
        .unwrap();

    assert_eq!(back.name, name);
    assert_eq!(back.schema.fields.len(), 2);
    assert_eq!(back.partition_key, None);
    assert_eq!(back.description, None);
}

#[tokio::test]
async fn test_table_with_partition_key() {
    let cp = SqlControlPlane::new_in_memory().await;

    common::seed_tenant(&cp).await;
    common::seed_data_lake(&cp).await;
    common::seed_object_store(&cp).await;
    common::seed_namespace(&cp).await;

    let name = TableName::parse("tenants/abcd/namespaces/xyz/tables/my-table").unwrap();
    let schema = SchemaBuilder::new(vec![
        Field::new("id", 1, DataType::Int64, false),
        Field::new("message", 2, DataType::Utf8, false),
    ])
    .build()
    .unwrap();
    let options = TableOptions::new_with_partition_key(schema.clone(), Some(1));

    let back = cp
        .create_table(name.clone(), options.clone())
        .await
        .unwrap();

    assert_eq!(back.name, name);
    assert_eq!(back.partition_key, Some(1));
}

#[tokio::test]
async fn test_get_table() {
    let cp = SqlControlPlane::new_in_memory().await;

    common::seed_tenant(&cp).await;
    common::seed_data_lake(&cp).await;
    common::seed_object_store(&cp).await;
    common::seed_namespace(&cp).await;
    common::seed_table(&cp).await;

    let name = TableName::parse("tenants/abcd/namespaces/xyz/tables/my-table").unwrap();

    let back = cp.get_table(name.clone(), TableView::Basic).await.unwrap();

    assert_eq!(back.name, name);
    assert_eq!(back.schema.fields.len(), 1);
}

#[tokio::test]
async fn test_get_table_fails_if_not_found() {
    let cp = SqlControlPlane::new_in_memory().await;

    common::seed_tenant(&cp).await;
    common::seed_data_lake(&cp).await;
    common::seed_object_store(&cp).await;
    common::seed_namespace(&cp).await;

    let name = TableName::parse("tenants/abcd/namespaces/xyz/tables/nonexistent").unwrap();

    let result = cp.get_table(name, TableView::Basic).await;

    assert!(matches!(result, Err(ClusterMetadataError::NotFound { .. })));
}

#[tokio::test]
async fn test_list_tables() {
    let cp = SqlControlPlane::new_in_memory().await;

    common::seed_tenant(&cp).await;
    common::seed_data_lake(&cp).await;
    common::seed_object_store(&cp).await;
    common::seed_namespace(&cp).await;
    common::seed_table(&cp).await;

    let namespace_name = NamespaceName::parse("tenants/abcd/namespaces/xyz").unwrap();
    let request = ListTablesRequest::new(namespace_name);

    let response = cp.list_tables(request).await.unwrap();

    assert_eq!(response.tables.len(), 1);
    assert_eq!(response.tables[0].name.id, "my-table");
    assert!(response.next_page_token.is_none());
}

#[tokio::test]
async fn test_list_tables_empty() {
    let cp = SqlControlPlane::new_in_memory().await;

    common::seed_tenant(&cp).await;
    common::seed_data_lake(&cp).await;
    common::seed_object_store(&cp).await;
    common::seed_namespace(&cp).await;

    let namespace_name = NamespaceName::parse("tenants/abcd/namespaces/xyz").unwrap();
    let request = ListTablesRequest::new(namespace_name);

    let response = cp.list_tables(request).await.unwrap();

    assert!(response.tables.is_empty());
    assert!(response.next_page_token.is_none());
}

#[tokio::test]
async fn test_delete_table() {
    let cp = SqlControlPlane::new_in_memory().await;

    common::seed_tenant(&cp).await;
    common::seed_data_lake(&cp).await;
    common::seed_object_store(&cp).await;
    common::seed_namespace(&cp).await;
    common::seed_table(&cp).await;

    let name = TableName::parse("tenants/abcd/namespaces/xyz/tables/my-table").unwrap();

    cp.delete_table(name.clone(), false).await.unwrap();

    let result = cp.get_table(name, TableView::Basic).await;
    assert!(matches!(result, Err(ClusterMetadataError::NotFound { .. })));
}

#[tokio::test]
async fn test_delete_table_fails_if_not_found() {
    let cp = SqlControlPlane::new_in_memory().await;

    common::seed_tenant(&cp).await;
    common::seed_data_lake(&cp).await;
    common::seed_object_store(&cp).await;
    common::seed_namespace(&cp).await;

    let name = TableName::parse("tenants/abcd/namespaces/xyz/tables/nonexistent").unwrap();

    let result = cp.delete_table(name, false).await;

    assert!(matches!(result, Err(ClusterMetadataError::NotFound { .. })));
}

#[tokio::test]
async fn test_create_table_fails_if_parent_namespace_doesnt_exist() {
    let cp = SqlControlPlane::new_in_memory().await;

    common::seed_tenant(&cp).await;

    let name = TableName::parse("tenants/abcd/namespaces/nonexistent/tables/my-table").unwrap();
    let schema = SchemaBuilder::new(vec![Field::new("message", 1, DataType::Utf8, false)])
        .build()
        .unwrap();
    let options = TableOptions::new(schema);

    let result = cp.create_table(name, options).await;

    assert!(matches!(result, Err(ClusterMetadataError::NotFound { .. })));
}

#[tokio::test]
async fn test_create_table_fails_if_already_exists() {
    let cp = SqlControlPlane::new_in_memory().await;

    common::seed_tenant(&cp).await;
    common::seed_data_lake(&cp).await;
    common::seed_object_store(&cp).await;
    common::seed_namespace(&cp).await;
    common::seed_table(&cp).await;

    let name = TableName::parse("tenants/abcd/namespaces/xyz/tables/my-table").unwrap();
    let schema = SchemaBuilder::new(vec![Field::new("message", 1, DataType::Utf8, false)])
        .build()
        .unwrap();
    let options = TableOptions::new(schema);

    let result = cp.create_table(name, options).await;

    assert!(matches!(
        result,
        Err(ClusterMetadataError::AlreadyExists { .. })
    ));
}

#[tokio::test]
async fn test_create_table_fails_if_partition_key_not_in_schema() {
    let cp = SqlControlPlane::new_in_memory().await;

    common::seed_tenant(&cp).await;
    common::seed_data_lake(&cp).await;
    common::seed_object_store(&cp).await;
    common::seed_namespace(&cp).await;

    let name = TableName::parse("tenants/abcd/namespaces/xyz/tables/my-table").unwrap();
    let schema = SchemaBuilder::new(vec![Field::new("message", 1, DataType::Utf8, false)])
        .build()
        .unwrap();
    // Try to use partition key 999 which doesn't exist
    let options = TableOptions::new_with_partition_key(schema, Some(999));

    let result = cp.create_table(name, options).await;

    assert!(matches!(
        result,
        Err(ClusterMetadataError::InvalidArgument { .. })
    ));
}

#[tokio::test]
async fn test_create_table_with_description() {
    let cp = SqlControlPlane::new_in_memory().await;

    common::seed_tenant(&cp).await;
    common::seed_data_lake(&cp).await;
    common::seed_object_store(&cp).await;
    common::seed_namespace(&cp).await;

    let name = TableName::parse("tenants/abcd/namespaces/xyz/tables/my-table").unwrap();
    let schema = SchemaBuilder::new(vec![Field::new("message", 1, DataType::Utf8, false)])
        .build()
        .unwrap();
    let options = TableOptions::new(schema).with_description("My test table");

    let back = cp
        .create_table(name.clone(), options.clone())
        .await
        .unwrap();

    assert_eq!(back.description, Some("My test table".to_string()));
}

#[tokio::test]
async fn test_create_table_fails_with_invalid_compaction() {
    let cp = SqlControlPlane::new_in_memory().await;

    common::seed_tenant(&cp).await;
    common::seed_data_lake(&cp).await;
    common::seed_object_store(&cp).await;
    common::seed_namespace(&cp).await;

    let name = TableName::parse("tenants/abcd/namespaces/xyz/tables/my-table").unwrap();
    let schema = SchemaBuilder::new(vec![Field::new("message", 1, DataType::Utf8, false)])
        .build()
        .unwrap();

    // Create compaction with freshness < 1 minute (invalid)
    let invalid_compaction = wings_resources::CompactionConfiguration {
        freshness: std::time::Duration::from_secs(30), // 30 seconds is invalid
        ttl: None,
        target_file_size: bytesize::ByteSize::mb(512),
    };
    let options = TableOptions::new(schema).with_compaction(invalid_compaction);

    let result = cp.create_table(name, options).await;

    assert!(matches!(
        result,
        Err(ClusterMetadataError::InvalidArgument { .. })
    ));
}
