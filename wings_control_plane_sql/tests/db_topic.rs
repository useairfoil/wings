use wings_control_plane_core::cluster_metadata::{ListTopicsRequest, TopicView};
use wings_control_plane_sql::db::Error;
use wings_resources::{NamespaceName, TopicName, TopicOptions};
use wings_schema::{DataType, Field, SchemaBuilder};

mod common;

#[tokio::test]
async fn test_topic_roundtrip() {
    let db = common::new_test_db().await;

    common::seed_tenant(&db).await;
    common::seed_data_lake(&db).await;
    common::seed_object_store(&db).await;
    common::seed_namespace(&db).await;

    let name = TopicName::parse("tenants/abcd/namespaces/xyz/topics/my-topic").unwrap();
    let schema = SchemaBuilder::new(vec![
        Field::new("id", 1, DataType::Int64, false),
        Field::new("message", 2, DataType::Utf8, false),
    ])
    .build()
    .unwrap();
    let options = TopicOptions::new(schema.clone());

    let back = db
        .create_topic(name.clone(), options.clone())
        .await
        .unwrap();

    assert_eq!(back.name, name);
    assert_eq!(back.schema.fields.len(), 2);
    assert_eq!(back.partition_key, None);
    assert_eq!(back.description, None);
}

#[tokio::test]
async fn test_topic_with_partition_key() {
    let db = common::new_test_db().await;

    common::seed_tenant(&db).await;
    common::seed_data_lake(&db).await;
    common::seed_object_store(&db).await;
    common::seed_namespace(&db).await;

    let name = TopicName::parse("tenants/abcd/namespaces/xyz/topics/my-topic").unwrap();
    let schema = SchemaBuilder::new(vec![
        Field::new("id", 1, DataType::Int64, false),
        Field::new("message", 2, DataType::Utf8, false),
    ])
    .build()
    .unwrap();
    let options = TopicOptions::new_with_partition_key(schema.clone(), Some(1));

    let back = db
        .create_topic(name.clone(), options.clone())
        .await
        .unwrap();

    assert_eq!(back.name, name);
    assert_eq!(back.partition_key, Some(1));
}

#[tokio::test]
async fn test_get_topic() {
    let db = common::new_test_db().await;

    common::seed_tenant(&db).await;
    common::seed_data_lake(&db).await;
    common::seed_object_store(&db).await;
    common::seed_namespace(&db).await;
    common::seed_topic(&db).await;

    let name = TopicName::parse("tenants/abcd/namespaces/xyz/topics/my-topic").unwrap();

    let back = db.get_topic(name.clone(), TopicView::Basic).await.unwrap();

    assert_eq!(back.name, name);
    assert_eq!(back.schema.fields.len(), 1);
}

#[tokio::test]
async fn test_get_topic_fails_if_not_found() {
    let db = common::new_test_db().await;

    common::seed_tenant(&db).await;
    common::seed_data_lake(&db).await;
    common::seed_object_store(&db).await;
    common::seed_namespace(&db).await;

    let name = TopicName::parse("tenants/abcd/namespaces/xyz/topics/nonexistent").unwrap();

    let result = db.get_topic(name, TopicView::Basic).await;

    assert!(matches!(
        result,
        Err(Error::NotFound {
            resource: "topic",
            ..
        })
    ));
}

#[tokio::test]
async fn test_list_topics() {
    let db = common::new_test_db().await;

    common::seed_tenant(&db).await;
    common::seed_data_lake(&db).await;
    common::seed_object_store(&db).await;
    common::seed_namespace(&db).await;
    common::seed_topic(&db).await;

    let namespace_name = NamespaceName::parse("tenants/abcd/namespaces/xyz").unwrap();
    let request = ListTopicsRequest::new(namespace_name);

    let response = db.list_topics(request).await.unwrap();

    assert_eq!(response.topics.len(), 1);
    assert_eq!(response.topics[0].name.id, "my-topic");
    assert!(response.next_page_token.is_none());
}

#[tokio::test]
async fn test_list_topics_empty() {
    let db = common::new_test_db().await;

    common::seed_tenant(&db).await;
    common::seed_data_lake(&db).await;
    common::seed_object_store(&db).await;
    common::seed_namespace(&db).await;

    let namespace_name = NamespaceName::parse("tenants/abcd/namespaces/xyz").unwrap();
    let request = ListTopicsRequest::new(namespace_name);

    let response = db.list_topics(request).await.unwrap();

    assert!(response.topics.is_empty());
    assert!(response.next_page_token.is_none());
}

#[tokio::test]
async fn test_delete_topic() {
    let db = common::new_test_db().await;

    common::seed_tenant(&db).await;
    common::seed_data_lake(&db).await;
    common::seed_object_store(&db).await;
    common::seed_namespace(&db).await;
    common::seed_topic(&db).await;

    let name = TopicName::parse("tenants/abcd/namespaces/xyz/topics/my-topic").unwrap();

    db.delete_topic(name.clone(), false).await.unwrap();

    let result = db.get_topic(name, TopicView::Basic).await;
    assert!(matches!(
        result,
        Err(Error::NotFound {
            resource: "topic",
            ..
        })
    ));
}

#[tokio::test]
async fn test_delete_topic_fails_if_not_found() {
    let db = common::new_test_db().await;

    common::seed_tenant(&db).await;
    common::seed_data_lake(&db).await;
    common::seed_object_store(&db).await;
    common::seed_namespace(&db).await;

    let name = TopicName::parse("tenants/abcd/namespaces/xyz/topics/nonexistent").unwrap();

    let result = db.delete_topic(name, false).await;

    assert!(matches!(
        result,
        Err(Error::NotFound {
            resource: "topic",
            ..
        })
    ));
}

#[tokio::test]
async fn test_create_topic_fails_if_parent_namespace_doesnt_exist() {
    let db = common::new_test_db().await;

    common::seed_tenant(&db).await;

    let name = TopicName::parse("tenants/abcd/namespaces/nonexistent/topics/my-topic").unwrap();
    let schema = SchemaBuilder::new(vec![Field::new("message", 1, DataType::Utf8, false)])
        .build()
        .unwrap();
    let options = TopicOptions::new(schema);

    let result = db.create_topic(name, options).await;

    assert!(matches!(
        result,
        Err(Error::NotFound {
            resource: "namespace",
            ..
        })
    ));
}

#[tokio::test]
async fn test_create_topic_fails_if_already_exists() {
    let db = common::new_test_db().await;

    common::seed_tenant(&db).await;
    common::seed_data_lake(&db).await;
    common::seed_object_store(&db).await;
    common::seed_namespace(&db).await;
    common::seed_topic(&db).await;

    let name = TopicName::parse("tenants/abcd/namespaces/xyz/topics/my-topic").unwrap();
    let schema = SchemaBuilder::new(vec![Field::new("message", 1, DataType::Utf8, false)])
        .build()
        .unwrap();
    let options = TopicOptions::new(schema);

    let result = db.create_topic(name, options).await;

    assert!(matches!(
        result,
        Err(Error::AlreadyExists {
            resource: "topic",
            ..
        })
    ));
}

#[tokio::test]
async fn test_create_topic_fails_if_partition_key_not_in_schema() {
    let db = common::new_test_db().await;

    common::seed_tenant(&db).await;
    common::seed_data_lake(&db).await;
    common::seed_object_store(&db).await;
    common::seed_namespace(&db).await;

    let name = TopicName::parse("tenants/abcd/namespaces/xyz/topics/my-topic").unwrap();
    let schema = SchemaBuilder::new(vec![Field::new("message", 1, DataType::Utf8, false)])
        .build()
        .unwrap();
    // Try to use partition key 999 which doesn't exist
    let options = TopicOptions::new_with_partition_key(schema, Some(999));

    let result = db.create_topic(name, options).await;

    assert!(matches!(
        result,
        Err(Error::InvalidArgument {
            resource: "topic",
            ..
        })
    ));
}

#[tokio::test]
async fn test_create_topic_with_description() {
    let db = common::new_test_db().await;

    common::seed_tenant(&db).await;
    common::seed_data_lake(&db).await;
    common::seed_object_store(&db).await;
    common::seed_namespace(&db).await;

    let name = TopicName::parse("tenants/abcd/namespaces/xyz/topics/my-topic").unwrap();
    let schema = SchemaBuilder::new(vec![Field::new("message", 1, DataType::Utf8, false)])
        .build()
        .unwrap();
    let options = TopicOptions::new(schema).with_description("My test topic");

    let back = db
        .create_topic(name.clone(), options.clone())
        .await
        .unwrap();

    assert_eq!(back.description, Some("My test topic".to_string()));
}

#[tokio::test]
async fn test_create_topic_fails_with_invalid_compaction() {
    let db = common::new_test_db().await;

    common::seed_tenant(&db).await;
    common::seed_data_lake(&db).await;
    common::seed_object_store(&db).await;
    common::seed_namespace(&db).await;

    let name = TopicName::parse("tenants/abcd/namespaces/xyz/topics/my-topic").unwrap();
    let schema = SchemaBuilder::new(vec![Field::new("message", 1, DataType::Utf8, false)])
        .build()
        .unwrap();

    // Create compaction with freshness < 1 minute (invalid)
    let invalid_compaction = wings_resources::CompactionConfiguration {
        freshness: std::time::Duration::from_secs(30), // 30 seconds is invalid
        ttl: None,
        target_file_size: bytesize::ByteSize::mb(512),
    };
    let options = TopicOptions::new(schema).with_compaction(invalid_compaction);

    let result = db.create_topic(name, options).await;

    assert!(matches!(
        result,
        Err(Error::InvalidArgument {
            resource: "topic",
            ..
        })
    ));
}
