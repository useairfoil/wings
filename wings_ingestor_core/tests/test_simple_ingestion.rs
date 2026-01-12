use std::{sync::Arc, time::Duration};

use common::{create_batch_ingestor, initialize_test_namespace};
use datafusion::common::{
    arrow::{array::RecordBatch, datatypes::DataType},
    create_array, record_batch,
};
use wings_control_plane::{
    cluster_metadata::ClusterMetadata,
    resources::{Namespace, Topic, TopicName, TopicOptions},
    schema::{Field, Schema},
};
use wings_ingestor_core::{Result, WriteBatchError, WriteBatchRequest};

mod common;

fn simple_ingestion_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", 0, DataType::Int32, false),
        Field::new("name", 1, DataType::Utf8, false),
        Field::new("age", 2, DataType::Int32, false),
    ])
}

fn simple_ingestion_records() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(simple_ingestion_schema().into()),
        vec![
            create_array!(Int32, vec![1, 2, 3]),
            create_array!(Utf8, vec!["Alice", "Bob", "Charlie"]),
            create_array!(Int32, vec![25, 30, 35]),
        ],
    )
    .expect("failed to create batch")
}

async fn initialize_test_topic(
    cluster_meta: &Arc<dyn ClusterMetadata>,
) -> (Arc<Namespace>, Arc<Topic>) {
    let namespace = initialize_test_namespace(cluster_meta).await;

    let topic_name = TopicName::new_unchecked("simple_ingestion", namespace.name.clone());
    let schema = simple_ingestion_schema();
    let topic = cluster_meta
        .create_topic(topic_name, TopicOptions::new(schema))
        .await
        .expect("create_topic");

    (namespace, topic.into())
}

#[tokio::test]
async fn test_simple_ingestion() -> Result<()> {
    let (ing_fut, client, admin, ct) = create_batch_ingestor();
    let (namespace, topic) = initialize_test_topic(&admin).await;
    let ct_guard = ct.drop_guard();

    tokio::time::pause();

    let write_fut = client.write(WriteBatchRequest {
        namespace: namespace.clone(),
        topic: topic.clone(),
        partition: None,
        records: simple_ingestion_records(),
        timestamp: None,
    });

    tokio::time::advance(Duration::from_secs(2)).await;

    let write_info = write_fut.await.expect("write failed");
    assert_eq!(0, write_info.start_offset);
    assert_eq!(2, write_info.end_offset);

    let first_write_fut = client.write(WriteBatchRequest {
        namespace: namespace.clone(),
        topic: topic.clone(),
        partition: None,
        records: simple_ingestion_records(),
        timestamp: None,
    });

    let second_write_fut = client.write(WriteBatchRequest {
        namespace: namespace.clone(),
        topic: topic.clone(),
        partition: None,
        records: simple_ingestion_records(),
        timestamp: None,
    });

    tokio::time::advance(Duration::from_secs(2)).await;

    // Generally first write completes before second write, but that's not guaranteed.
    let first_write_info = first_write_fut.await.expect("first write failed");
    assert_eq!(
        2,
        first_write_info.end_offset - first_write_info.start_offset,
    );

    let second_write_info = second_write_fut.await.expect("second write failed");
    assert_eq!(
        2,
        second_write_info.end_offset - second_write_info.start_offset,
    );

    drop(ct_guard);
    ing_fut.await.expect("ingestion terminated");

    Ok(())
}

#[tokio::test]
async fn test_ingestion_if_schema_does_not_match() -> Result<()> {
    let (ing_fut, client, admin, ct) = create_batch_ingestor();
    let (namespace, topic) = initialize_test_topic(&admin).await;
    let ct_guard = ct.drop_guard();

    tokio::time::pause();

    let first_write_fut = client.write(WriteBatchRequest {
        namespace: namespace.clone(),
        topic: topic.clone(),
        partition: None,
        records: simple_ingestion_records(),
        timestamp: None,
    });

    // Notice that the schema created by record_batch! contains nullable fields
    let bad_record_batch = record_batch!(
        ("id", Int32, vec![1, 2, 3]),
        ("name", Utf8, vec!["Alice", "Bob", "Charlie"]),
        ("age", Utf8, vec!["one", "two", "three"])
    )
    .expect("create record batch");

    let err_write_fut = client.write(WriteBatchRequest {
        namespace: namespace.clone(),
        topic: topic.clone(),
        partition: None,
        records: bad_record_batch,
        timestamp: None,
    });

    let second_write_fut = client.write(WriteBatchRequest {
        namespace: namespace.clone(),
        topic: topic.clone(),
        partition: None,
        records: simple_ingestion_records(),
        timestamp: None,
    });

    tokio::time::advance(Duration::from_secs(2)).await;

    let err_result = err_write_fut.await.unwrap_err();
    assert!(matches!(err_result, WriteBatchError::Validation { .. }));

    let first_write_info = first_write_fut.await.expect("first write failed");
    assert_eq!(
        2,
        first_write_info.end_offset - first_write_info.start_offset,
    );

    let second_write_info = second_write_fut.await.expect("second write failed");
    assert_eq!(
        2,
        second_write_info.end_offset - second_write_info.start_offset,
    );

    drop(ct_guard);
    ing_fut.await.expect("ingestion terminated");

    Ok(())
}
