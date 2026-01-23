use std::sync::Arc;

use common::{create_batch_ingestor, initialize_test_namespace};
use datafusion::common::{arrow::array::RecordBatch, create_array};
use wings_control_plane::{
    cluster_metadata::ClusterMetadata,
    resources::{Namespace, PartitionValue, Topic, TopicName, TopicOptions},
    schema::{DataType, Field, Schema, SchemaBuilder},
};
use wings_ingestor_core::{Result, WriteBatchError, WriteBatchRequest};

mod common;

fn partitioned_ingestion_schema() -> Schema {
    SchemaBuilder::new(vec![
        Field::new("region_code", 0, DataType::Int32, false),
        Field::new("name", 1, DataType::Utf8, false),
        Field::new("age", 2, DataType::Int32, false),
    ])
    .build()
    .unwrap()
}

fn partitioned_ingestion_schema_without_region_code() -> Schema {
    SchemaBuilder::new(vec![
        Field::new("name", 1, DataType::Utf8, false),
        Field::new("age", 2, DataType::Int32, false),
    ])
    .build()
    .unwrap()
}

fn partitioned_ingestion_records() -> RecordBatch {
    RecordBatch::try_new(
        partitioned_ingestion_schema_without_region_code()
            .arrow_schema()
            .into(),
        vec![
            create_array!(Utf8, vec!["Alice", "Bob", "Charlie"]),
            create_array!(Int32, vec![25, 30, 35]),
        ],
    )
    .expect("failed to create batch")
}

/// Initialize a test topic with partition key by region_code
async fn initialize_test_topic(
    cluster_meta: &Arc<dyn ClusterMetadata>,
) -> (Arc<Namespace>, Arc<Topic>) {
    let namespace = initialize_test_namespace(cluster_meta).await;

    let topic_name = TopicName::new_unchecked("simple_ingestion", namespace.name.clone());
    let schema = partitioned_ingestion_schema();
    let topic = cluster_meta
        .create_topic(
            topic_name,
            TopicOptions::new_with_partition_key(schema, Some(0)),
        )
        .await
        .expect("create_topic");

    (namespace, topic.into())
}

#[tokio::test]
async fn test_ingest_different_partitions() -> Result<()> {
    let (ing_fut, client, admin, ct) = create_batch_ingestor();
    let (namespace, topic) = initialize_test_topic(&admin).await;
    let ct_guard = ct.drop_guard();

    tokio::time::pause();

    let write_region_100_fut = client.write(WriteBatchRequest {
        namespace: namespace.clone(),
        topic: topic.clone(),
        partition: Some(PartitionValue::Int32(100)),
        records: partitioned_ingestion_records(),
        timestamp: None,
    });

    let write_region_200_fut = client.write(WriteBatchRequest {
        namespace: namespace.clone(),
        topic: topic.clone(),
        partition: Some(PartitionValue::Int32(200)),
        records: partitioned_ingestion_records(),
        timestamp: None,
    });

    let write_region_100 = write_region_100_fut.await.unwrap();
    assert_eq!(0, write_region_100.start_offset);
    assert_eq!(2, write_region_100.end_offset);

    let write_region_200 = write_region_200_fut.await.unwrap();
    assert_eq!(0, write_region_200.start_offset);
    assert_eq!(2, write_region_200.end_offset);

    drop(ct_guard);
    ing_fut.await.expect("ingestion terminated");

    Ok(())
}

#[tokio::test]
async fn test_ingest_fails_with_invalid_partition_type() -> Result<()> {
    let (ing_fut, client, admin, ct) = create_batch_ingestor();
    let (namespace, topic) = initialize_test_topic(&admin).await;
    let ct_guard = ct.drop_guard();

    tokio::time::pause();

    let write_fut = client.write(WriteBatchRequest {
        namespace: namespace.clone(),
        topic: topic.clone(),
        partition: Some(PartitionValue::UInt64(100)),
        records: partitioned_ingestion_records(),
        timestamp: None,
    });

    let write_err = write_fut.await.unwrap_err();
    assert!(matches!(write_err, WriteBatchError::Validation { .. }));

    drop(ct_guard);
    ing_fut.await.expect("ingestion terminated");

    Ok(())
}

#[tokio::test]
async fn test_ingest_fails_with_missing_partition() -> Result<()> {
    let (ing_fut, client, admin, ct) = create_batch_ingestor();
    let (namespace, topic) = initialize_test_topic(&admin).await;
    let ct_guard = ct.drop_guard();

    tokio::time::pause();

    let write_fut = client.write(WriteBatchRequest {
        namespace: namespace.clone(),
        topic: topic.clone(),
        partition: None,
        records: partitioned_ingestion_records(),
        timestamp: None,
    });

    let write_err = write_fut.await.unwrap_err();
    assert!(matches!(write_err, WriteBatchError::Validation { .. }));

    drop(ct_guard);
    ing_fut.await.expect("ingestion terminated");

    Ok(())
}
