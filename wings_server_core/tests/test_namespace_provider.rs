use std::{sync::Arc, time::Duration};

use common::{
    create_ingestor_and_provider, initialize_test_namespace, initialize_test_partitioned_topic,
    initialize_test_topic, schema_without_partition,
};
use datafusion::{
    assert_batches_sorted_eq, common::arrow::array::RecordBatch, common::create_array,
};
use wings_control_plane::resources::{Namespace, PartitionValue, Topic};
use wings_ingestor_core::{BatchIngestorClient, Result, WriteBatchError, WriteBatchRequest};

mod common;

#[tokio::test]
async fn test_metadata_system_tables() -> Result<()> {
    let (ing_fut, _ingestion, provider_factory, admin, ct) = create_ingestor_and_provider();
    let namespace = initialize_test_namespace(&admin).await;
    let _topic = initialize_test_topic(&admin, &namespace.name).await;
    let _topic = initialize_test_partitioned_topic(&admin, &namespace.name).await;
    let ct_guard = ct.drop_guard();

    tokio::time::pause();

    let provider = provider_factory
        .create_provider(namespace.name.clone())
        .await
        .expect("create_provider");

    let ctx = provider
        .new_session_context()
        .await
        .expect("new_session_context");

    let out = ctx
        .sql("SELECT * FROM information_schema.tables WHERE table_schema = 'system'")
        .await
        .expect("sql");
    let out = out.collect().await.expect("collect");
    let expected = [
        "+---------------+--------------+-----------------------+------------+",
        "| table_catalog | table_schema | table_name            | table_type |",
        "+---------------+--------------+-----------------------+------------+",
        "| wings         | system       | metrics               | VIEW       |",
        "| wings         | system       | namespace_info        | VIEW       |",
        "| wings         | system       | topic                 | VIEW       |",
        "| wings         | system       | topic_offset_location | VIEW       |",
        "| wings         | system       | topic_partition_value | VIEW       |",
        "| wings         | system       | topic_schema          | VIEW       |",
        "+---------------+--------------+-----------------------+------------+",
    ];
    assert_batches_sorted_eq!(expected, &out);

    drop(ct_guard);
    ing_fut.await.expect("ingestion terminated");

    Ok(())
}

#[tokio::test]
async fn test_topic_and_topic_schema() -> Result<()> {
    let (ing_fut, _ingestion, provider_factory, admin, ct) = create_ingestor_and_provider();
    let namespace = initialize_test_namespace(&admin).await;
    let _topic = initialize_test_topic(&admin, &namespace.name).await;
    let _topic = initialize_test_partitioned_topic(&admin, &namespace.name).await;
    let ct_guard = ct.drop_guard();

    tokio::time::pause();

    let provider = provider_factory
        .create_provider(namespace.name.clone())
        .await
        .expect("create_provider");

    let ctx = provider
        .new_session_context()
        .await
        .expect("new_session_context");

    {
        let out = ctx.sql("SELECT * FROM system.topic").await.expect("sql");
        let out = out.collect().await.expect("collect");
        let expected = [
            "+--------+-----------+----------------------+---------------+-------------+-------------------------+-------------------+",
            "| tenant | namespace | topic                | partition_key | description | compaction_freshness_ms | compaction_ttl_ms |",
            "+--------+-----------+----------------------+---------------+-------------+-------------------------+-------------------+",
            "| test   | test-ns   | my_partitioned_topic | 0             |             | 300000                  |                   |",
            "| test   | test-ns   | my_topic             |               |             | 300000                  |                   |",
            "+--------+-----------+----------------------+---------------+-------------+-------------------------+-------------------+",
        ];
        assert_batches_sorted_eq!(expected, &out);
    }
    {
        let out = ctx
            .sql("SELECT * FROM system.topic_schema")
            .await
            .expect("sql");
        let out = out.collect().await.expect("collect");
        let expected = [
            "+--------+-----------+----------------------+-----------+-----------+----------+------------------+",
            "| tenant | namespace | topic                | field     | data_type | nullable | is_partition_key |",
            "+--------+-----------+----------------------+-----------+-----------+----------+------------------+",
            "| test   | test-ns   | my_partitioned_topic | age       | Int32     | false    | false            |",
            "| test   | test-ns   | my_partitioned_topic | id        | Int32     | false    | false            |",
            "| test   | test-ns   | my_partitioned_topic | name      | Utf8      | false    | false            |",
            "| test   | test-ns   | my_partitioned_topic | region_id | Int64     | false    | true             |",
            "| test   | test-ns   | my_topic             | age       | Int32     | false    | false            |",
            "| test   | test-ns   | my_topic             | id        | Int32     | false    | false            |",
            "| test   | test-ns   | my_topic             | name      | Utf8      | false    | false            |",
            "+--------+-----------+----------------------+-----------+-----------+----------+------------------+",
        ];
        assert_batches_sorted_eq!(expected, &out);
    }

    drop(ct_guard);
    ing_fut.await.expect("ingestion terminated");

    Ok(())
}

#[tokio::test]
async fn test_topic_offset_location() -> Result<()> {
    let (ing_fut, ingestion, provider_factory, admin, ct) = create_ingestor_and_provider();
    let namespace = initialize_test_namespace(&admin).await;
    let simple_topic = initialize_test_topic(&admin, &namespace.name).await;
    let partitioned_topic = initialize_test_partitioned_topic(&admin, &namespace.name).await;
    let ct_guard = ct.drop_guard();

    tokio::time::pause();

    ingest_some_data(
        &ingestion,
        namespace.clone(),
        simple_topic,
        partitioned_topic,
    )
    .await
    .unwrap();

    let provider = provider_factory
        .create_provider(namespace.name.clone())
        .await
        .expect("create_provider");

    let ctx = provider
        .new_session_context()
        .await
        .expect("new_session_context");

    let out = ctx
        .sql("SELECT topic, partition_value, start_offset, end_offset, location_type FROM system.topic_offset_location")
        .await
        .expect("sql");
    let out = out.collect().await.expect("collect");
    let expected = vec![
        "+----------------------+-----------------+--------------+------------+---------------+",
        "| topic                | partition_value | start_offset | end_offset | location_type |",
        "+----------------------+-----------------+--------------+------------+---------------+",
        "| my_partitioned_topic | 100             | 0            | 2          | folio         |",
        "| my_partitioned_topic | 100             | 3            | 5          | folio         |",
        "| my_partitioned_topic | 200             | 0            | 2          | folio         |",
        "| my_topic             |                 | 0            | 2          | folio         |",
        "| my_topic             |                 | 3            | 5          | folio         |",
        "| my_topic             |                 | 6            | 8          | folio         |",
        "+----------------------+-----------------+--------------+------------+---------------+",
    ];
    assert_batches_sorted_eq!(expected, &out);

    drop(ct_guard);
    ing_fut.await.expect("ingestion terminated");

    Ok(())
}

#[tokio::test]
async fn test_topic_partition_value() -> Result<()> {
    let (ing_fut, ingestion, provider_factory, admin, ct) = create_ingestor_and_provider();
    let namespace = initialize_test_namespace(&admin).await;
    let simple_topic = initialize_test_topic(&admin, &namespace.name).await;
    let partitioned_topic = initialize_test_partitioned_topic(&admin, &namespace.name).await;
    let ct_guard = ct.drop_guard();

    tokio::time::pause();

    ingest_some_data(
        &ingestion,
        namespace.clone(),
        simple_topic,
        partitioned_topic,
    )
    .await
    .unwrap();

    let provider = provider_factory
        .create_provider(namespace.name.clone())
        .await
        .expect("create_provider");

    let ctx = provider
        .new_session_context()
        .await
        .expect("new_session_context");

    let out = ctx
        .sql("SELECT * FROM system.topic_partition_value")
        .await
        .expect("sql")
        // Timestamp columns change value based on the current time
        .drop_columns(&["latest_timestamp"])
        .expect("drop timestamp columns");
    let out = out.collect().await.expect("collect");
    let expected = [
        "+--------+-----------+----------------------+-----------------+-------------+",
        "| tenant | namespace | topic                | partition_value | next_offset |",
        "+--------+-----------+----------------------+-----------------+-------------+",
        "| test   | test-ns   | my_partitioned_topic | 100             | 6           |",
        "| test   | test-ns   | my_partitioned_topic | 200             | 3           |",
        "| test   | test-ns   | my_topic             |                 | 9           |",
        "+--------+-----------+----------------------+-----------------+-------------+",
    ];
    assert_batches_sorted_eq!(expected, &out);

    drop(ct_guard);
    ing_fut.await.expect("ingestion terminated");

    Ok(())
}

async fn ingest_some_data(
    client: &BatchIngestorClient,
    namespace: Arc<Namespace>,
    simple_topic: Arc<Topic>,
    partitioned_topic: Arc<Topic>,
) -> Result<(), WriteBatchError> {
    let records = RecordBatch::try_new(
        schema_without_partition().arrow_schema().into(),
        vec![
            create_array!(Int32, vec![101, 102, 103]),
            create_array!(
                Utf8,
                vec![
                    "Alice".to_string(),
                    "Bob".to_string(),
                    "Charlie".to_string()
                ]
            ),
            create_array!(Int32, vec![32, 27, 99]),
        ],
    )
    .expect("create record batch");

    // Write together so they're in the same folio.
    {
        let first_write = client.write(WriteBatchRequest {
            namespace: namespace.clone(),
            topic: simple_topic.clone(),
            partition: None,
            records: records.clone(),
            timestamp: None,
        });

        let second_write = client.write(WriteBatchRequest {
            namespace: namespace.clone(),
            topic: simple_topic.clone(),
            partition: None,
            records: records.clone(),
            timestamp: None,
        });

        let third_write = client.write(WriteBatchRequest {
            namespace: namespace.clone(),
            topic: partitioned_topic.clone(),
            partition: Some(PartitionValue::Int64(100)),
            records: records.clone(),
            timestamp: None,
        });

        let fourth_write = client.write(WriteBatchRequest {
            namespace: namespace.clone(),
            topic: partitioned_topic.clone(),
            partition: Some(PartitionValue::Int64(200)),
            records: records.clone(),
            timestamp: None,
        });

        tokio::time::advance(Duration::from_secs(2)).await;

        first_write.await?;
        second_write.await?;
        third_write.await?;
        fourth_write.await?;
    }

    client
        .write(WriteBatchRequest {
            namespace: namespace.clone(),
            topic: simple_topic.clone(),
            partition: None,
            records: records.clone(),
            timestamp: None,
        })
        .await?;

    client
        .write(WriteBatchRequest {
            namespace: namespace.clone(),
            topic: partitioned_topic.clone(),
            partition: Some(PartitionValue::Int64(100)),
            records: records.clone(),
            timestamp: None,
        })
        .await?;

    Ok(())
}
