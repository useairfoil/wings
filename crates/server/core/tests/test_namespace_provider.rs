use std::{sync::Arc, time::Duration};

use common::{
    create_ingestor_and_provider, initialize_test_namespace, initialize_test_partitioned_table,
    initialize_test_table, schema_without_partition,
};
use datafusion::{
    assert_batches_sorted_eq,
    common::{arrow::array::RecordBatch, create_array},
};
use futures::stream;
use wings_ingestor_core::{IngestorClient, IngestorError, Result, WriteBatchRequest};
use wings_resources::{Namespace, PartitionValue, Table};

mod common;

#[tokio::test]
async fn test_metadata_system_tables() -> Result<()> {
    let (ing_fut, _ingestion, provider_factory, admin, ct) = create_ingestor_and_provider().await;
    let namespace = initialize_test_namespace(&admin).await;
    let _table = initialize_test_table(&admin, &namespace.name).await;
    let _table = initialize_test_partitioned_table(&admin, &namespace.name).await;
    let ct_guard = ct.drop_guard();

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
        "| wings         | system       | table                 | VIEW       |",
        "| wings         | system       | table_row_location    | VIEW       |",
        "| wings         | system       | table_partition_value | VIEW       |",
        "| wings         | system       | table_schema          | VIEW       |",
        "+---------------+--------------+-----------------------+------------+",
    ];
    assert_batches_sorted_eq!(expected, &out);

    drop(ct_guard);
    ing_fut.await.expect("ingestion terminated");

    Ok(())
}

#[tokio::test]
async fn test_table_and_table_schema() -> Result<()> {
    let (ing_fut, _ingestion, provider_factory, admin, ct) = create_ingestor_and_provider().await;
    let namespace = initialize_test_namespace(&admin).await;
    let _table = initialize_test_table(&admin, &namespace.name).await;
    let _table = initialize_test_partitioned_table(&admin, &namespace.name).await;
    let ct_guard = ct.drop_guard();

    let provider = provider_factory
        .create_provider(namespace.name.clone())
        .await
        .expect("create_provider");

    let ctx = provider
        .new_session_context()
        .await
        .expect("new_session_context");

    {
        let out = ctx.sql("SELECT * FROM system.table").await.expect("sql");
        let out = out.collect().await.expect("collect");
        let expected = [
            "+--------+-----------+----------------------+---------------+-------------+-------------------------+-------------------+",
            "| tenant | namespace | table                | partition_key | description | compaction_freshness_ms | compaction_ttl_ms |",
            "+--------+-----------+----------------------+---------------+-------------+-------------------------+-------------------+",
            "| test   | test-ns   | my_partitioned_table | 0             |             | 300000                  |                   |",
            "| test   | test-ns   | my_table             |               |             | 300000                  |                   |",
            "+--------+-----------+----------------------+---------------+-------------+-------------------------+-------------------+",
        ];
        assert_batches_sorted_eq!(expected, &out);
    }
    {
        let out = ctx
            .sql("SELECT * FROM system.table_schema")
            .await
            .expect("sql");
        let out = out.collect().await.expect("collect");
        let expected = [
            "+--------+-----------+----------------------+-----------+-----------+----------+------------------+",
            "| tenant | namespace | table                | field     | data_type | nullable | is_partition_key |",
            "+--------+-----------+----------------------+-----------+-----------+----------+------------------+",
            "| test   | test-ns   | my_partitioned_table | age       | Int32     | false    | false            |",
            "| test   | test-ns   | my_partitioned_table | id        | Int32     | false    | false            |",
            "| test   | test-ns   | my_partitioned_table | name      | Utf8      | false    | false            |",
            "| test   | test-ns   | my_partitioned_table | region_id | Int64     | false    | true             |",
            "| test   | test-ns   | my_table             | age       | Int32     | false    | false            |",
            "| test   | test-ns   | my_table             | id        | Int32     | false    | false            |",
            "| test   | test-ns   | my_table             | name      | Utf8      | false    | false            |",
            "+--------+-----------+----------------------+-----------+-----------+----------+------------------+",
        ];
        assert_batches_sorted_eq!(expected, &out);
    }

    drop(ct_guard);
    ing_fut.await.expect("ingestion terminated");

    Ok(())
}

#[tokio::test]
async fn test_table_row_location() -> Result<()> {
    let (ing_fut, ingestion, provider_factory, admin, ct) = create_ingestor_and_provider().await;
    let namespace = initialize_test_namespace(&admin).await;
    let simple_table = initialize_test_table(&admin, &namespace.name).await;
    let partitioned_table = initialize_test_partitioned_table(&admin, &namespace.name).await;
    let ct_guard = ct.drop_guard();

    tokio::time::pause();

    ingest_some_data(
        &ingestion,
        namespace.clone(),
        simple_table,
        partitioned_table,
    )
    .await
    .unwrap();

    tokio::time::resume();

    let provider = provider_factory
        .create_provider(namespace.name.clone())
        .await
        .expect("create_provider");

    let ctx = provider
        .new_session_context()
        .await
        .expect("new_session_context");

    let out = ctx
        .sql("SELECT table, partition_value, start_seqnum, end_seqnum, location_type FROM system.table_row_location")
        .await
        .expect("sql");
    let out = out.collect().await.expect("collect");
    let expected = vec![
        "+----------------------+-----------------+--------------+------------+---------------+",
        "| table                | partition_value | start_seqnum | end_seqnum | location_type |",
        "+----------------------+-----------------+--------------+------------+---------------+",
        "| my_partitioned_table | 100             | 0            | 2          | folio         |",
        "| my_partitioned_table | 100             | 3            | 5          | folio         |",
        "| my_partitioned_table | 200             | 0            | 2          | folio         |",
        "| my_table             |                 | 0            | 2          | folio         |",
        "| my_table             |                 | 3            | 5          | folio         |",
        "| my_table             |                 | 6            | 8          | folio         |",
        "+----------------------+-----------------+--------------+------------+---------------+",
    ];
    assert_batches_sorted_eq!(expected, &out);

    drop(ct_guard);
    ing_fut.await.expect("ingestion terminated");

    Ok(())
}

#[tokio::test]
async fn test_table_partition_value() -> Result<()> {
    let (ing_fut, ingestion, provider_factory, admin, ct) = create_ingestor_and_provider().await;
    let namespace = initialize_test_namespace(&admin).await;
    let simple_table = initialize_test_table(&admin, &namespace.name).await;
    let partitioned_table = initialize_test_partitioned_table(&admin, &namespace.name).await;
    let ct_guard = ct.drop_guard();

    tokio::time::pause();

    ingest_some_data(
        &ingestion,
        namespace.clone(),
        simple_table,
        partitioned_table,
    )
    .await
    .unwrap();

    tokio::time::resume();

    let provider = provider_factory
        .create_provider(namespace.name.clone())
        .await
        .expect("create_provider");

    let ctx = provider
        .new_session_context()
        .await
        .expect("new_session_context");

    let out = ctx
        .sql("SELECT * FROM system.table_partition_value")
        .await
        .expect("sql")
        // Timestamp columns change value based on the current time
        .drop_columns(&["latest_timestamp"])
        .expect("drop timestamp columns");
    let out = out.collect().await.expect("collect");
    let expected = [
        "+--------+-----------+----------------------+-----------------+-------------+",
        "| tenant | namespace | table                | partition_value | next_offset |",
        "+--------+-----------+----------------------+-----------------+-------------+",
        "| test   | test-ns   | my_partitioned_table | 100             | 6           |",
        "| test   | test-ns   | my_partitioned_table | 200             | 3           |",
        "| test   | test-ns   | my_table             |                 | 9           |",
        "+--------+-----------+----------------------+-----------------+-------------+",
    ];
    assert_batches_sorted_eq!(expected, &out);

    drop(ct_guard);
    ing_fut.await.expect("ingestion terminated");

    Ok(())
}

async fn ingest_some_data(
    client: &IngestorClient,
    namespace: Arc<Namespace>,
    simple_table: Arc<Table>,
    partitioned_table: Arc<Table>,
) -> Result<(), IngestorError> {
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
        let writes = vec![
            WriteBatchRequest {
                batch_id: 0,
                table: simple_table.clone(),
                partition: None,
                records: records.clone(),
                timestamp: None,
            }
            .into(),
            WriteBatchRequest {
                batch_id: 0,
                table: simple_table.clone(),
                partition: None,
                records: records.clone(),
                timestamp: None,
            }
            .into(),
            WriteBatchRequest {
                batch_id: 0,
                table: partitioned_table.clone(),
                partition: Some(PartitionValue::Int64(100)),
                records: records.clone(),
                timestamp: None,
            }
            .into(),
            WriteBatchRequest {
                batch_id: 0,
                table: partitioned_table.clone(),
                partition: Some(PartitionValue::Int64(200)),
                records: records.clone(),
                timestamp: None,
            }
            .into(),
        ];
        let write_result = client.ingest(namespace.clone(), stream::iter(writes));

        tokio::time::advance(Duration::from_secs(2)).await;

        write_result.await?;
    }

    client
        .ingest(
            namespace.clone(),
            stream::iter(vec![
                WriteBatchRequest {
                    batch_id: 0,
                    table: simple_table.clone(),
                    partition: None,
                    records: records.clone(),
                    timestamp: None,
                }
                .into(),
            ]),
        )
        .await?;

    client
        .ingest(
            namespace.clone(),
            stream::iter(vec![
                WriteBatchRequest {
                    batch_id: 0,
                    table: partitioned_table.clone(),
                    partition: Some(PartitionValue::Int64(100)),
                    records: records.clone(),
                    timestamp: None,
                }
                .into(),
            ]),
        )
        .await?;

    Ok(())
}
