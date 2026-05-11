use common::{
    create_ingestor_and_provider, initialize_test_namespace, initialize_test_partitioned_topic,
    schema_without_partition,
};
use datafusion::{
    assert_batches_sorted_eq,
    common::{arrow::array::RecordBatch, create_array},
};
use wings_ingestor_core::{Result, WriteBatchRequest};
use wings_resources::PartitionValue;

mod common;

#[tokio::test]
async fn test_partitioned_query_with_no_data() -> Result<()> {
    let (ing_fut, _ingestion, provider_factory, admin, ct) = create_ingestor_and_provider().await;
    let namespace = initialize_test_namespace(&admin).await;
    let _topic = initialize_test_partitioned_topic(&admin, &namespace.name).await;
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
        .sql("SELECT * FROM my_partitioned_topic WHERE __offset__ BETWEEN 0 AND 100 AND region_id = 1")
        .await
        .expect("sql");

    let out = out.collect().await.expect("collect");
    assert!(out.is_empty());

    drop(ct_guard);
    ing_fut.await.expect("ingestion terminated");

    Ok(())
}

#[tokio::test]
async fn test_partitioned_query_with_data_from_multiple_batches() -> Result<()> {
    let (ing_fut, ingestion, provider_factory, admin, ct) = create_ingestor_and_provider().await;
    let namespace = initialize_test_namespace(&admin).await;
    let topic = initialize_test_partitioned_topic(&admin, &namespace.name).await;
    let ct_guard = ct.drop_guard();

    {
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

        ingestion
            .write(WriteBatchRequest {
                batch_id: 0,
                namespace: namespace.clone(),
                topic: topic.clone(),
                partition: Some(PartitionValue::Int64(100)),
                records,
                timestamp: None,
            })
            .await
            .expect("first_write");
    };

    {
        let records = RecordBatch::try_new(
            schema_without_partition().arrow_schema().into(),
            vec![
                create_array!(Int32, vec![104, 105]),
                create_array!(Utf8, vec!["Dylan".to_string(), "Erik".to_string(),]),
                create_array!(Int32, vec![75, 42]),
            ],
        )
        .expect("create record batch");

        ingestion
            .write(WriteBatchRequest {
                batch_id: 0,
                namespace: namespace.clone(),
                topic: topic.clone(),
                partition: Some(PartitionValue::Int64(200)),
                records,
                timestamp: None,
            })
            .await
            .expect("second_write");
    };

    {
        let records = RecordBatch::try_new(
            schema_without_partition().arrow_schema().into(),
            vec![
                create_array!(Int32, vec![201, 202, 203]),
                create_array!(
                    Utf8,
                    vec![
                        "Frank".to_string(),
                        "Grace".to_string(),
                        "Henry".to_string()
                    ]
                ),
                create_array!(Int32, vec![28, 31, 45]),
            ],
        )
        .expect("create record batch");

        ingestion
            .write(WriteBatchRequest {
                batch_id: 0,
                namespace: namespace.clone(),
                topic: topic.clone(),
                partition: Some(PartitionValue::Int64(100)),
                records,
                timestamp: None,
            })
            .await
            .expect("third_write");
    };

    let provider = provider_factory
        .create_provider(namespace.name.clone())
        .await
        .expect("create_provider");

    let ctx = provider
        .new_session_context()
        .await
        .expect("new_session_context");

    let df = ctx
        .sql("SELECT * FROM my_partitioned_topic WHERE __offset__ BETWEEN 0 AND 100 AND region_id = 100")
        .await
        .expect("sql")
        .drop_columns(&["__timestamp__"])
        .expect("drop timestamp");

    let out = df.collect().await.expect("collect");
    let expected = vec![
        "+-----+---------+-----+-----------+------------+",
        "| id  | name    | age | region_id | __offset__ |",
        "+-----+---------+-----+-----------+------------+",
        "| 101 | Alice   | 32  | 100       | 0          |",
        "| 102 | Bob     | 27  | 100       | 1          |",
        "| 103 | Charlie | 99  | 100       | 2          |",
        "| 201 | Frank   | 28  | 100       | 3          |",
        "| 202 | Grace   | 31  | 100       | 4          |",
        "| 203 | Henry   | 45  | 100       | 5          |",
        "+-----+---------+-----+-----------+------------+",
    ];
    assert_batches_sorted_eq!(expected, &out);

    drop(ct_guard);
    ing_fut.await.expect("ingestion terminated");

    Ok(())
}

#[tokio::test]
async fn test_partitioned_query_with_missing_offset_bounds() -> Result<()> {
    let (ing_fut, _ingestion, provider_factory, admin, ct) = create_ingestor_and_provider().await;
    let namespace = initialize_test_namespace(&admin).await;
    let _topic = initialize_test_partitioned_topic(&admin, &namespace.name).await;
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
        .sql("SELECT * FROM my_partitioned_topic WHERE region_id = 100")
        .await
        .expect("sql");

    let err = out.collect().await.unwrap_err();
    assert_eq!(
        "No __offset__ filter provided. You must provide a lower and upper bound for __offset__.",
        err.message()
    );

    drop(ct_guard);
    ing_fut.await.expect("ingestion terminated");

    Ok(())
}

#[tokio::test]
async fn test_partitioned_query_with_missing_partition_value() -> Result<()> {
    let (ing_fut, _ingestion, provider_factory, admin, ct) = create_ingestor_and_provider().await;
    let namespace = initialize_test_namespace(&admin).await;
    let _topic = initialize_test_partitioned_topic(&admin, &namespace.name).await;
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
        .sql("SELECT * FROM my_partitioned_topic WHERE __offset__ BETWEEN 0 AND 100")
        .await
        .expect("sql");

    let err = out.collect().await.unwrap_err();
    assert_eq!(
        "No region_id filter provided. You must provide a value for the partition column.",
        err.message()
    );

    drop(ct_guard);
    ing_fut.await.expect("ingestion terminated");

    Ok(())
}
