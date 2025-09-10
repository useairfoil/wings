use common::{
    create_ingestor_and_provider, initialize_test_namespace, initialize_test_topic,
    schema_without_partition,
};
use datafusion::{
    assert_batches_sorted_eq, common::arrow::array::RecordBatch, common::create_array,
};
use wings_ingestor_core::{Result, WriteBatchRequest};

mod common;

#[tokio::test]
async fn test_simple_query_with_no_data() -> Result<()> {
    let (ing_fut, _ingestion, provider_factory, admin, ct) = create_ingestor_and_provider();
    let namespace = initialize_test_namespace(&admin).await;
    let _topic = initialize_test_topic(&admin, &namespace.name).await;
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
        .sql("SELECT * FROM my_topic WHERE __offset__ BETWEEN 0 AND 100")
        .await
        .expect("sql");

    let out = out.collect().await.expect("collect");
    assert!(out.is_empty());

    drop(ct_guard);
    ing_fut.await.expect("ingestion terminated");

    Ok(())
}

#[tokio::test]
async fn test_simple_query_with_data_from_multiple_batches() -> Result<()> {
    let (ing_fut, ingestion, provider_factory, admin, ct) = create_ingestor_and_provider();
    let namespace = initialize_test_namespace(&admin).await;
    let topic = initialize_test_topic(&admin, &namespace.name).await;
    let ct_guard = ct.drop_guard();

    tokio::time::pause();

    {
        let records = RecordBatch::try_new(
            schema_without_partition().into(),
            vec![
                create_array!(Int32, vec![1, 2, 3]),
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
                namespace: namespace.clone(),
                topic: topic.clone(),
                partition: None,
                records,
                timestamp: None,
            })
            .await
            .expect("first_write");
    };

    {
        let records = RecordBatch::try_new(
            schema_without_partition().into(),
            vec![
                create_array!(Int32, vec![4, 5]),
                create_array!(Utf8, vec!["Dylan".to_string(), "Erik".to_string(),]),
                create_array!(Int32, vec![75, 42]),
            ],
        )
        .expect("create record batch");

        ingestion
            .write(WriteBatchRequest {
                namespace: namespace.clone(),
                topic: topic.clone(),
                partition: None,
                records,
                timestamp: None,
            })
            .await
            .expect("second_write");
    };

    let provider = provider_factory
        .create_provider(namespace.name.clone())
        .await
        .expect("create_provider");

    let ctx = provider
        .new_session_context()
        .await
        .expect("new_session_context");

    let out = ctx
        .sql("SELECT * FROM my_topic WHERE __offset__ BETWEEN 0 AND 100")
        .await
        .expect("sql");

    let out = out.collect().await.expect("collect");
    let expected = vec![
        "+----+---------+-----+------------+",
        "| id | name    | age | __offset__ |",
        "+----+---------+-----+------------+",
        "| 1  | Alice   | 32  | 0          |",
        "| 2  | Bob     | 27  | 1          |",
        "| 3  | Charlie | 99  | 2          |",
        "| 4  | Dylan   | 75  | 3          |",
        "| 5  | Erik    | 42  | 4          |",
        "+----+---------+-----+------------+",
    ];
    assert_batches_sorted_eq!(expected, &out);

    drop(ct_guard);
    ing_fut.await.expect("ingestion terminated");

    Ok(())
}

#[tokio::test]
async fn test_simple_query_with_missing_offset_bounds() -> Result<()> {
    let (ing_fut, _ingestion, provider_factory, admin, ct) = create_ingestor_and_provider();
    let namespace = initialize_test_namespace(&admin).await;
    let _topic = initialize_test_topic(&admin, &namespace.name).await;
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

    let out = ctx.sql("SELECT * FROM my_topic").await.expect("sql");

    let err = out.collect().await.unwrap_err();
    assert_eq!(
        "No __offset__ filter provided. You must provide a lower and upper bound for __offset__.",
        err.message()
    );

    drop(ct_guard);
    ing_fut.await.expect("ingestion terminated");

    Ok(())
}
