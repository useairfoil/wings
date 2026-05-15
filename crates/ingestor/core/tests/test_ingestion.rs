mod common;

use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use common::{
    MockTableMetadataService, MockObjectStoreFactory, MockObjectStorePutOpts, TestIngestor,
    initialize_test_namespace, initialize_test_partitioned_table, initialize_test_table,
    people_records, sort_by_batch_id,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use wings_control_plane_core::table_metadata::{
    AcceptedBatchInfo, CommittedBatch, RejectedBatchInfo,
};
use wings_ingestor_core::{IngestionRequest, Result, WriteBatchRequest};
use wings_resources::PartitionValue;

#[tokio::test]
async fn ingests_single_message_without_partition_value() -> Result<()> {
    let mut table_metadata = MockTableMetadataService::new();
    table_metadata.expect_commit().times(1).returning({
        move |_namespace, batches| {
            insta::assert_yaml_snapshot!(batches, {
                "[].file_ref" => "[file-ref]",
                "[].timestamp" => "[timestamp]"
            }, @r#"
            - batch_id: 0
              table_name: tenants/test/namespaces/test-ns/tables/people
              partition_value: ~
              file_ref: "[file-ref]"
              page_offset_bytes: 0
              page_size_bytes: 1256
              timestamp: "[timestamp]"
              num_rows: 1
            "#);
            Ok(vec![CommittedBatch::Accepted(AcceptedBatchInfo {
                batch_id: 0,
                start_seqnum: 0,
                end_seqnum: 0,
                timestamp: SystemTime::now(),
            })])
        }
    });

    let ingestor = TestIngestor::start(table_metadata).await;
    let namespace = initialize_test_namespace(&ingestor.cluster_meta).await;
    let table = initialize_test_table(&ingestor.cluster_meta, &namespace.name).await;

    let mut responses = ingestor
        .ingest(
            namespace.clone(),
            vec![WriteBatchRequest::new(
                table.clone(),
                people_records(&table, &[(1, "Alice", 32)]),
            )],
        )
        .await?;

    sort_by_batch_id(&mut responses);

    insta::assert_yaml_snapshot!(responses, {
        "[].timestamp" => "[timestamp]"
    }, @r#"
    - Accepted:
        batch_id: 0
        start_seqnum: 0
        end_seqnum: 0
        timestamp: "[timestamp]"
    "#);

    insta::assert_yaml_snapshot!(ingestor.list_files(&namespace).await, {
        "[].path" => insta::dynamic_redaction(|value, _path| {
            assert!(value.as_str().unwrap().starts_with("tenants/test/namespaces/test-ns/folio/"));
            "[path]"
        })
    }, @r#"
    - path: "[path]"
      size: 1256
    "#);

    ingestor.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn abort_returns_empty_response_without_committing() -> Result<()> {
    let mut table_metadata = MockTableMetadataService::new();
    table_metadata.expect_commit().times(0);

    let ingestor = TestIngestor::start(table_metadata).await;
    let namespace = initialize_test_namespace(&ingestor.cluster_meta).await;
    let table = initialize_test_table(&ingestor.cluster_meta, &namespace.name).await;

    let responses = ingestor
        .client
        .ingest(
            namespace.clone(),
            futures::stream::iter([
                WriteBatchRequest::new(table.clone(), people_records(&table, &[(1, "Alice", 32)]))
                    .into(),
                IngestionRequest::Abort,
            ]),
        )
        .await?;

    assert!(responses.is_empty());

    ingestor.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn ingests_multiple_messages_without_partition_values() -> Result<()> {
    let mut table_metadata = MockTableMetadataService::new();
    table_metadata.expect_commit().times(1).returning({
        move |_namespace, batches| {
            insta::assert_yaml_snapshot!(batches, {
                "[].file_ref" => "[file-ref]",
                "[].timestamp" => "[timestamp]"
            }, @r#"
            - batch_id: 0
              table_name: tenants/test/namespaces/test-ns/tables/people
              partition_value: ~
              file_ref: "[file-ref]"
              page_offset_bytes: 0
              page_size_bytes: 1315
              timestamp: "[timestamp]"
              num_rows: 2
            - batch_id: 1
              table_name: tenants/test/namespaces/test-ns/tables/people
              partition_value: ~
              file_ref: "[file-ref]"
              page_offset_bytes: 0
              page_size_bytes: 1315
              timestamp: "[timestamp]"
              num_rows: 2
            "#);

            Ok(vec![
                CommittedBatch::Accepted(AcceptedBatchInfo {
                    batch_id: 0,
                    start_seqnum: 0,
                    end_seqnum: 1,
                    timestamp: SystemTime::now(),
                }),
                CommittedBatch::Accepted(AcceptedBatchInfo {
                    batch_id: 2,
                    start_seqnum: 2,
                    end_seqnum: 3,
                    timestamp: SystemTime::now(),
                }),
            ])
        }
    });

    let ingestor = TestIngestor::start(table_metadata).await;
    let namespace = initialize_test_namespace(&ingestor.cluster_meta).await;
    let table = initialize_test_table(&ingestor.cluster_meta, &namespace.name).await;

    let mut responses = ingestor
        .ingest(
            namespace.clone(),
            vec![
                WriteBatchRequest::new(
                    table.clone(),
                    people_records(&table, &[(1, "Alice", 32), (2, "Bob", 27)]),
                ),
                WriteBatchRequest::new(
                    table.clone(),
                    people_records(&table, &[(3, "Charlie", 99), (4, "Dylan", 75)]),
                )
                .with_batch_id(1),
            ],
        )
        .await?;

    sort_by_batch_id(&mut responses);

    insta::assert_yaml_snapshot!(responses, {
        "[].timestamp" => "[timestamp]"
    }, @r#"
    - Accepted:
        batch_id: 0
        start_seqnum: 0
        end_seqnum: 1
        timestamp: "[timestamp]"
    - Accepted:
        batch_id: 2
        start_seqnum: 2
        end_seqnum: 3
        timestamp: "[timestamp]"
    "#);

    insta::assert_yaml_snapshot!(ingestor.list_files(&namespace).await, {
        "[].path" => "[path]"
    }, @r#"
    - path: "[path]"
      size: 1315
    "#);

    ingestor.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn ingests_multiple_messages_belonging_to_different_folio() -> Result<()> {
    let mut table_metadata = MockTableMetadataService::new();
    table_metadata.expect_commit().times(1).returning({
        move |_namespace, batches| {
            insta::assert_yaml_snapshot!(batches, {
                "[].file_ref" => "[file-ref]",
                "[].timestamp" => "[timestamp]"
            }, @r#"
            - batch_id: 0
              table_name: tenants/test/namespaces/test-ns/tables/people
              partition_value: ~
              file_ref: "[file-ref]"
              page_offset_bytes: 0
              page_size_bytes: 1270
              timestamp: "[timestamp]"
              num_rows: 2
            - batch_id: 0
              table_name: tenants/test/namespaces/test-ns/tables/people
              partition_value: ~
              file_ref: "[file-ref]"
              page_offset_bytes: 0
              page_size_bytes: 1270
              timestamp: "[timestamp]"
              num_rows: 2
            "#);

            Ok(vec![
                CommittedBatch::Accepted(AcceptedBatchInfo {
                    batch_id: 0,
                    start_seqnum: 0,
                    end_seqnum: 1,
                    timestamp: SystemTime::now(),
                }),
                CommittedBatch::Accepted(AcceptedBatchInfo {
                    batch_id: 2,
                    start_seqnum: 2,
                    end_seqnum: 3,
                    timestamp: SystemTime::now(),
                }),
            ])
        }
    });

    let ingestor = TestIngestor::start(table_metadata).await;
    let namespace = initialize_test_namespace(&ingestor.cluster_meta).await;
    let table = initialize_test_table(&ingestor.cluster_meta, &namespace.name).await;

    let (tx, rx) = mpsc::channel(128);

    let handle = tokio::spawn(async move {
        let response = ingestor
            .ingest_stream(namespace.clone(), ReceiverStream::new(rx))
            .await
            .expect("ingest stream");

        insta::assert_yaml_snapshot!(ingestor.list_files(&namespace).await, {
            "[].path" => insta::dynamic_redaction(|value, _path| {
                assert!(value.as_str().unwrap().starts_with("tenants/test/namespaces/test-ns/folio/"));
                "[path]"
            })
        }, @r#"
        - path: "[path]"
          size: 1270
        - path: "[path]"
          size: 1270
        "#);

        response
    });

    let _ = tx
        .send(WriteBatchRequest::new(
            table.clone(),
            people_records(&table, &[(1, "Alice", 32), (2, "Bob", 27)]),
        ))
        .await;

    // Wait for the first batch to be flushed to the folio
    tokio::time::sleep(Duration::from_millis(500)).await;

    let _ = tx
        .send(WriteBatchRequest::new(
            table.clone(),
            people_records(&table, &[(1, "Alice", 32), (2, "Bob", 27)]),
        ))
        .await;

    drop(tx);

    let mut responses = handle.await.expect("ingest task");
    sort_by_batch_id(&mut responses);

    insta::assert_yaml_snapshot!(responses, {
        "[].timestamp" => "[timestamp]"
    }, @r#"
    - Accepted:
        batch_id: 0
        start_seqnum: 0
        end_seqnum: 1
        timestamp: "[timestamp]"
    - Accepted:
        batch_id: 2
        start_seqnum: 2
        end_seqnum: 3
        timestamp: "[timestamp]"
    "#);

    Ok(())
}

#[tokio::test]
async fn rejects_batch_when_object_store_put_fails() -> Result<()> {
    let table_metadata = MockTableMetadataService::new();

    let mut object_store = MockObjectStorePutOpts::new();
    object_store
        .expect_put_opts()
        .times(1)
        .returning(|_location, _payload, _opts| {
            Box::pin(async { Err(object_store::Error::NotImplemented) })
        });

    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store);
    let object_store_factory = Arc::new(MockObjectStoreFactory::new(object_store));

    let ingestor = TestIngestor::start_with_factory(table_metadata, object_store_factory).await;
    let namespace = initialize_test_namespace(&ingestor.cluster_meta).await;
    let table = initialize_test_table(&ingestor.cluster_meta, &namespace.name).await;

    let mut responses = ingestor
        .ingest(
            namespace.clone(),
            vec![
                WriteBatchRequest::new(table.clone(), people_records(&table, &[(1, "Alice", 32)]))
                    .with_batch_id(0),
                WriteBatchRequest::new(table.clone(), people_records(&table, &[(2, "Bob", 32)]))
                    .with_batch_id(1),
            ],
        )
        .await?;

    sort_by_batch_id(&mut responses);

    insta::assert_yaml_snapshot!(responses, @r#"
    - Rejected:
        batch_id: 0
        num_rows: 1
        reason: "object store error: failed to upload folio"
    - Rejected:
        batch_id: 1
        num_rows: 1
        reason: "object store error: failed to upload folio"
    "#);

    ingestor.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn ingests_single_message_with_partition_value() -> Result<()> {
    let mut table_metadata = MockTableMetadataService::new();
    table_metadata.expect_commit().times(1).returning({
        move |_namespace, batches| {
            insta::assert_yaml_snapshot!(batches, {
                "[].file_ref" => "[file-ref]",
                "[].timestamp" => "[timestamp]"
            }, @r#"
            - batch_id: 0
              table_name: tenants/test/namespaces/test-ns/tables/people_by_region
              partition_value:
                Int64: 100
              file_ref: "[file-ref]"
              page_offset_bytes: 0
              page_size_bytes: 1271
              timestamp: "[timestamp]"
              num_rows: 1
            "#);

            Ok(vec![CommittedBatch::Accepted(AcceptedBatchInfo {
                batch_id: 0,
                start_seqnum: 0,
                end_seqnum: 0,
                timestamp: SystemTime::now(),
            })])
        }
    });

    let ingestor = TestIngestor::start(table_metadata).await;
    let namespace = initialize_test_namespace(&ingestor.cluster_meta).await;
    let table = initialize_test_partitioned_table(&ingestor.cluster_meta, &namespace.name).await;
    let partition = PartitionValue::Int64(100);

    let mut responses = ingestor
        .ingest(
            namespace.clone(),
            vec![
                WriteBatchRequest::new(table.clone(), people_records(&table, &[(1, "Alice", 32)]))
                    .with_partition(partition.clone()),
            ],
        )
        .await?;

    sort_by_batch_id(&mut responses);

    insta::assert_yaml_snapshot!(responses, {
            "[].timestamp" => "[timestamp]"
    }, @r#"
    - Accepted:
        batch_id: 0
        start_seqnum: 0
        end_seqnum: 0
        timestamp: "[timestamp]"
    "#);

    insta::assert_yaml_snapshot!(ingestor.list_files(&namespace).await, {
        "[].path" => "[path]"
    }, @r#"
    - path: "[path]"
      size: 1271
    "#);

    ingestor.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn ingests_multiple_messages_with_partition_values() -> Result<()> {
    let mut table_metadata = MockTableMetadataService::new();
    table_metadata.expect_commit().times(1).returning({
        move |_namespace, batches| {
            insta::assert_yaml_snapshot!(batches, {
                "[].file_ref" => "[file-ref]",
                "[].page_offset_bytes" => "[page-seqnum-bytes]",
                "[].timestamp" => "[timestamp]"
            }, @r#"
            - batch_id: 0
              table_name: tenants/test/namespaces/test-ns/tables/people_by_region
              partition_value:
                Int64: 100
              file_ref: "[file-ref]"
              page_offset_bytes: "[page-seqnum-bytes]"
              page_size_bytes: 1309
              timestamp: "[timestamp]"
              num_rows: 2
            - batch_id: 1
              table_name: tenants/test/namespaces/test-ns/tables/people_by_region
              partition_value:
                Int64: 200
              file_ref: "[file-ref]"
              page_offset_bytes: "[page-seqnum-bytes]"
              page_size_bytes: 1281
              timestamp: "[timestamp]"
              num_rows: 1
            - batch_id: 2
              table_name: tenants/test/namespaces/test-ns/tables/people_by_region
              partition_value:
                Int64: 100
              file_ref: "[file-ref]"
              page_offset_bytes: "[page-seqnum-bytes]"
              page_size_bytes: 1309
              timestamp: "[timestamp]"
              num_rows: 1
            "#);

            Ok(vec![
                CommittedBatch::Accepted(AcceptedBatchInfo {
                    batch_id: 0,
                    start_seqnum: 0,
                    end_seqnum: 1,
                    timestamp: SystemTime::now(),
                }),
                CommittedBatch::Accepted(AcceptedBatchInfo {
                    batch_id: 1,
                    start_seqnum: 0,
                    end_seqnum: 0,
                    timestamp: SystemTime::now(),
                }),
                CommittedBatch::Accepted(AcceptedBatchInfo {
                    batch_id: 2,
                    start_seqnum: 2,
                    end_seqnum: 2,
                    timestamp: SystemTime::now(),
                }),
            ])
        }
    });

    let ingestor = TestIngestor::start(table_metadata).await;
    let namespace = initialize_test_namespace(&ingestor.cluster_meta).await;
    let table = initialize_test_partitioned_table(&ingestor.cluster_meta, &namespace.name).await;

    let partition_100 = PartitionValue::Int64(100);
    let partition_200 = PartitionValue::Int64(200);
    let mut responses = ingestor
        .ingest(
            namespace.clone(),
            vec![
                WriteBatchRequest::new(
                    table.clone(),
                    people_records(&table, &[(1, "Alice", 32), (2, "Bob", 27)]),
                )
                .with_partition(partition_100.clone()),
                WriteBatchRequest::new(
                    table.clone(),
                    people_records(&table, &[(3, "Charlie", 99)]),
                )
                .with_partition(partition_200.clone())
                .with_batch_id(1),
                WriteBatchRequest::new(table.clone(), people_records(&table, &[(4, "Dylan", 75)]))
                    .with_partition(partition_100.clone())
                    .with_batch_id(2),
            ],
        )
        .await?;

    sort_by_batch_id(&mut responses);

    insta::assert_yaml_snapshot!(responses, {
            "[].timestamp" => "[timestamp]"
    }, @r#"
    - Accepted:
        batch_id: 0
        start_seqnum: 0
        end_seqnum: 1
        timestamp: "[timestamp]"
    - Accepted:
        batch_id: 1
        start_seqnum: 0
        end_seqnum: 0
        timestamp: "[timestamp]"
    - Accepted:
        batch_id: 2
        start_seqnum: 2
        end_seqnum: 2
        timestamp: "[timestamp]"
    "#);

    insta::assert_yaml_snapshot!(ingestor.list_files(&namespace).await, {
        "[].path" => "[path]"
    }, @r#"
    - path: "[path]"
      size: 2590
    "#);

    ingestor.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn rejects_batch_with_mismatched_schema() -> Result<()> {
    let mut table_metadata = MockTableMetadataService::new();
    table_metadata.expect_commit().times(1).returning({
        move |_namespace, batches| {
            insta::assert_yaml_snapshot!(batches, {
                "[].file_ref" => "[file-ref]",
                "[].timestamp" => "[timestamp]"
            }, @r#"
            - batch_id: 0
              table_name: tenants/test/namespaces/test-ns/tables/people
              partition_value: ~
              file_ref: "[file-ref]"
              page_offset_bytes: 0
              page_size_bytes: 1300
              timestamp: "[timestamp]"
              num_rows: 2
            - batch_id: 2
              table_name: tenants/test/namespaces/test-ns/tables/people
              partition_value: ~
              file_ref: "[file-ref]"
              page_offset_bytes: 0
              page_size_bytes: 1300
              timestamp: "[timestamp]"
              num_rows: 1
            "#);

            Ok(vec![
                CommittedBatch::Accepted(AcceptedBatchInfo {
                    batch_id: 0,
                    start_seqnum: 0,
                    end_seqnum: 1,
                    timestamp: SystemTime::now(),
                }),
                CommittedBatch::Accepted(AcceptedBatchInfo {
                    batch_id: 2,
                    start_seqnum: 2,
                    end_seqnum: 2,
                    timestamp: SystemTime::now(),
                }),
            ])
        }
    });

    let ingestor = TestIngestor::start(table_metadata).await;
    let namespace = initialize_test_namespace(&ingestor.cluster_meta).await;
    let table = initialize_test_table(&ingestor.cluster_meta, &namespace.name).await;

    let mut responses = ingestor
        .ingest(
            namespace.clone(),
            vec![
                WriteBatchRequest::new(
                    table.clone(),
                    people_records(&table, &[(1, "Alice", 32), (2, "Bob", 27)]),
                ),
                WriteBatchRequest::new(table.clone(), common::mismatched_records(2))
                    .with_batch_id(1),
                WriteBatchRequest::new(
                    table.clone(),
                    people_records(&table, &[(3, "Charlie", 99)]),
                )
                .with_batch_id(2),
            ],
        )
        .await?;

    sort_by_batch_id(&mut responses);

    insta::assert_yaml_snapshot!(responses, {
            "[].timestamp" => "[timestamp]"
    }, @r#"
    - Accepted:
        batch_id: 0
        start_seqnum: 0
        end_seqnum: 1
        timestamp: "[timestamp]"
    - Rejected:
        batch_id: 1
        num_rows: 2
        reason: "validation error: batch schema does not match writer's schema"
    - Accepted:
        batch_id: 2
        start_seqnum: 2
        end_seqnum: 2
        timestamp: "[timestamp]"
    "#);

    insta::assert_yaml_snapshot!(ingestor.list_files(&namespace).await, {
        "[].path" => "[path]"
    }, @r#"
    - path: "[path]"
      size: 1300
    "#);

    ingestor.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn rejects_batch_when_table_metadata_rejects_it() -> Result<()> {
    let mut table_metadata = MockTableMetadataService::new();
    table_metadata.expect_commit().times(1).returning({
        move |_namespace, batches| {
            insta::assert_yaml_snapshot!(batches, {
                "[].file_ref" => "[file-ref]",
                "[].timestamp" => "[timestamp]"
            }, @r#"
            - batch_id: 0
              table_name: tenants/test/namespaces/test-ns/tables/people
              partition_value: ~
              file_ref: "[file-ref]"
              page_offset_bytes: 0
              page_size_bytes: 1329
              timestamp: "[timestamp]"
              num_rows: 2
            - batch_id: 1
              table_name: tenants/test/namespaces/test-ns/tables/people
              partition_value: ~
              file_ref: "[file-ref]"
              page_offset_bytes: 0
              page_size_bytes: 1329
              timestamp: "[timestamp]"
              num_rows: 2
            - batch_id: 2
              table_name: tenants/test/namespaces/test-ns/tables/people
              partition_value: ~
              file_ref: "[file-ref]"
              page_offset_bytes: 0
              page_size_bytes: 1329
              timestamp: "[timestamp]"
              num_rows: 1
            "#);

            Ok(vec![
                CommittedBatch::Accepted(AcceptedBatchInfo {
                    batch_id: 0,
                    start_seqnum: 0,
                    end_seqnum: 1,
                    timestamp: SystemTime::now(),
                }),
                CommittedBatch::Rejected(RejectedBatchInfo {
                    batch_id: 1,
                    num_rows: 2,
                    reason: "mock rejection".to_string(),
                }),
                CommittedBatch::Accepted(AcceptedBatchInfo {
                    batch_id: 2,
                    start_seqnum: 2,
                    end_seqnum: 2,
                    timestamp: SystemTime::now(),
                }),
            ])
        }
    });

    let ingestor = TestIngestor::start(table_metadata).await;
    let namespace = initialize_test_namespace(&ingestor.cluster_meta).await;
    let table = initialize_test_table(&ingestor.cluster_meta, &namespace.name).await;

    let mut responses = ingestor
        .ingest(
            namespace.clone(),
            vec![
                WriteBatchRequest::new(
                    table.clone(),
                    people_records(&table, &[(1, "Alice", 32), (2, "Bob", 27)]),
                ),
                WriteBatchRequest::new(
                    table.clone(),
                    people_records(&table, &[(3, "Charlie", 99), (4, "Dylan", 75)]),
                )
                .with_batch_id(1),
                WriteBatchRequest::new(table.clone(), people_records(&table, &[(5, "Eve", 45)]))
                    .with_batch_id(2),
            ],
        )
        .await?;

    sort_by_batch_id(&mut responses);

    insta::assert_yaml_snapshot!(responses, {
            "[].timestamp" => "[timestamp]"
    }, @r#"
    - Accepted:
        batch_id: 0
        start_seqnum: 0
        end_seqnum: 1
        timestamp: "[timestamp]"
    - Rejected:
        batch_id: 1
        num_rows: 2
        reason: mock rejection
    - Accepted:
        batch_id: 2
        start_seqnum: 2
        end_seqnum: 2
        timestamp: "[timestamp]"
    "#);

    insta::assert_yaml_snapshot!(ingestor.list_files(&namespace).await, {
        "[].path" => "[path]"
    }, @r#"
    - path: "[path]"
      size: 1329
    "#);

    ingestor.shutdown().await;
    Ok(())
}
