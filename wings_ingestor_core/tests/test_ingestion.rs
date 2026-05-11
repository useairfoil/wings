mod common;

use std::time::SystemTime;

use common::{
    MockLogMetadataService, TestIngestor, initialize_test_namespace,
    initialize_test_partitioned_topic, initialize_test_topic, people_records, sort_by_batch_id,
};
use wings_control_plane_core::log_metadata::{
    AcceptedBatchInfo, CommittedBatch, RejectedBatchInfo,
};
use wings_ingestor_core::{Result, WriteBatchRequest};
use wings_resources::PartitionValue;

#[tokio::test]
async fn ingests_single_message_without_partition_value() -> Result<()> {
    let mut log_meta = MockLogMetadataService::new();
    log_meta.expect_commit().times(1).returning({
        move |_namespace, batches| {
            insta::assert_yaml_snapshot!(batches, {
                "[].file_ref" => "[file-ref]",
                "[].timestamp" => "[timestamp]"
            }, @r#"
            - batch_id: 0
              topic_name: tenants/test/namespaces/test-ns/topics/people
              partition_value: ~
              file_ref: "[file-ref]"
              page_offset_bytes: 0
              page_size_bytes: 1256
              timestamp: "[timestamp]"
              num_rows: 1
            "#);
            Ok(vec![CommittedBatch::Accepted(AcceptedBatchInfo {
                batch_id: 0,
                start_offset: 0,
                end_offset: 0,
                timestamp: SystemTime::now(),
            })])
        }
    });

    let ingestor = TestIngestor::start(log_meta).await;
    let namespace = initialize_test_namespace(&ingestor.cluster_meta).await;
    let topic = initialize_test_topic(&ingestor.cluster_meta, &namespace.name).await;

    let mut responses = ingestor
        .ingest(
            namespace.clone(),
            vec![WriteBatchRequest::new(
                topic.clone(),
                people_records(&topic, &[(1, "Alice", 32)]),
            )],
        )
        .await?;

    sort_by_batch_id(&mut responses);

    ingestor.assert_folio_written(&namespace).await;

    insta::assert_yaml_snapshot!(responses, {
        "[].timestamp" => "[timestamp]"
    }, @r#"
    - Accepted:
        batch_id: 0
        start_offset: 0
        end_offset: 0
        timestamp: "[timestamp]"
    "#);

    ingestor.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn ingests_multiple_messages_without_partition_values() -> Result<()> {
    let mut log_meta = MockLogMetadataService::new();
    log_meta.expect_commit().times(1).returning({
        move |_namespace, batches| {
            insta::assert_yaml_snapshot!(batches, {
                "[].file_ref" => "[file-ref]",
                "[].timestamp" => "[timestamp]"
            }, @r#"
            - batch_id: 0
              topic_name: tenants/test/namespaces/test-ns/topics/people
              partition_value: ~
              file_ref: "[file-ref]"
              page_offset_bytes: 0
              page_size_bytes: 1315
              timestamp: "[timestamp]"
              num_rows: 2
            - batch_id: 1
              topic_name: tenants/test/namespaces/test-ns/topics/people
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
                    start_offset: 0,
                    end_offset: 1,
                    timestamp: SystemTime::now(),
                }),
                CommittedBatch::Accepted(AcceptedBatchInfo {
                    batch_id: 2,
                    start_offset: 2,
                    end_offset: 3,
                    timestamp: SystemTime::now(),
                }),
            ])
        }
    });

    let ingestor = TestIngestor::start(log_meta).await;
    let namespace = initialize_test_namespace(&ingestor.cluster_meta).await;
    let topic = initialize_test_topic(&ingestor.cluster_meta, &namespace.name).await;

    let mut responses = ingestor
        .ingest(
            namespace.clone(),
            vec![
                WriteBatchRequest::new(
                    topic.clone(),
                    people_records(&topic, &[(1, "Alice", 32), (2, "Bob", 27)]),
                ),
                WriteBatchRequest::new(
                    topic.clone(),
                    people_records(&topic, &[(3, "Charlie", 99), (4, "Dylan", 75)]),
                )
                .with_batch_id(1),
            ],
        )
        .await?;

    sort_by_batch_id(&mut responses);

    ingestor.assert_folio_written(&namespace).await;

    insta::assert_yaml_snapshot!(responses, {
        "[].timestamp" => "[timestamp]"
    }, @r#"
    - Accepted:
        batch_id: 0
        start_offset: 0
        end_offset: 1
        timestamp: "[timestamp]"
    - Accepted:
        batch_id: 1
        start_offset: 2
        end_offset: 3
        timestamp: "[timestamp]"
    "#);

    ingestor.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn ingests_single_message_with_partition_value() -> Result<()> {
    let mut log_meta = MockLogMetadataService::new();
    log_meta.expect_commit().times(1).returning({
        move |_namespace, batches| {
            insta::assert_yaml_snapshot!(batches, {
                "[].file_ref" => "[file-ref]",
                "[].timestamp" => "[timestamp]"
            }, @r#"
            - batch_id: 0
              topic_name: tenants/test/namespaces/test-ns/topics/people_by_region
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
                start_offset: 0,
                end_offset: 0,
                timestamp: SystemTime::now(),
            })])
        }
    });

    let ingestor = TestIngestor::start(log_meta).await;
    let namespace = initialize_test_namespace(&ingestor.cluster_meta).await;
    let topic = initialize_test_partitioned_topic(&ingestor.cluster_meta, &namespace.name).await;
    let partition = PartitionValue::Int64(100);

    let mut responses = ingestor
        .ingest(
            namespace.clone(),
            vec![
                WriteBatchRequest::new(topic.clone(), people_records(&topic, &[(1, "Alice", 32)]))
                    .with_partition(partition.clone()),
            ],
        )
        .await?;

    sort_by_batch_id(&mut responses);

    ingestor.assert_folio_written(&namespace).await;

    insta::assert_yaml_snapshot!(responses, {
            "[].timestamp" => "[timestamp]"
    }, @r#"
    - Accepted:
        batch_id: 0
        start_offset: 0
        end_offset: 0
        timestamp: "[timestamp]"
    "#);

    ingestor.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn ingests_multiple_messages_with_partition_values() -> Result<()> {
    let mut log_meta = MockLogMetadataService::new();
    log_meta.expect_commit().times(1).returning({
        move |_namespace, batches| {
            insta::assert_yaml_snapshot!(batches, {
                "[].file_ref" => "[file-ref]",
                "[].timestamp" => "[timestamp]"
            }, @r#"
            - batch_id: 0
              topic_name: tenants/test/namespaces/test-ns/topics/people_by_region
              partition_value:
                Int64: 100
              file_ref: "[file-ref]"
              page_offset_bytes: 0
              page_size_bytes: 1309
              timestamp: "[timestamp]"
              num_rows: 2
            - batch_id: 1
              topic_name: tenants/test/namespaces/test-ns/topics/people_by_region
              partition_value:
                Int64: 200
              file_ref: "[file-ref]"
              page_offset_bytes: 1309
              page_size_bytes: 1281
              timestamp: "[timestamp]"
              num_rows: 1
            - batch_id: 2
              topic_name: tenants/test/namespaces/test-ns/topics/people_by_region
              partition_value:
                Int64: 100
              file_ref: "[file-ref]"
              page_offset_bytes: 0
              page_size_bytes: 1309
              timestamp: "[timestamp]"
              num_rows: 1
            "#);

            Ok(vec![
                CommittedBatch::Accepted(AcceptedBatchInfo {
                    batch_id: 0,
                    start_offset: 0,
                    end_offset: 1,
                    timestamp: SystemTime::now(),
                }),
                CommittedBatch::Accepted(AcceptedBatchInfo {
                    batch_id: 1,
                    start_offset: 0,
                    end_offset: 0,
                    timestamp: SystemTime::now(),
                }),
                CommittedBatch::Accepted(AcceptedBatchInfo {
                    batch_id: 2,
                    start_offset: 2,
                    end_offset: 2,
                    timestamp: SystemTime::now(),
                }),
            ])
        }
    });

    let ingestor = TestIngestor::start(log_meta).await;
    let namespace = initialize_test_namespace(&ingestor.cluster_meta).await;
    let topic = initialize_test_partitioned_topic(&ingestor.cluster_meta, &namespace.name).await;

    let partition_100 = PartitionValue::Int64(100);
    let partition_200 = PartitionValue::Int64(200);
    let mut responses = ingestor
        .ingest(
            namespace.clone(),
            vec![
                WriteBatchRequest::new(
                    topic.clone(),
                    people_records(&topic, &[(1, "Alice", 32), (2, "Bob", 27)]),
                )
                .with_partition(partition_100.clone()),
                WriteBatchRequest::new(
                    topic.clone(),
                    people_records(&topic, &[(3, "Charlie", 99)]),
                )
                .with_partition(partition_200.clone())
                .with_batch_id(1),
                WriteBatchRequest::new(topic.clone(), people_records(&topic, &[(4, "Dylan", 75)]))
                    .with_partition(partition_100.clone())
                    .with_batch_id(2),
            ],
        )
        .await?;

    sort_by_batch_id(&mut responses);

    ingestor.assert_folio_written(&namespace).await;

    insta::assert_yaml_snapshot!(responses, {
            "[].timestamp" => "[timestamp]"
    }, @r#"
    - Accepted:
        batch_id: 0
        start_offset: 0
        end_offset: 1
        timestamp: "[timestamp]"
    - Accepted:
        batch_id: 1
        start_offset: 0
        end_offset: 0
        timestamp: "[timestamp]"
    - Accepted:
        batch_id: 2
        start_offset: 2
        end_offset: 2
        timestamp: "[timestamp]"
    "#);

    ingestor.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn rejects_batch_with_mismatched_schema() -> Result<()> {
    let mut log_meta = MockLogMetadataService::new();
    log_meta.expect_commit().times(1).returning({
        move |_namespace, batches| {
            insta::assert_yaml_snapshot!(batches, {
                "[].file_ref" => "[file-ref]",
                "[].timestamp" => "[timestamp]"
            }, @r#"
            - batch_id: 0
              topic_name: tenants/test/namespaces/test-ns/topics/people
              partition_value: ~
              file_ref: "[file-ref]"
              page_offset_bytes: 0
              page_size_bytes: 1300
              timestamp: "[timestamp]"
              num_rows: 2
            - batch_id: 2
              topic_name: tenants/test/namespaces/test-ns/topics/people
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
                    start_offset: 0,
                    end_offset: 1,
                    timestamp: SystemTime::now(),
                }),
                CommittedBatch::Accepted(AcceptedBatchInfo {
                    batch_id: 2,
                    start_offset: 2,
                    end_offset: 2,
                    timestamp: SystemTime::now(),
                }),
            ])
        }
    });

    let ingestor = TestIngestor::start(log_meta).await;
    let namespace = initialize_test_namespace(&ingestor.cluster_meta).await;
    let topic = initialize_test_topic(&ingestor.cluster_meta, &namespace.name).await;

    let mut responses = ingestor
        .ingest(
            namespace.clone(),
            vec![
                WriteBatchRequest::new(
                    topic.clone(),
                    people_records(&topic, &[(1, "Alice", 32), (2, "Bob", 27)]),
                ),
                WriteBatchRequest::new(topic.clone(), common::mismatched_records(2))
                    .with_batch_id(1),
                WriteBatchRequest::new(
                    topic.clone(),
                    people_records(&topic, &[(3, "Charlie", 99)]),
                )
                .with_batch_id(2),
            ],
        )
        .await?;

    sort_by_batch_id(&mut responses);

    ingestor.assert_folio_written(&namespace).await;

    insta::assert_yaml_snapshot!(responses, {
            "[].timestamp" => "[timestamp]"
    }, @r#"
    - Accepted:
        batch_id: 0
        start_offset: 0
        end_offset: 1
        timestamp: "[timestamp]"
    - Rejected:
        batch_id: 1
        num_rows: 2
        reason: "validation error: batch schema does not match writer's schema"
    - Accepted:
        batch_id: 2
        start_offset: 2
        end_offset: 2
        timestamp: "[timestamp]"
    "#);

    ingestor.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn rejects_batch_when_log_metadata_rejects_it() -> Result<()> {
    let mut log_meta = MockLogMetadataService::new();
    log_meta.expect_commit().times(1).returning({
        move |_namespace, batches| {
            insta::assert_yaml_snapshot!(batches, {
                "[].file_ref" => "[file-ref]",
                "[].timestamp" => "[timestamp]"
            }, @r#"
            - batch_id: 0
              topic_name: tenants/test/namespaces/test-ns/topics/people
              partition_value: ~
              file_ref: "[file-ref]"
              page_offset_bytes: 0
              page_size_bytes: 1329
              timestamp: "[timestamp]"
              num_rows: 2
            - batch_id: 1
              topic_name: tenants/test/namespaces/test-ns/topics/people
              partition_value: ~
              file_ref: "[file-ref]"
              page_offset_bytes: 0
              page_size_bytes: 1329
              timestamp: "[timestamp]"
              num_rows: 2
            - batch_id: 2
              topic_name: tenants/test/namespaces/test-ns/topics/people
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
                    start_offset: 0,
                    end_offset: 1,
                    timestamp: SystemTime::now(),
                }),
                CommittedBatch::Rejected(RejectedBatchInfo {
                    batch_id: 1,
                    num_rows: 2,
                    reason: "mock rejection".to_string(),
                }),
                CommittedBatch::Accepted(AcceptedBatchInfo {
                    batch_id: 2,
                    start_offset: 2,
                    end_offset: 2,
                    timestamp: SystemTime::now(),
                }),
            ])
        }
    });

    let ingestor = TestIngestor::start(log_meta).await;
    let namespace = initialize_test_namespace(&ingestor.cluster_meta).await;
    let topic = initialize_test_topic(&ingestor.cluster_meta, &namespace.name).await;

    let mut responses = ingestor
        .ingest(
            namespace.clone(),
            vec![
                WriteBatchRequest::new(
                    topic.clone(),
                    people_records(&topic, &[(1, "Alice", 32), (2, "Bob", 27)]),
                ),
                WriteBatchRequest::new(
                    topic.clone(),
                    people_records(&topic, &[(3, "Charlie", 99), (4, "Dylan", 75)]),
                )
                .with_batch_id(1),
                WriteBatchRequest::new(topic.clone(), people_records(&topic, &[(5, "Eve", 45)]))
                    .with_batch_id(2),
            ],
        )
        .await?;

    sort_by_batch_id(&mut responses);

    ingestor.assert_folio_written(&namespace).await;

    insta::assert_yaml_snapshot!(responses, {
            "[].timestamp" => "[timestamp]"
    }, @r#"
    - Accepted:
        batch_id: 0
        start_offset: 0
        end_offset: 1
        timestamp: "[timestamp]"
    - Rejected:
        batch_id: 1
        num_rows: 2
        reason: mock rejection
    - Accepted:
        batch_id: 2
        start_offset: 2
        end_offset: 2
        timestamp: "[timestamp]"
    "#);

    ingestor.shutdown().await;
    Ok(())
}
