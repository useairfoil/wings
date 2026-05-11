mod common;

use common::{
    MockLogMetadataService, TestIngestor, assert_accepted_batch, initialize_test_namespace,
    initialize_test_partitioned_topic, initialize_test_topic, people_records, write_request,
};
use wings_control_plane_core::log_metadata::{
    AcceptedBatchInfo, CommitBatchRequest, CommittedBatch,
};
use wings_ingestor_core::Result;
use wings_resources::{NamespaceName, PartitionValue, TenantName, TopicName};

#[derive(Clone)]
struct ExpectedBatch {
    topic_name: TopicName,
    partition_value: Option<PartitionValue>,
    num_rows: u32,
}

#[tokio::test]
async fn ingests_single_message_without_partition_value() -> Result<()> {
    let namespace_name = test_namespace_name();
    let topic_name = TopicName::new_unchecked("people", namespace_name.clone());
    let mut log_meta = MockLogMetadataService::new();
    log_meta.expect_commit().times(1).returning({
        let namespace_name = namespace_name.clone();
        let expected_batches = vec![ExpectedBatch {
            topic_name: topic_name.clone(),
            partition_value: None,
            num_rows: 1,
        }];
        let commit_response = committed_batches(&[(0, 0)]);
        move |namespace, batches| {
            assert_commit_request(&namespace_name, &expected_batches, &namespace, &batches);
            Ok(commit_response.clone())
        }
    });

    let ingestor = TestIngestor::start(log_meta).await;
    let namespace = initialize_test_namespace(&ingestor.cluster_meta).await;
    let topic = initialize_test_topic(&ingestor.cluster_meta, &namespace.name).await;

    let responses = ingestor
        .ingest(
            namespace.clone(),
            vec![write_request(
                topic.clone(),
                None,
                people_records(&topic, &[(1, "Alice", 32)]),
            )],
        )
        .await?;

    assert_eq!(responses.len(), 1);
    assert_accepted_batch(&responses[0], 0, 0);
    ingestor.assert_folio_written(&namespace).await;

    ingestor.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn ingests_multiple_messages_without_partition_values() -> Result<()> {
    let namespace_name = test_namespace_name();
    let topic_name = TopicName::new_unchecked("people", namespace_name.clone());
    let mut log_meta = MockLogMetadataService::new();
    log_meta.expect_commit().times(1).returning({
        let namespace_name = namespace_name.clone();
        let expected_batches = vec![
            ExpectedBatch {
                topic_name: topic_name.clone(),
                partition_value: None,
                num_rows: 2,
            },
            ExpectedBatch {
                topic_name: topic_name.clone(),
                partition_value: None,
                num_rows: 2,
            },
        ];
        let commit_response = committed_batches(&[(0, 1), (2, 3)]);
        move |namespace, batches| {
            assert_commit_request(&namespace_name, &expected_batches, &namespace, &batches);
            Ok(commit_response.clone())
        }
    });

    let ingestor = TestIngestor::start(log_meta).await;
    let namespace = initialize_test_namespace(&ingestor.cluster_meta).await;
    let topic = initialize_test_topic(&ingestor.cluster_meta, &namespace.name).await;

    let responses = ingestor
        .ingest(
            namespace.clone(),
            vec![
                write_request(
                    topic.clone(),
                    None,
                    people_records(&topic, &[(1, "Alice", 32), (2, "Bob", 27)]),
                ),
                write_request(
                    topic.clone(),
                    None,
                    people_records(&topic, &[(3, "Charlie", 99), (4, "Dylan", 75)]),
                ),
            ],
        )
        .await?;

    assert_eq!(responses.len(), 2);
    assert_accepted_batch(&responses[0], 0, 1);
    assert_accepted_batch(&responses[1], 2, 3);
    ingestor.assert_folio_written(&namespace).await;

    ingestor.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn ingests_single_message_with_partition_value() -> Result<()> {
    let namespace_name = test_namespace_name();
    let topic_name = TopicName::new_unchecked("people_by_region", namespace_name.clone());
    let partition = PartitionValue::Int64(100);
    let mut log_meta = MockLogMetadataService::new();
    log_meta.expect_commit().times(1).returning({
        let namespace_name = namespace_name.clone();
        let expected_batches = vec![ExpectedBatch {
            topic_name: topic_name.clone(),
            partition_value: Some(partition.clone()),
            num_rows: 1,
        }];
        let commit_response = committed_batches(&[(0, 0)]);
        move |namespace, batches| {
            assert_commit_request(&namespace_name, &expected_batches, &namespace, &batches);
            Ok(commit_response.clone())
        }
    });

    let ingestor = TestIngestor::start(log_meta).await;
    let namespace = initialize_test_namespace(&ingestor.cluster_meta).await;
    let topic = initialize_test_partitioned_topic(&ingestor.cluster_meta, &namespace.name).await;
    let partition = PartitionValue::Int64(100);

    let responses = ingestor
        .ingest(
            namespace.clone(),
            vec![write_request(
                topic.clone(),
                Some(partition.clone()),
                people_records(&topic, &[(1, "Alice", 32)]),
            )],
        )
        .await?;

    assert_eq!(responses.len(), 1);
    assert_accepted_batch(&responses[0], 0, 0);
    ingestor.assert_folio_written(&namespace).await;

    ingestor.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn ingests_multiple_messages_with_partition_values() -> Result<()> {
    let namespace_name = test_namespace_name();
    let topic_name = TopicName::new_unchecked("people_by_region", namespace_name.clone());
    let partition_100 = PartitionValue::Int64(100);
    let partition_200 = PartitionValue::Int64(200);
    let mut log_meta = MockLogMetadataService::new();
    log_meta.expect_commit().times(1).returning({
        let namespace_name = namespace_name.clone();
        let expected_batches = vec![
            ExpectedBatch {
                topic_name: topic_name.clone(),
                partition_value: Some(partition_100.clone()),
                num_rows: 2,
            },
            ExpectedBatch {
                topic_name: topic_name.clone(),
                partition_value: Some(partition_200.clone()),
                num_rows: 1,
            },
            ExpectedBatch {
                topic_name: topic_name.clone(),
                partition_value: Some(partition_100.clone()),
                num_rows: 1,
            },
        ];
        let commit_response = committed_batches(&[(0, 1), (0, 0), (2, 2)]);
        move |namespace, batches| {
            assert_commit_request(&namespace_name, &expected_batches, &namespace, &batches);
            Ok(commit_response.clone())
        }
    });

    let ingestor = TestIngestor::start(log_meta).await;
    let namespace = initialize_test_namespace(&ingestor.cluster_meta).await;
    let topic = initialize_test_partitioned_topic(&ingestor.cluster_meta, &namespace.name).await;

    let partition_100 = PartitionValue::Int64(100);
    let partition_200 = PartitionValue::Int64(200);
    let responses = ingestor
        .ingest(
            namespace.clone(),
            vec![
                write_request(
                    topic.clone(),
                    Some(partition_100.clone()),
                    people_records(&topic, &[(1, "Alice", 32), (2, "Bob", 27)]),
                ),
                write_request(
                    topic.clone(),
                    Some(partition_200.clone()),
                    people_records(&topic, &[(3, "Charlie", 99)]),
                ),
                write_request(
                    topic.clone(),
                    Some(partition_100.clone()),
                    people_records(&topic, &[(4, "Dylan", 75)]),
                ),
            ],
        )
        .await?;

    assert_eq!(responses.len(), 3);
    assert_accepted_batch(&responses[0], 0, 1);
    assert_accepted_batch(&responses[1], 0, 0);
    assert_accepted_batch(&responses[2], 2, 2);
    ingestor.assert_folio_written(&namespace).await;

    ingestor.shutdown().await;
    Ok(())
}

fn test_namespace_name() -> NamespaceName {
    let tenant_name = TenantName::new_unchecked("test");
    NamespaceName::new_unchecked("test-ns", tenant_name)
}

fn assert_commit_request(
    expected_namespace: &NamespaceName,
    expected_batches: &[ExpectedBatch],
    namespace: &NamespaceName,
    batches: &[CommitBatchRequest],
) {
    assert_eq!(namespace, expected_namespace);
    assert_eq!(batches.len(), expected_batches.len());

    for (batch, expected_batch) in batches.iter().zip(expected_batches) {
        assert_eq!(batch.topic_name, expected_batch.topic_name);
        assert_eq!(batch.partition_value, expected_batch.partition_value);
        assert_eq!(batch.num_rows, expected_batch.num_rows);
        assert!(
            batch
                .file_ref
                .starts_with(&format!("{expected_namespace}/folio/"))
        );
        assert!(batch.file_ref.ends_with(".folio"));
        assert!(batch.batch_size_bytes > 0);
    }

    if batches.len() == 1 {
        assert_eq!(batches[0].offset_bytes, 0);
    } else {
        let first_file_ref = &batches[0].file_ref;
        assert!(
            batches
                .iter()
                .all(|batch| batch.file_ref == *first_file_ref)
        );
    }
}

fn committed_batches(offsets: &[(u64, u64)]) -> Vec<CommittedBatch> {
    offsets
        .iter()
        .map(|(start_offset, end_offset)| {
            CommittedBatch::Accepted(AcceptedBatchInfo {
                start_offset: *start_offset,
                end_offset: *end_offset,
                timestamp: std::time::SystemTime::now(),
            })
        })
        .collect()
}
