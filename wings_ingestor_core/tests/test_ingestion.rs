mod common;

use common::{
    MockLogMetadataService, TestIngestor, assert_accepted_batch, initialize_test_namespace,
    initialize_test_partitioned_topic, initialize_test_topic, people_records, write_request,
};
use wings_control_plane_core::log_metadata::{
    AcceptedBatchInfo, CommitPageRequest, CommitPageResponse, CommittedBatch,
};
use wings_ingestor_core::Result;
use wings_resources::{NamespaceName, PartitionValue, TenantName, TopicName};

#[derive(Clone)]
struct ExpectedPage {
    topic_name: TopicName,
    partition_value: Option<PartitionValue>,
    batch_rows: Vec<u32>,
}

#[tokio::test]
async fn ingests_single_message_without_partition_value() -> Result<()> {
    let namespace_name = test_namespace_name();
    let topic_name = TopicName::new_unchecked("people", namespace_name.clone());
    let mut log_meta = MockLogMetadataService::new();
    log_meta.expect_commit_folio().times(1).returning({
        let namespace_name = namespace_name.clone();
        let expected_pages = vec![ExpectedPage {
            topic_name: topic_name.clone(),
            partition_value: None,
            batch_rows: vec![1],
        }];
        let commit_response = vec![commit_page_response(topic_name, None, &[(0, 0)])];
        move |namespace, file_ref, pages| {
            assert_commit_request(
                &namespace_name,
                &expected_pages,
                &namespace,
                &file_ref,
                pages,
            );
            Ok(commit_response.clone())
        }
    });

    let ingestor = TestIngestor::start(log_meta).await;
    let namespace = initialize_test_namespace(&ingestor.cluster_meta).await;
    let topic = initialize_test_topic(&ingestor.cluster_meta, &namespace.name).await;

    let responses = ingestor
        .ingest(
            namespace,
            vec![write_request(
                topic.clone(),
                None,
                people_records(&topic, &[(1, "Alice", 32)]),
            )],
        )
        .await?;

    assert_eq!(responses.len(), 1);
    assert_accepted_batch(&responses[0], 0, 0);

    ingestor.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn ingests_multiple_messages_without_partition_values() -> Result<()> {
    let namespace_name = test_namespace_name();
    let topic_name = TopicName::new_unchecked("people", namespace_name.clone());
    let mut log_meta = MockLogMetadataService::new();
    log_meta.expect_commit_folio().times(1).returning({
        let namespace_name = namespace_name.clone();
        let expected_pages = vec![ExpectedPage {
            topic_name: topic_name.clone(),
            partition_value: None,
            batch_rows: vec![2, 2],
        }];
        let commit_response = vec![commit_page_response(topic_name, None, &[(0, 1), (2, 3)])];
        move |namespace, file_ref, pages| {
            assert_commit_request(
                &namespace_name,
                &expected_pages,
                &namespace,
                &file_ref,
                pages,
            );
            Ok(commit_response.clone())
        }
    });

    let ingestor = TestIngestor::start(log_meta).await;
    let namespace = initialize_test_namespace(&ingestor.cluster_meta).await;
    let topic = initialize_test_topic(&ingestor.cluster_meta, &namespace.name).await;

    let responses = ingestor
        .ingest(
            namespace,
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

    ingestor.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn ingests_single_message_with_partition_value() -> Result<()> {
    let namespace_name = test_namespace_name();
    let topic_name = TopicName::new_unchecked("people_by_region", namespace_name.clone());
    let partition = PartitionValue::Int64(100);
    let mut log_meta = MockLogMetadataService::new();
    log_meta.expect_commit_folio().times(1).returning({
        let namespace_name = namespace_name.clone();
        let expected_pages = vec![ExpectedPage {
            topic_name: topic_name.clone(),
            partition_value: Some(partition.clone()),
            batch_rows: vec![1],
        }];
        let commit_response = vec![commit_page_response(
            topic_name,
            Some(partition.clone()),
            &[(0, 0)],
        )];
        move |namespace, file_ref, pages| {
            assert_commit_request(
                &namespace_name,
                &expected_pages,
                &namespace,
                &file_ref,
                pages,
            );
            Ok(commit_response.clone())
        }
    });

    let ingestor = TestIngestor::start(log_meta).await;
    let namespace = initialize_test_namespace(&ingestor.cluster_meta).await;
    let topic = initialize_test_partitioned_topic(&ingestor.cluster_meta, &namespace.name).await;
    let partition = PartitionValue::Int64(100);

    let responses = ingestor
        .ingest(
            namespace,
            vec![write_request(
                topic.clone(),
                Some(partition.clone()),
                people_records(&topic, &[(1, "Alice", 32)]),
            )],
        )
        .await?;

    assert_eq!(responses.len(), 1);
    assert_accepted_batch(&responses[0], 0, 0);

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
    log_meta.expect_commit_folio().times(1).returning({
        let namespace_name = namespace_name.clone();
        let expected_pages = vec![
            ExpectedPage {
                topic_name: topic_name.clone(),
                partition_value: Some(partition_100.clone()),
                batch_rows: vec![2, 1],
            },
            ExpectedPage {
                topic_name: topic_name.clone(),
                partition_value: Some(partition_200.clone()),
                batch_rows: vec![1],
            },
        ];
        let commit_response = vec![
            commit_page_response(
                topic_name.clone(),
                Some(partition_100.clone()),
                &[(0, 1), (2, 2)],
            ),
            commit_page_response(topic_name, Some(partition_200.clone()), &[(0, 0)]),
        ];
        move |namespace, file_ref, pages| {
            assert_commit_request(
                &namespace_name,
                &expected_pages,
                &namespace,
                &file_ref,
                pages,
            );
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
            namespace,
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

    ingestor.shutdown().await;
    Ok(())
}

fn test_namespace_name() -> NamespaceName {
    let tenant_name = TenantName::new_unchecked("test");
    NamespaceName::new_unchecked("test-ns", tenant_name)
}

fn assert_commit_request(
    expected_namespace: &NamespaceName,
    expected_pages: &[ExpectedPage],
    namespace: &NamespaceName,
    file_ref: &str,
    pages: &[CommitPageRequest],
) {
    assert_eq!(namespace, expected_namespace);
    assert!(file_ref.starts_with(&format!("{expected_namespace}/folio/")));
    assert!(file_ref.ends_with(".folio"));
    assert_eq!(pages.len(), expected_pages.len());

    for (page, expected_page) in pages.iter().zip(expected_pages) {
        let expected_num_rows = expected_page.batch_rows.iter().sum::<u32>();
        assert_eq!(page.topic_name, expected_page.topic_name);
        assert_eq!(page.partition_value, expected_page.partition_value);
        assert_eq!(page.num_rows, expected_num_rows);
        assert_eq!(page.batches.len(), expected_page.batch_rows.len());
        assert!(page.batch_size_bytes > 0);

        let batch_rows = page
            .batches
            .iter()
            .map(|batch| batch.num_rows)
            .collect::<Vec<_>>();
        assert_eq!(batch_rows, expected_page.batch_rows);
    }

    if pages.len() == 1 {
        assert_eq!(pages[0].offset_bytes, 0);
    } else {
        let mut offsets = pages
            .iter()
            .map(|page| page.offset_bytes)
            .collect::<Vec<_>>();
        offsets.sort_unstable();
        offsets.dedup();
        assert_eq!(offsets.len(), pages.len());
        assert_eq!(offsets[0], 0);
    }
}

fn commit_page_response(
    topic_name: TopicName,
    partition_value: Option<PartitionValue>,
    offsets: &[(u64, u64)],
) -> CommitPageResponse {
    CommitPageResponse {
        topic_name,
        partition_value,
        batches: offsets
            .iter()
            .map(|(start_offset, end_offset)| {
                CommittedBatch::Accepted(AcceptedBatchInfo {
                    start_offset: *start_offset,
                    end_offset: *end_offset,
                    timestamp: std::time::SystemTime::now(),
                })
            })
            .collect(),
    }
}
