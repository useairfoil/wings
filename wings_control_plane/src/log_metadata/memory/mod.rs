//! In-memory implementation of the log metadata trait.
//!
//! This implementation stores all data in memory and is suitable for testing
//! and development.

mod candidate;
mod page;
mod partition;
mod topic;

use std::{
    ops::Bound,
    sync::Arc,
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use dashmap::DashMap;
use tokio::time::Instant;
use tracing::{debug, trace};

use crate::{
    cluster_metadata::{ClusterMetadata, cache::TopicCache},
    log_metadata::memory::{
        partition::{GetLogLocationResult, PartitionKey},
        topic::TopicLogState,
    },
    resources::{NamespaceName, PartitionValue, TopicName},
};

use super::{
    CommitPageRequest, CommitPageResponse, CompleteTaskRequest, CompleteTaskResponse,
    GetLogLocationRequest, ListPartitionsRequest, ListPartitionsResponse, LogLocation, LogMetadata,
    LogMetadataError, PartitionMetadata, RequestTaskRequest, RequestTaskResponse, Result,
};

#[derive(Clone)]
pub struct InMemoryLogMetadata {
    /// Maps topic names to their log state
    topics: Arc<DashMap<TopicName, TopicLogState>>,
    topic_cache: TopicCache,
}

impl InMemoryLogMetadata {
    pub fn new(cluster_meta: Arc<dyn ClusterMetadata>) -> Self {
        let topic_cache = TopicCache::new(cluster_meta);
        Self {
            topics: Default::default(),
            topic_cache,
        }
    }
}

#[async_trait]
impl LogMetadata for InMemoryLogMetadata {
    async fn commit_folio(
        &self,
        namespace: NamespaceName,
        file_ref: String,
        pages: &[CommitPageRequest],
    ) -> Result<Vec<CommitPageResponse>> {
        page::validate_pages_to_commit(pages)?;

        // Assign the same timestamp to all batches across pages.
        let now_ts = SystemTime::now();

        let mut committed_pages = Vec::new();

        for page in pages {
            // This should have been checked already by the caller.
            assert_eq!(page.topic_name.parent(), &namespace);

            let partition_key =
                PartitionKey::new(page.topic_name.clone(), page.partition_value.clone());

            let mut topic_state = self
                .topics
                .entry(page.topic_name.clone())
                .or_insert_with(|| TopicLogState::default());

            let committed_page =
                topic_state.commit_page(partition_key, page, file_ref.clone(), now_ts)?;

            committed_pages.push(committed_page);
        }

        Ok(committed_pages)
    }

    async fn get_log_location(&self, request: GetLogLocationRequest) -> Result<Vec<LogLocation>> {
        trace!(?request, "InMemoryLogMetadata::get_log_location");
        let partition_key = PartitionKey::new(request.topic_name.clone(), request.partition_value);
        let mut locations = Vec::new();

        let deadline = Instant::now() + request.options.deadline;

        loop {
            let notified = {
                let Some(topic_state) = self.topics.get(&request.topic_name) else {
                    return Ok(Vec::default());
                };

                match topic_state.get_log_location(
                    &partition_key,
                    request.offset,
                    &request.options,
                    &mut locations,
                )? {
                    GetLogLocationResult::Done => {
                        return Ok(locations);
                    }
                    GetLogLocationResult::NeedMore(notified) => notified,
                }
            };

            let timeout = tokio::time::sleep_until(deadline);
            debug!("Waiting for more log locations");

            tokio::select! {
                _ = notified => {
                    debug!("Notified of state change");
                }
                _ = timeout => {
                    return Ok(locations);
                }
            }
        }
    }

    async fn list_partitions(
        &self,
        request: ListPartitionsRequest,
    ) -> Result<ListPartitionsResponse> {
        let Some(topic_state) = self.topics.get(&request.topic_name) else {
            return Ok(ListPartitionsResponse {
                partitions: vec![],
                next_page_token: None,
            });
        };

        let topic = self
            .topic_cache
            .get(request.topic_name.clone())
            .await
            .map_err(|_| LogMetadataError::InvalidArgument {
                message: "could not get topic".to_string(),
            })?;

        let page_size = request.page_size.unwrap_or(100);

        let partition_value = if let Some(data_type) = topic.partition_field_data_type() {
            if let Some(page_token) = request.page_token.as_ref() {
                let pv =
                    PartitionValue::parse_with_datatype(data_type, page_token).map_err(|err| {
                        LogMetadataError::InvalidArgument {
                            message: format!("invalid partition value: {err}"),
                        }
                    })?;
                Some(pv)
            } else {
                None
            }
        } else {
            None
        };

        let partition_key = PartitionKey::new(request.topic_name.clone(), partition_value);

        let start_key_range: Bound<&PartitionKey> =
            if topic.partition_field().is_none() || partition_key.partition_value.is_none() {
                Bound::Unbounded
            } else {
                Bound::Excluded(&partition_key)
            };

        debug!(
            start_key_range = ?start_key_range,
            "InMemoryLogMetadata::list_partitions start"
        );

        let mut partitions = Vec::new();
        let mut next_page_token = None;

        for (key, state) in topic_state
            .partition_range(start_key_range, Bound::Unbounded)
            .take(page_size)
        {
            next_page_token = key.partition_value.as_ref().map(|pv| pv.to_string());
            partitions.push(PartitionMetadata {
                partition_value: key.partition_value.clone(),
                end_offset: state.next_offset(),
            });
        }

        if partitions.len() < page_size {
            next_page_token = None;
        }

        debug!(
            next_page_token = ?next_page_token,
            "InMemoryLogMetadata::list_partitions done"
        );

        Ok(ListPartitionsResponse {
            partitions,
            next_page_token,
        })
    }

    async fn request_task(&self, _request: RequestTaskRequest) -> Result<RequestTaskResponse> {
        debug!("Received task request");

        /*
        let timeout = tokio::time::sleep(Duration::from_secs(1));
        tokio::pin!(timeout);

        loop {
            let waiter = {
                let mut task_manager = self.task_manager.lock().map_err(|_| {
                    InternalSnafu {
                        message: "failed to acquire poisoned lock".to_string(),
                    }
                    .build()
                })?;

                if let Some(task) = task_manager.next_task() {
                    info!(task_id = task.task_id(), "Assigned task to worker");
                    return Ok(RequestTaskResponse { task: task.into() });
                }

                task_manager.notify.clone().notified_owned()
            };

            debug!("No task available. Waiting.");

            tokio::select! {
                _ = waiter => {
                    continue;
                }
                _ = &mut timeout => {
                    break;
                }
            }
        }

        */
        tokio::time::sleep(Duration::from_secs(1)).await;
        Ok(RequestTaskResponse { task: None })
    }

    async fn complete_task(&self, request: CompleteTaskRequest) -> Result<CompleteTaskResponse> {
        /*
        let mut task_manager = self.task_manager.lock().map_err(|_| {
            InternalSnafu {
                message: "failed to acquire poisoned lock".to_string(),
            }
            .build()
        })?;

        let Some(task) = task_manager.complete_task(&request.task_id) else {
            return Err(LogMetadataError::TaskNotFound {
                task_id: request.task_id,
            });
        };

        info!(task_id = task.task_id(), "Task completed by worker");

        Ok(CompleteTaskResponse { success: true })
        */
        todo!();
    }
}
