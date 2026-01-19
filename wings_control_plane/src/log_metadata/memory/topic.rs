use std::{
    collections::BTreeMap,
    ops::Bound,
    time::{Duration, SystemTime},
};

use crate::{
    log_metadata::{
        CommitPageRequest, CommitPageResponse, CreateTableTask, GetLogLocationOptions, LogLocation,
        Task, TaskCompletionResult, TaskMetadata, TaskStatus, error::Result,
        memory::partition::GetLogLocationResult,
    },
    resources::CompactionConfiguration,
};

use super::{
    candidate::CandidateTask,
    partition::{PartitionKey, PartitionLogState},
};

/// Status of table creation for a topic.
#[derive(Debug, Clone)]
pub enum TableStatus {
    /// Table has not been created yet.
    NotCreated {
        /// Pending partition candidates waiting for table creation
        pending_candidates: Vec<CandidateTask>,
    },
    /// Table creation is in progress.
    InProgress {
        task: Task,
        /// Pending partition candidates waiting for table creation
        pending_candidates: Vec<CandidateTask>,
    },
    /// Table has been created.
    Created { table_id: String },
}

impl Default for TableStatus {
    fn default() -> Self {
        TableStatus::NotCreated {
            pending_candidates: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TopicLogState {
    /// Maps partition keys to their offset tracking
    partitions: BTreeMap<PartitionKey, PartitionLogState>,
    /// Status of table creation for this topic
    table_status: TableStatus,
    /// Topic compaction configuration, fetched when topic is first created
    compaction_config: CompactionConfiguration,
}

impl TopicLogState {
    pub fn new(compaction_config: CompactionConfiguration) -> Self {
        Self {
            partitions: BTreeMap::new(),
            table_status: TableStatus::default(),
            compaction_config,
        }
    }

    pub fn partition_range(
        &self,
        start: Bound<&PartitionKey>,
        end: Bound<&PartitionKey>,
    ) -> impl Iterator<Item = (&PartitionKey, &PartitionLogState)> {
        self.partitions.range((start, end))
    }

    pub fn commit_page(
        &mut self,
        partition_key: PartitionKey,
        page: &CommitPageRequest,
        file_ref: String,
        now_ts: SystemTime,
    ) -> Result<(CommitPageResponse, Option<(CandidateTask, Duration)>)> {
        let partition_state = self
            .partitions
            .entry(partition_key.clone())
            .or_insert_with(|| PartitionLogState::new(partition_key.clone()));

        let (response, candidate_task) =
            partition_state.commit_page(page, file_ref, now_ts, Some(&self.compaction_config))?;

        Ok((response, candidate_task))
    }

    pub fn get_log_location(
        &self,
        partition_key: &PartitionKey,
        offset: u64,
        options: &GetLogLocationOptions,
        locations: &mut Vec<LogLocation>,
    ) -> Result<GetLogLocationResult> {
        let Some(partition_state) = self.partitions.get(partition_key) else {
            return Ok(GetLogLocationResult::Done);
        };

        partition_state.get_log_location(offset, options, locations)
    }

    /// Create a table creation task for this topic if needed.
    /// Returns the task and any pending partition candidates that should be processed.
    pub fn candidate_task(&mut self, candidate: CandidateTask) -> Option<Task> {
        match candidate {
            CandidateTask::Topic(topic_name) => match &mut self.table_status {
                TableStatus::NotCreated { pending_candidates } => {
                    let task_id = ulid::Ulid::new().to_string();

                    let task_metadata = TaskMetadata {
                        task_id,
                        status: TaskStatus::Pending,
                        created_at: SystemTime::now(),
                        updated_at: SystemTime::now(),
                    };

                    let task = Task::CreateTable {
                        metadata: task_metadata,
                        task: CreateTableTask { topic_name },
                    };

                    // Move pending candidates to the new InProgress state
                    let existing_pending = std::mem::take(pending_candidates);
                    self.table_status = TableStatus::InProgress {
                        task: task.clone(),
                        pending_candidates: existing_pending,
                    };

                    Some(task)
                }
                _ => None,
            },
            CandidateTask::Partition(topic_name, partition_value) => {
                match &mut self.table_status {
                    TableStatus::Created { .. } => {
                        // Table is created, try to get candidate task from partition state
                        let _partition_key = PartitionKey::new(topic_name, partition_value);
                        // TODO: we will request candidates from the partition log states
                        None
                    }
                    TableStatus::InProgress {
                        pending_candidates, ..
                    }
                    | TableStatus::NotCreated { pending_candidates } => {
                        pending_candidates
                            .push(CandidateTask::Partition(topic_name, partition_value));
                        None
                    }
                }
            }
        }
    }

    /// Complete a table creation task for this topic.
    /// Returns success status and any pending partition candidates that should be processed.
    pub fn complete_task(
        &mut self,
        task_id: &str,
        result: TaskCompletionResult,
    ) -> Result<(bool, Vec<CandidateTask>)> {
        match &mut self.table_status {
            TableStatus::InProgress {
                task,
                pending_candidates,
            } => {
                if task.task_id() == task_id {
                    match result {
                        TaskCompletionResult::Success(task_result) => {
                            match task_result {
                                crate::log_metadata::TaskResult::CreateTable(
                                    create_table_result,
                                ) => {
                                    let table_id = create_table_result.table_id.clone();
                                    // Take the pending candidates before changing the state
                                    let pending_candidates = std::mem::take(pending_candidates);
                                    self.table_status = TableStatus::Created { table_id };
                                    Ok((true, pending_candidates))
                                }
                                _ => {
                                    // For now, we only handle table creation tasks
                                    Ok((false, Vec::new()))
                                }
                            }
                        }
                        TaskCompletionResult::Failure(_error_message) => {
                            // For now, we don't change the state on failure
                            // In a real implementation, we might want to retry the task or mark it as failed
                            Ok((false, Vec::new()))
                        }
                    }
                } else {
                    Ok((false, Vec::new()))
                }
            }
            _ => Ok((false, Vec::new())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::{CompactionConfiguration, NamespaceName, PartitionValue, TopicName};

    fn create_test_topic_name(name: &str) -> TopicName {
        let namespace =
            NamespaceName::parse("tenants/default/namespaces/default").expect("valid namespace");
        TopicName::new(name.to_string(), namespace).expect("valid topic name")
    }

    #[test]
    fn test_partition_candidate_pending_when_table_not_created() {
        let mut topic_state = TopicLogState::new(CompactionConfiguration::default());
        let topic_name = create_test_topic_name("test-topic");
        let partition_value = Some(PartitionValue::String("partition-1".to_string()));

        let candidate = CandidateTask::Partition(topic_name.clone(), partition_value.clone());
        let task = topic_state.candidate_task(candidate);

        // Should not create a task since table is not created
        assert!(task.is_none());

        // Should have added the candidate to pending list
        match &topic_state.table_status {
            TableStatus::NotCreated { pending_candidates } => {
                assert_eq!(pending_candidates.len(), 1);
                assert_eq!(
                    pending_candidates[0],
                    CandidateTask::Partition(topic_name, partition_value)
                );
            }
            _ => panic!("Expected NotCreated status"),
        }
    }

    #[test]
    fn test_partition_candidate_pending_when_table_in_progress() {
        let mut topic_state = TopicLogState::new(CompactionConfiguration::default());
        let topic_name = create_test_topic_name("test-topic");

        // First create a table creation task to set status to InProgress
        let topic_candidate = CandidateTask::Topic(topic_name.clone());
        let task = topic_state.candidate_task(topic_candidate);
        assert!(task.is_some());

        // Now try a partition candidate
        let partition_value = Some(PartitionValue::String("partition-1".to_string()));
        let partition_candidate =
            CandidateTask::Partition(topic_name.clone(), partition_value.clone());
        let task = topic_state.candidate_task(partition_candidate);

        // Should not create a task since table creation is in progress
        assert!(task.is_none());

        // Should have added the candidate to pending list
        match &topic_state.table_status {
            TableStatus::InProgress {
                pending_candidates, ..
            } => {
                assert_eq!(pending_candidates.len(), 1);
                assert_eq!(
                    pending_candidates[0],
                    CandidateTask::Partition(topic_name, partition_value)
                );
            }
            _ => panic!("Expected InProgress status"),
        }
    }

    #[test]
    fn test_pending_candidates_returned_on_table_creation_complete() {
        let mut topic_state = TopicLogState::new(CompactionConfiguration::default());
        let topic_name = create_test_topic_name("test-topic");

        // Add some pending partition candidates
        let partition_value1 = Some(PartitionValue::String("partition-1".to_string()));
        let partition_value2 = Some(PartitionValue::String("partition-2".to_string()));

        // Add candidates to the pending list in NotCreated state by creating partition candidates
        let candidate1 = CandidateTask::Partition(topic_name.clone(), partition_value1.clone());
        let candidate2 = CandidateTask::Partition(topic_name.clone(), partition_value2.clone());
        topic_state.candidate_task(candidate1);
        topic_state.candidate_task(candidate2);

        // Create a table creation task first
        let topic_candidate = CandidateTask::Topic(topic_name.clone());
        let task = topic_state.candidate_task(topic_candidate);
        let task = task.expect("should create table task");
        let task_id = task.task_id().to_string();

        // Complete the table creation task
        let create_table_result = crate::log_metadata::CreateTableResult {
            table_id: "test-table-id".to_string(),
        };
        let task_result = crate::log_metadata::TaskResult::CreateTable(create_table_result);
        let completion_result = TaskCompletionResult::Success(task_result);

        let (success, pending_candidates) = topic_state
            .complete_task(&task_id, completion_result)
            .expect("complete_task should succeed");

        assert!(success);
        assert_eq!(pending_candidates.len(), 2);
        assert_eq!(
            pending_candidates[0],
            CandidateTask::Partition(topic_name.clone(), partition_value1)
        );
        assert_eq!(
            pending_candidates[1],
            CandidateTask::Partition(topic_name, partition_value2)
        );

        // Pending list should be empty after completion
        match &topic_state.table_status {
            TableStatus::Created { .. } => {
                // Created status has no pending candidates, which is correct
            }
            _ => panic!("Expected Created status"),
        }
    }
}
