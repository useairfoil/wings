use std::{
    collections::{BTreeMap, HashMap},
    ops::Bound,
    time::{Duration, SystemTime},
};

use tracing::{debug, warn};
use wings_control_plane_core::log_metadata::{
    CommitPageRequest, CommitPageResponse, CommitTask, CreateTableTask, FileInfo,
    GetLogLocationOptions, LogLocation, Result, Task, TaskCompletionResult, TaskResult,
};
use wings_resources::{CompactionConfiguration, TopicName};

use super::{
    candidate::CandidateTask,
    partition::{GetLogLocationResult, PartitionKey, PartitionLogState},
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
    Created {
        table_id: String,
        /// Map between task_ids and and which partition they belong to.
        partition_tasks: HashMap<String, PartitionKey>,
    },
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
    topic_name: TopicName,
    /// Maps partition keys to their offset tracking
    partitions: BTreeMap<PartitionKey, PartitionLogState>,
    /// Status of table creation for this topic
    table_status: TableStatus,
    /// Topic compaction configuration, fetched when topic is first created
    compaction_config: CompactionConfiguration,
    /// Files from compaction tasks that are not yet committed
    pending_files: Vec<FileInfo>,
    /// In-progress commit task to prevent duplicate commits
    in_progress_commit_task: Option<Task>,
}

impl TopicLogState {
    pub fn new(topic_name: TopicName, compaction_config: CompactionConfiguration) -> Self {
        Self {
            topic_name,
            partitions: BTreeMap::new(),
            table_status: TableStatus::default(),
            compaction_config,
            pending_files: Vec::new(),
            in_progress_commit_task: None,
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
        // Check if we should create a commit task first
        if self.in_progress_commit_task.is_none() && !self.pending_files.is_empty() {
            let topic_name = candidate.topic_name().clone();
            let commit_task = CommitTask {
                topic_name,
                new_files: std::mem::take(&mut self.pending_files),
            };
            let task = Task::new_commit(commit_task);
            self.in_progress_commit_task = Some(task.clone());
            return Some(task);
        }

        match candidate {
            CandidateTask::Topic(topic_name) => match &mut self.table_status {
                TableStatus::NotCreated { pending_candidates } => {
                    let task = Task::new_create_table(CreateTableTask { topic_name });

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
            CandidateTask::Partition(topic_name, partition_value) => match &mut self.table_status {
                TableStatus::Created {
                    partition_tasks, ..
                } => {
                    let partition_key = PartitionKey::new(topic_name, partition_value);

                    let partition_state = self.partitions.get_mut(&partition_key)?;

                    let task = partition_state.candidate_task(&self.compaction_config)?;
                    partition_tasks.insert(task.task_id().to_string(), partition_key);
                    Some(task)
                }
                TableStatus::InProgress {
                    pending_candidates, ..
                }
                | TableStatus::NotCreated { pending_candidates } => {
                    pending_candidates.push(CandidateTask::Partition(topic_name, partition_value));
                    None
                }
            },
        }
    }

    /// Complete a task for this topic.
    /// Returns success status and any pending partition candidates that should be processed.
    /// Handles both table creation and compaction tasks.
    pub fn complete_task(
        &mut self,
        task_id: &str,
        result: TaskCompletionResult,
    ) -> Result<(bool, Vec<CandidateTask>)> {
        debug!(task_id, "Received task completion result in topic state");

        // Check if this is a commit task
        if let Some(commit_task) = &self.in_progress_commit_task
            && commit_task.task_id() == task_id
        {
            match result {
                TaskCompletionResult::Success(task_result) => {
                    match task_result {
                        TaskResult::Commit(_) => {
                            debug!(task_id, "Commit task completed successfully");
                            self.in_progress_commit_task = None;
                            return Ok((true, Vec::new()));
                        }
                        _ => {
                            // Wrong task result type
                            self.in_progress_commit_task = None;
                            return Ok((false, Vec::new()));
                        }
                    }
                }
                TaskCompletionResult::Failure(error_message) => {
                    warn!(task_id, error = error_message, "Commit task failed");
                    let Some(commit) = commit_task.as_commit() else {
                        self.in_progress_commit_task = None;
                        return Ok((false, Vec::new()));
                    };

                    // Add back the files to the pending files so that they can be committed.
                    self.pending_files.extend(commit.new_files.clone());
                    self.in_progress_commit_task = None;

                    return Ok((false, Vec::new()));
                }
            }
        }

        // First check if this is a table creation task in progress
        match &mut self.table_status {
            TableStatus::InProgress {
                task,
                pending_candidates,
            } => {
                if task.task_id() == task_id {
                    match result {
                        TaskCompletionResult::Success(task_result) => {
                            match task_result {
                                TaskResult::CreateTable(create_table_result) => {
                                    let table_id = create_table_result.table_id.clone();
                                    // Take the pending candidates before changing the state
                                    let pending_candidates = std::mem::take(pending_candidates);

                                    debug!(table_id, "Table created successfully");

                                    self.table_status = TableStatus::Created {
                                        table_id,
                                        partition_tasks: Default::default(),
                                    };

                                    Ok((true, pending_candidates))
                                }
                                _ => {
                                    // Table creation task but wrong result type - this shouldn't happen
                                    Ok((false, Vec::new()))
                                }
                            }
                        }
                        TaskCompletionResult::Failure(error_message) => {
                            warn!("Table creation failed: {}", error_message);
                            // For now, we don't change the state on failure
                            // In a real implementation, we might want to retry the task or mark it as failed
                            Ok((false, Vec::new()))
                        }
                    }
                } else {
                    Ok((false, Vec::new()))
                }
            }
            TableStatus::Created {
                partition_tasks, ..
            } => {
                // TODO: if it's a topic task we can track it in the create struct
                // and handle it locally.
                // For now assume it's always to be forwarded to the partition.
                let Some(partition_key) = partition_tasks.get(task_id) else {
                    debug!(task_id, "Task is not assigned to any partition");
                    return Ok((false, Vec::new()));
                };

                let Some(partition_state) = self.partitions.get_mut(partition_key) else {
                    debug!(task_id, ?partition_key, "Task partition does not exist");
                    return Ok((false, Vec::new()));
                };

                // Remove the partition task since it's complete
                partition_tasks.remove(task_id);

                let new_files = partition_state.complete_task(task_id, result)?;

                // Accumulate new files into pending_files
                if !new_files.is_empty() {
                    debug!(
                        task_id,
                        num_files = new_files.len(),
                        "Partition task generated new files"
                    );

                    self.pending_files.extend(new_files);
                    return Ok((true, vec![CandidateTask::Topic(self.topic_name.clone())]));
                }

                Ok((true, Vec::new()))
            }
            _ => {
                // Table not created or other states, can't handle compaction tasks
                Ok((false, Vec::new()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use wings_control_plane_core::log_metadata::CreateTableResult;
    use wings_resources::{CompactionConfiguration, NamespaceName, PartitionValue, TopicName};

    use super::*;

    fn create_test_topic_name(name: &str) -> TopicName {
        let namespace =
            NamespaceName::parse("tenants/default/namespaces/default").expect("valid namespace");
        TopicName::new(name.to_string(), namespace).expect("valid topic name")
    }

    #[test]
    fn test_partition_candidate_pending_when_table_not_created() {
        let topic_name = create_test_topic_name("test-topic");
        let mut topic_state =
            TopicLogState::new(topic_name.clone(), CompactionConfiguration::default());
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
        let topic_name = create_test_topic_name("test-topic");
        let mut topic_state =
            TopicLogState::new(topic_name.clone(), CompactionConfiguration::default());

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
        let topic_name = create_test_topic_name("test-topic");
        let mut topic_state =
            TopicLogState::new(topic_name.clone(), CompactionConfiguration::default());

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
        let create_table_result = CreateTableResult {
            table_id: "test-table-id".to_string(),
        };
        let task_result = TaskResult::CreateTable(create_table_result);
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
