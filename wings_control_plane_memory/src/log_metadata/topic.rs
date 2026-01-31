use std::{
    collections::{BTreeMap, HashMap},
    ops::Bound,
    time::{Duration, SystemTime},
};

use tracing::{debug, warn};
use wings_control_plane_core::log_metadata::{
    CommitPageRequest, CommitPageResponse, CommitTask, CreateTableTask, FileInfo,
    GetLogLocationOptions, LogLocation, Result, Task, TaskCompletionResult,
};
use wings_resources::{CompactionConfiguration, TopicCondition, TopicName};

use super::{
    candidate::CandidateTask,
    partition::{GetLogLocationResult, PartitionKey, PartitionLogState},
};

const TABLE_CREATED_CONDITION: &str = "TableCreated";
const COMPACTION_CONDITION: &str = "DataCompacted";
const COMMIT_CONDITION: &str = "DataCommitted";

/// Status of maintenance operations.
#[derive(Debug, Clone)]
pub enum OperationStatus<T> {
    NotStarted {
        timestamp: SystemTime,
    },
    InProgress {
        task: Task,
        timestamp: SystemTime,
    },
    Completed {
        result: T,
        timestamp: SystemTime,
    },
    Failed {
        error: String,
        timestamp: SystemTime,
    },
}

#[derive(Debug, Clone)]
pub struct TopicLogState {
    topic_name: TopicName,
    /// Maps partition keys to their offset tracking
    partitions: BTreeMap<PartitionKey, PartitionLogState>,
    /// Topic compaction configuration, fetched when topic is first created
    compaction_config: CompactionConfiguration,
    /// Files from compaction tasks that are not yet committed
    pending_files: Vec<FileInfo>,
    /// A list of pending task candidates to process next time state changes.
    pending_task_candidates: Vec<CandidateTask>,
    /// Maps task ids to their partition keys
    partition_tasks: HashMap<String, PartitionKey>,
    /// Table creation operation
    table_status: OperationStatus<String>,
    /// Write operation
    compaction_status: OperationStatus<String>,
    /// Commit operation
    commit_status: OperationStatus<String>,
}

impl TopicLogState {
    pub fn new(topic_name: TopicName, compaction_config: CompactionConfiguration) -> Self {
        Self {
            topic_name,
            partitions: BTreeMap::new(),
            compaction_config,
            pending_files: Vec::new(),
            pending_task_candidates: Vec::new(),
            partition_tasks: HashMap::new(),
            table_status: OperationStatus::default(),
            compaction_status: OperationStatus::default(),
            commit_status: OperationStatus::default(),
        }
    }

    pub fn num_partitions(&self) -> u64 {
        self.partitions.len() as _
    }

    pub fn conditions(&self) -> Vec<TopicCondition> {
        let table_condition = match &self.table_status {
            OperationStatus::NotStarted { timestamp } => TopicCondition {
                condition_type: TABLE_CREATED_CONDITION.to_string(),
                last_transition_time: *timestamp,
                status: false,
                reason: "NotStarted".to_string(),
                message: "Table not created".to_string(),
            },
            OperationStatus::InProgress { timestamp, .. } => TopicCondition {
                condition_type: TABLE_CREATED_CONDITION.to_string(),
                last_transition_time: *timestamp,
                status: false,
                reason: "InProgress".to_string(),
                message: "Table creation in progress".to_string(),
            },
            OperationStatus::Completed { result, timestamp } => TopicCondition {
                condition_type: TABLE_CREATED_CONDITION.to_string(),
                last_transition_time: *timestamp,
                status: true,
                reason: "Success".to_string(),
                message: format!("Table {result} created"),
            },
            OperationStatus::Failed { error, timestamp } => TopicCondition {
                condition_type: TABLE_CREATED_CONDITION.to_string(),
                last_transition_time: *timestamp,
                status: false,
                reason: "Failure".to_string(),
                message: format!("Table creation failed: {error}"),
            },
        };

        let compaction_condition = match &self.compaction_status {
            OperationStatus::NotStarted { timestamp } => TopicCondition {
                condition_type: COMPACTION_CONDITION.to_string(),
                last_transition_time: *timestamp,
                status: false,
                reason: "NotStarted".to_string(),
                message: "No data written".to_string(),
            },
            OperationStatus::InProgress { timestamp, .. } => TopicCondition {
                condition_type: COMPACTION_CONDITION.to_string(),
                last_transition_time: *timestamp,
                status: false,
                reason: "InProgress".to_string(),
                message: "Compaction in progress".to_string(),
            },
            OperationStatus::Completed { timestamp, .. } => TopicCondition {
                condition_type: COMPACTION_CONDITION.to_string(),
                last_transition_time: *timestamp,
                status: true,
                reason: "Success".to_string(),
                message: "Data written successfully".to_string(),
            },
            OperationStatus::Failed { error, timestamp } => TopicCondition {
                condition_type: COMPACTION_CONDITION.to_string(),
                last_transition_time: *timestamp,
                status: false,
                reason: "Failure".to_string(),
                message: format!("Failed to write data: {error}"),
            },
        };

        let commit_condition = match &self.commit_status {
            OperationStatus::NotStarted { timestamp } => TopicCondition {
                condition_type: COMMIT_CONDITION.to_string(),
                last_transition_time: *timestamp,
                status: false,
                reason: "NotStarted".to_string(),
                message: "No files committed".to_string(),
            },
            OperationStatus::InProgress { timestamp, .. } => TopicCondition {
                condition_type: COMMIT_CONDITION.to_string(),
                last_transition_time: *timestamp,
                status: false,
                reason: "InProgress".to_string(),
                message: "Committing files".to_string(),
            },
            OperationStatus::Completed { result, timestamp } => TopicCondition {
                condition_type: COMMIT_CONDITION.to_string(),
                last_transition_time: *timestamp,
                status: true,
                reason: "Success".to_string(),
                message: format!("Commited files. New table version {result}"),
            },
            OperationStatus::Failed { error, timestamp } => TopicCondition {
                condition_type: COMMIT_CONDITION.to_string(),
                last_transition_time: *timestamp,
                status: false,
                reason: "Failure".to_string(),
                message: format!("Failed to commit data: {error}"),
            },
        };

        vec![table_condition, compaction_condition, commit_condition]
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
        // This is essentially a reconciliation step.
        //
        //  - Create a table if needed
        //  - Commit files if needed
        //  - If nothing else, write new data.
        match &self.table_status {
            OperationStatus::NotStarted { .. } => {
                let task = Task::new_create_table(CreateTableTask {
                    topic_name: candidate.topic_name().clone(),
                });

                self.table_status = OperationStatus::new_in_progress(&task);

                return Some(task);
            }
            OperationStatus::Completed { .. } => {}
            OperationStatus::InProgress { .. } | OperationStatus::Failed { .. } => {
                // No point in storing pending candidates if the table creation failed
                if self.table_status.is_in_progress() {
                    self.pending_task_candidates.push(candidate);
                }

                return None;
            }
        }

        match candidate {
            CandidateTask::Topic(topic_name) => match &self.commit_status {
                OperationStatus::Completed { .. } | OperationStatus::NotStarted { .. } => {
                    if self.pending_files.is_empty() {
                        return None;
                    }

                    let new_files = std::mem::take(&mut self.pending_files);

                    let task = Task::new_commit(CommitTask {
                        topic_name,
                        new_files,
                    });

                    self.commit_status = OperationStatus::new_in_progress(&task);

                    Some(task)
                }
                OperationStatus::Failed { .. } | OperationStatus::InProgress { .. } => {
                    if self.commit_status.is_in_progress() {
                        self.pending_task_candidates
                            .push(CandidateTask::Topic(topic_name));
                    }

                    None
                }
            },
            CandidateTask::Partition(topic_name, partition_value) => {
                match &self.compaction_status {
                    OperationStatus::Completed { .. } | OperationStatus::NotStarted { .. } => {
                        let partition_key = PartitionKey::new(topic_name, partition_value);

                        let partition_state = self.partitions.get_mut(&partition_key)?;
                        let task = partition_state.candidate_task(&self.compaction_config)?;

                        self.partition_tasks
                            .insert(task.task_id().to_string(), partition_key);

                        self.compaction_status = OperationStatus::new_in_progress(&task);

                        Some(task)
                    }
                    OperationStatus::Failed { .. } | OperationStatus::InProgress { .. } => {
                        if self.compaction_status.is_in_progress() {
                            self.pending_task_candidates
                                .push(CandidateTask::Partition(topic_name, partition_value));
                        }

                        None
                    }
                }
            }
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

        match &self.table_status {
            OperationStatus::InProgress { task, .. } if task.task_id() == task_id => match result {
                TaskCompletionResult::Success(task_result) => {
                    let Some(create_table) = task_result.take_create_table() else {
                        return Ok((false, Vec::new()));
                    };

                    let table_id = create_table.table_id.clone();
                    debug!(topic = %self.topic_name, table_id, "Table created successfully");

                    self.table_status = OperationStatus::new_completed(table_id);

                    let pending_candidates = std::mem::take(&mut self.pending_task_candidates);
                    return Ok((true, pending_candidates));
                }
                TaskCompletionResult::Failure(error) => {
                    debug!(topic = %self.topic_name, error, "Table creation failed");
                    self.table_status = OperationStatus::new_failed(error);

                    return Ok((false, Vec::new()));
                }
            },
            _ => {}
        }

        match &self.commit_status {
            OperationStatus::InProgress { task, .. } if task.task_id() == task_id => match result {
                TaskCompletionResult::Success(task_result) => {
                    let Some(commit) = task_result.take_commit() else {
                        return Ok((false, Vec::new()));
                    };

                    self.commit_status = OperationStatus::new_completed(commit.table_version);

                    return Ok((true, Vec::new()));
                }
                TaskCompletionResult::Failure(error) => {
                    debug!(topic = %self.topic_name, error, "Table commit failed");
                    self.commit_status = OperationStatus::new_failed(error);

                    return Ok((false, Vec::new()));
                }
            },
            _ => {}
        }

        match &self.compaction_status {
            OperationStatus::InProgress { task, .. } if task.task_id() == task_id => match result {
                TaskCompletionResult::Success(task_result) => {
                    let Some(compaction) = task_result.take_compaction() else {
                        return Ok((false, Vec::new()));
                    };

                    let Some(partition_key) = self.partition_tasks.get(task_id) else {
                        debug!(topic = %self.topic_name, task_id, "Compaction task is not assigned to any partition");
                        return Ok((false, Vec::new()));
                    };

                    let Some(partition_state) = self.partitions.get_mut(partition_key) else {
                        debug!(topic = %self.topic_name, task_id, ?partition_key, "Compaction task partition does not exist");
                        return Ok((false, Vec::new()));
                    };

                    self.partition_tasks.remove(task_id);

                    self.compaction_status = OperationStatus::new_completed("".to_string());

                    let new_files = partition_state.complete_task(task_id, compaction)?;

                    // Accumulate new files into pending_files
                    if !new_files.is_empty() {
                        debug!(
                            topic = %self.topic_name,
                            task_id,
                            num_files = new_files.len(),
                            "Topic compaction task generated new files"
                        );

                        self.pending_files.extend(new_files);
                        return Ok((true, vec![CandidateTask::Topic(self.topic_name.clone())]));
                    }

                    return Ok((true, Vec::new()));
                }
                TaskCompletionResult::Failure(error) => {
                    debug!(topic = %self.topic_name, error, "Table write failed");

                    self.partition_tasks.remove(task_id);
                    self.compaction_status = OperationStatus::new_failed(error);

                    return Ok((false, Vec::new()));
                }
            },
            _ => {}
        }

        warn!(topic = %self.topic_name, "Received unknown task completion result");

        Ok((true, Vec::new()))
    }
}

impl<T> OperationStatus<T> {
    pub fn new_in_progress(task: &Task) -> Self {
        Self::InProgress {
            timestamp: SystemTime::now(),
            task: task.clone(),
        }
    }

    pub fn new_completed(result: T) -> Self {
        Self::Completed {
            timestamp: SystemTime::now(),
            result,
        }
    }

    pub fn new_failed(error: String) -> Self {
        Self::Failed {
            timestamp: SystemTime::now(),
            error,
        }
    }

    pub fn is_in_progress(&self) -> bool {
        matches!(self, OperationStatus::InProgress { .. })
    }
}

impl<T> Default for OperationStatus<T> {
    fn default() -> Self {
        Self::NotStarted {
            timestamp: SystemTime::now(),
        }
    }
}

#[cfg(test)]
mod tests {
    use wings_control_plane_core::log_metadata::{CreateTableResult, TaskResult};
    use wings_resources::{CompactionConfiguration, NamespaceName, PartitionValue, TopicName};

    use super::*;

    fn create_test_topic_name(name: &str) -> TopicName {
        let namespace =
            NamespaceName::parse("tenants/default/namespaces/default").expect("valid namespace");
        TopicName::new(name.to_string(), namespace).expect("valid topic name")
    }

    #[test]
    fn test_pending_candidates_returned_on_table_creation_complete() {
        let topic_name = create_test_topic_name("test-topic");
        let mut topic_state =
            TopicLogState::new(topic_name.clone(), CompactionConfiguration::default());

        // Add some pending partition candidates
        let partition_value1 = Some(PartitionValue::String("partition-1".to_string()));

        // Start table creation
        let candidate1 = CandidateTask::Topic(topic_name.clone());
        let task = topic_state
            .candidate_task(candidate1)
            .expect("should create table task");
        let task_id = task.task_id().to_string();

        let candidate2 = CandidateTask::Topic(topic_name.clone());
        topic_state.candidate_task(candidate2);
        let candidate3 = CandidateTask::Partition(topic_name.clone(), partition_value1.clone());
        topic_state.candidate_task(candidate3);

        // Table creation in progress
        assert!(matches!(
            topic_state.table_status,
            OperationStatus::InProgress { .. }
        ));

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
            CandidateTask::Topic(topic_name.clone())
        );
        assert_eq!(
            pending_candidates[1],
            CandidateTask::Partition(topic_name, partition_value1)
        );

        // Pending list should be empty after completion
        assert!(matches!(
            topic_state.table_status,
            OperationStatus::Completed { .. }
        ));
        assert!(topic_state.pending_task_candidates.is_empty());
    }
}
