use std::{collections::BTreeMap, ops::Bound, time::SystemTime};

use crate::log_metadata::{
    CommitPageRequest, CommitPageResponse, CreateTableTask, GetLogLocationOptions, LogLocation,
    Task, TaskCompletionResult, TaskMetadata, TaskStatus, error::Result,
    memory::partition::GetLogLocationResult,
};

use super::{
    candidate::CandidateTask,
    partition::{PartitionKey, PartitionLogState},
};

/// Status of table creation for a topic.
#[derive(Default, Debug, Clone)]
pub enum TableStatus {
    /// Table has not been created yet.
    #[default]
    NotCreated,
    /// Table creation is in progress.
    InProgress { task: Task },
    /// Table has been created.
    Created { table_id: String },
}

#[derive(Default, Debug, Clone)]
pub struct TopicLogState {
    /// Maps partition keys to their offset tracking
    partitions: BTreeMap<PartitionKey, PartitionLogState>,
    /// Status of table creation for this topic
    table_status: TableStatus,
}

impl TopicLogState {
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
    ) -> Result<CommitPageResponse> {
        let partition_state = self
            .partitions
            .entry(partition_key.clone())
            .or_insert_with(|| PartitionLogState::new(partition_key));

        partition_state.commit_page(page, file_ref, now_ts)
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

    /// Check if this topic needs a table creation task.
    pub fn needs_table_creation(&self) -> bool {
        matches!(self.table_status, TableStatus::NotCreated)
    }

    /// Create a table creation task for this topic if needed.
    pub fn candidate_task(&mut self, candidate: CandidateTask) -> Option<Task> {
        match candidate {
            CandidateTask::Topic(topic_name) => match &self.table_status {
                TableStatus::NotCreated => {
                    let task_id = format!(
                        "create_table_{}_{}",
                        topic_name.name(),
                        SystemTime::now().elapsed().unwrap_or_default().as_millis()
                    );

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

                    self.table_status = TableStatus::InProgress { task: task.clone() };

                    Some(task)
                }
                TableStatus::InProgress { .. } | TableStatus::Created { .. } => None,
            },
            CandidateTask::Partition(_, _) => {
                // For now, we ignore partition candidates
                None
            }
        }
    }

    /// Complete a table creation task for this topic.
    pub fn complete_task(&mut self, task_id: &str, result: TaskCompletionResult) -> Result<bool> {
        match &mut self.table_status {
            TableStatus::InProgress { task } => {
                if task.task_id() == task_id {
                    match result {
                        TaskCompletionResult::Success(task_result) => {
                            match task_result {
                                crate::log_metadata::TaskResult::CreateTable(
                                    create_table_result,
                                ) => {
                                    let table_id = create_table_result.table_id.clone();
                                    self.table_status = TableStatus::Created { table_id };
                                    Ok(true)
                                }
                                _ => {
                                    // For now, we only handle table creation tasks
                                    Ok(false)
                                }
                            }
                        }
                        TaskCompletionResult::Failure(_error_message) => {
                            // For now, we don't change the state on failure
                            // In a real implementation, we might want to retry the task or mark it as failed
                            Ok(false)
                        }
                    }
                } else {
                    Ok(false)
                }
            }
            _ => Ok(false),
        }
    }
}
