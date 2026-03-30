use sea_orm::entity::prelude::*;
use time::OffsetDateTime;
use wings_control_plane_core::{
    log_metadata::{CommitTask, CompactionTask, CreateTableTask, Task, TaskMetadata, TaskStatus},
    pb,
};

use super::error::Error;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "tasks")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: String,
    pub created_at: OffsetDateTime,
    pub status: Status,
    pub run_at: OffsetDateTime,
    pub task_type_url: String,
    pub task_payload_pb: Vec<u8>,
    pub updated_at: Option<OffsetDateTime>,
    pub attempts: u32,
    pub max_attempts: u32,
    pub error_message: Option<String>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, EnumIter, DeriveActiveEnum)]
#[sea_orm(rs_type = "String", db_type = "String(StringLen::N(1))")]
pub enum Status {
    #[sea_orm(string_value = "Q")]
    Queued,
    #[sea_orm(string_value = "P")]
    Processing,
    #[sea_orm(string_value = "C")]
    Completed,
    #[sea_orm(string_value = "F")]
    Failed,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}

impl Model {
    pub fn task_metadata(&self) -> TaskMetadata {
        TaskMetadata {
            task_id: self.id.clone(),
            status: self.status.into(),
            created_at: self.created_at.into(),
            updated_at: self.updated_at.unwrap_or(self.created_at).into(),
        }
    }
}

impl TryFrom<Model> for Task {
    type Error = Error;

    fn try_from(model: Model) -> Result<Self, Self::Error> {
        use prost::Message;
        let metadata = model.task_metadata();

        match model.task_type_url.as_ref() {
            CompactionTask::TYPE_URL => {
                let task =
                    pb::CompactionTask::decode(model.task_payload_pb.as_slice())?.try_into()?;
                Ok(Task::Compaction { metadata, task })
            }
            CreateTableTask::TYPE_URL => {
                let task =
                    pb::CreateTableTask::decode(model.task_payload_pb.as_slice())?.try_into()?;
                Ok(Task::CreateTable { metadata, task })
            }
            CommitTask::TYPE_URL => {
                let task = pb::CommitTask::decode(model.task_payload_pb.as_slice())?.try_into()?;
                Ok(Task::Commit { metadata, task })
            }
            type_url => Err(Error::Internal {
                message: format!("unknown task type: {type_url}"),
            }),
        }
    }
}

impl From<Status> for TaskStatus {
    fn from(status: Status) -> Self {
        match status {
            Status::Queued => TaskStatus::Pending,
            Status::Processing => TaskStatus::InProgress,
            Status::Completed => TaskStatus::Completed,
            Status::Failed => TaskStatus::Failed,
        }
    }
}
