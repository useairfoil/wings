use sea_orm::{
    ActiveValue::Set,
    ColumnTrait, DbErr, EntityTrait, QueryFilter, QueryOrder, QuerySelect, QueryTrait,
    sea_query::{LockBehavior, LockType},
};
use snafu::Snafu;
use time::OffsetDateTime;
use wings_control_plane_core::log_metadata::{
    LogMetadataError, RequestTaskRequest, RequestTaskResponse,
};

use crate::{Database, db::entities};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(transparent)]
    Entity { source: entities::Error },
    #[snafu(transparent)]
    Db { source: DbErr },
}

impl Database {
    /// Requests a task to be processed.
    ///
    /// This method finds the oldest queued task that is ready to run (run_at <= now),
    /// updates its status to Processing, and returns it.
    pub async fn request_task(
        &self,
        _request: &RequestTaskRequest,
    ) -> Result<RequestTaskResponse, Error> {
        let now = OffsetDateTime::now_utc();

        self.with_transaction(|tx| {
            Box::pin(async move {
                let next_task_id = entities::task::Entity::find()
                    .select_only()
                    .column(entities::task::Column::Id)
                    .filter(entities::task::Column::Status.eq(entities::task::Status::Queued))
                    .filter(entities::task::Column::RunAt.lte(now))
                    .order_by_asc(entities::task::Column::RunAt)
                    .limit(1)
                    .lock_with_behavior(LockType::Update, LockBehavior::SkipLocked)
                    .into_query();

                let new_value = entities::task::ActiveModel {
                    status: Set(entities::task::Status::Processing),
                    updated_at: Set(Some(now)),
                    ..Default::default()
                };

                let Some(task) = entities::task::Entity::update_many()
                    .set(new_value)
                    .filter(entities::task::Column::Id.in_subquery(next_task_id))
                    .exec_with_returning(tx)
                    .await?
                    .into_iter()
                    .next()
                else {
                    return Ok(RequestTaskResponse { task: None });
                };

                let task = task.try_into()?;

                Ok(RequestTaskResponse { task: Some(task) })
            })
        })
        .await
    }
}

impl From<Error> for LogMetadataError {
    fn from(err: Error) -> Self {
        match err {
            Error::Entity { source } => source.into(),
            Error::Db { source } => LogMetadataError::Internal {
                message: format!("db error: {source}"),
            },
        }
    }
}
