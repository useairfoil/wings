use sea_orm::{ColumnTrait, Condition, DbErr, EntityTrait, QueryFilter, QueryOrder};
use snafu::Snafu;
use wings_control_plane_core::log_metadata::{
    GetLogLocationRequest, LogLocation, LogMetadataError,
};

use crate::{
    Database,
    db::{PartitionKey, entities},
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(transparent)]
    Entity { source: entities::Error },
    #[snafu(transparent)]
    Db { source: DbErr },
}

impl Database {
    pub async fn get_log_location(
        &self,
        request: GetLogLocationRequest,
    ) -> Result<Vec<LogLocation>, Error> {
        let GetLogLocationRequest {
            topic_name,
            partition_value,
            offset,
            options,
        } = request;

        let target_offset = offset + options.min_rows as u64;

        self.with_transaction(|tx| {
            Box::pin(async move {
                let partition_key = PartitionKey::new(&topic_name, partition_value);

                let entities = entities::partition_location::Entity::find()
                    .filter(
                        Condition::all()
                            .add(entities::partition_location::topic_partition_condition(
                                partition_key,
                            ))
                            .add(entities::partition_location::Column::EndOffset.gte(offset))
                            .add(
                                entities::partition_location::Column::StartOffset
                                    .lte(target_offset),
                            ),
                    )
                    .order_by_asc(entities::partition_location::Column::EndOffset)
                    .all(tx)
                    .await?;

                let locations = entities
                    .into_iter()
                    .map(|entity| entity.try_into())
                    .collect::<Result<Vec<LogLocation>, _>>()?;

                Ok(locations)
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
