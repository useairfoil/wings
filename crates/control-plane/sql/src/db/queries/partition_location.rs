use sea_orm::{ColumnTrait, Condition, DbErr, EntityTrait, QueryFilter, QueryOrder};
use snafu::Snafu;
use wings_control_plane_core::table_metadata::{
    GetTableLocationRequest, TableLocation, TableMetadataError,
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
    pub async fn get_table_location(
        &self,
        request: GetTableLocationRequest,
    ) -> Result<Vec<TableLocation>, Error> {
        let GetTableLocationRequest {
            table_name,
            partition_value,
            seqnum,
            options,
        } = request;

        let target_seqnum = seqnum + options.min_rows as u64;

        self.with_transaction(|tx| {
            Box::pin(async move {
                let partition_key = PartitionKey::new(&table_name, partition_value);

                let entities = entities::partition_location::Entity::find()
                    .filter(
                        Condition::all()
                            .add(entities::partition_location::table_partition_condition(
                                partition_key,
                            ))
                            .add(entities::partition_location::Column::EndSeqnum.gte(seqnum))
                            .add(
                                entities::partition_location::Column::StartSeqnum
                                    .lte(target_seqnum),
                            ),
                    )
                    .order_by_asc(entities::partition_location::Column::EndSeqnum)
                    .all(tx)
                    .await?;

                let locations = entities
                    .into_iter()
                    .map(|entity| entity.try_into())
                    .collect::<Result<Vec<TableLocation>, _>>()?;

                Ok(locations)
            })
        })
        .await
    }
}

impl From<Error> for TableMetadataError {
    fn from(err: Error) -> Self {
        match err {
            Error::Entity { source } => source.into(),
            Error::Db { source } => TableMetadataError::Internal {
                message: format!("db error: {source}"),
            },
        }
    }
}
