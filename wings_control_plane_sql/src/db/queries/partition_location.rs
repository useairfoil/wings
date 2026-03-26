use sea_orm::{ColumnTrait, Condition, EntityTrait, QueryFilter, QueryOrder};
use wings_control_plane_core::log_metadata::{GetLogLocationOptions, LogLocation};
use wings_resources::{PartitionValue, TopicName};

use crate::{
    Database,
    db::{PartitionKey, entities, error::Result},
};

impl Database {
    pub async fn get_log_location(
        &self,
        topic_name: TopicName,
        partition_value: Option<PartitionValue>,
        offset: u64,
        options: GetLogLocationOptions,
    ) -> Result<Vec<LogLocation>> {
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
                    .collect::<Result<Vec<LogLocation>>>()?;

                Ok(locations)
            })
        })
        .await
    }
}
