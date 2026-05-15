use sea_orm::{
    ActiveValue::Set, ColumnTrait, DbErr, EntityTrait, PaginatorTrait, QueryFilter, QueryOrder,
};
use snafu::Snafu;
use time::OffsetDateTime;
use tracing::debug;
use wings_control_plane_core::{
    ClusterMetadataError,
    cluster_metadata::{ListTablesRequest, ListTablesResponse, TableView},
    table_metadata::CreateTableTask,
};
use wings_resources::{Table, TableName, TableOptions, validate_compaction};

use crate::{Database, db::entities};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("table {name} already exists"))]
    AlreadyExists { name: TableName },
    #[snafu(display("table {name} not found"))]
    NotFound { name: TableName },
    #[snafu(display("no field with id {key} found in schema"))]
    InvalidPartitionKey { key: u64 },
    #[snafu(display("invalid compaction configuration: {message}"))]
    InvalidCompactionConfiguration { message: String },
    #[snafu(transparent)]
    Json { source: serde_json::Error },
    #[snafu(transparent)]
    Entity { source: entities::Error },
    #[snafu(transparent)]
    Db { source: DbErr },
}

impl Database {
    pub async fn create_table(
        &self,
        name: TableName,
        options: TableOptions,
    ) -> Result<Table, Error> {
        let namespace_name = name.parent().clone();
        let tenant_id = namespace_name.parent().id().to_owned();
        let namespace_id = namespace_name.id().to_owned();
        let id = name.id().to_owned();

        // Validate partition key exists in schema
        if let Some(partition_key) = options.partition_key
            && !options
                .schema
                .fields_iter()
                .any(|field| field.id == partition_key)
        {
            return Err(Error::InvalidPartitionKey { key: partition_key });
        }

        // Validate compaction configuration
        if let Err(errors) = validate_compaction(&options.compaction) {
            let message = errors.join(", ");
            return Err(Error::InvalidCompactionConfiguration { message });
        }

        let schema_fields = serde_json::to_value(&options.schema.fields)?;
        let schema_metadata = serde_json::to_value(&options.schema.metadata)?;

        let table = entities::table::ActiveModel {
            tenant_id: Set(tenant_id.clone()),
            namespace_id: Set(namespace_id.clone()),
            id: Set(id.clone()),
            schema_fields: Set(schema_fields),
            schema_metadata: Set(schema_metadata),
            partition_key: Set(options.partition_key.map(|id| id as u32)),
            description: Set(options.description.clone()),
            compaction_freshness_ms: Set(options.compaction.freshness.as_millis() as u32),
            compaction_ttl_ms: Set(options.compaction.ttl.map(|ttl| ttl.as_millis() as u32)),
            compaction_target_file_size_bytes: Set(
                options.compaction.target_file_size.as_u64() as u32
            ),
        };

        self.with_transaction(|tx| {
            Box::pin(async move {
                entities::namespace::expect_exists(tx, &namespace_name).await?;

                let existing = entities::table::Entity::find_by_id((
                    tenant_id.clone(),
                    namespace_id.clone(),
                    id.clone(),
                ))
                .one(tx)
                .await?;

                if existing.is_some() {
                    return Err(Error::AlreadyExists { name });
                }

                let entity = entities::table::Entity::insert(table)
                    .exec_with_returning(tx)
                    .await?;

                let task_id = entities::task::insert_task(
                    tx,
                    CreateTableTask {
                        table_name: name.clone(),
                    },
                    OffsetDateTime::now_utc(),
                )
                .await?;

                debug!(task_id, table = %name, "Inserted table create task");

                entity.try_into().map_err(Into::into)
            })
        })
        .await
    }

    pub async fn get_table(&self, name: TableName, _view: TableView) -> Result<Table, Error> {
        let tenant_id = name.parent().parent().id().to_owned();
        let namespace_id = name.parent().id().to_owned();
        let id = name.id().to_owned();

        let existing = entities::table::Entity::find_by_id((tenant_id, namespace_id, id))
            .one(&self.pool)
            .await?;

        match existing {
            Some(entity) => entity.try_into().map_err(Into::into),
            None => Err(Error::NotFound { name }),
        }
    }

    pub async fn list_tables(
        &self,
        request: ListTablesRequest,
    ) -> Result<ListTablesResponse, Error> {
        let tenant_id = request.parent.parent().id().to_owned();
        let namespace_id = request.parent.id().to_owned();
        let page_size = request.page_size.unwrap_or(100).clamp(1, 1_000) as u64;

        let mut query = entities::table::Entity::find()
            .filter(entities::table::Column::TenantId.eq(&tenant_id))
            .filter(entities::table::Column::NamespaceId.eq(&namespace_id))
            .order_by_asc(entities::table::Column::Id);

        if let Some(id) = request.page_token {
            query = query.filter(entities::table::Column::Id.gt(id));
        }

        let entities = query.paginate(&self.pool, page_size).fetch().await?;

        let has_more = entities.len() == page_size as usize;

        let next_page_token = if has_more {
            let last_entity_index = page_size as usize - 1;
            Some(entities[last_entity_index].id.clone())
        } else {
            None
        };

        let tables = entities
            .into_iter()
            .map(|entity| entity.try_into())
            .collect::<Result<Vec<_>, _>>()?;

        Ok(ListTablesResponse {
            tables,
            next_page_token,
        })
    }

    pub async fn delete_table(&self, name: TableName, _force: bool) -> Result<(), Error> {
        let tenant_id = name.parent().parent().id().to_owned();
        let namespace_id = name.parent().id().to_owned();
        let id = name.id().to_owned();

        self.with_transaction(|tx| {
            Box::pin(async move {
                entities::table::expect_exists(tx, &name).await?;

                let result = entities::table::Entity::delete_by_id((
                    tenant_id.clone(),
                    namespace_id.clone(),
                    id.clone(),
                ))
                .exec(tx)
                .await?;

                if result.rows_affected == 0 {
                    return Err(Error::NotFound { name });
                }

                Ok(())
            })
        })
        .await
    }
}

impl From<Error> for ClusterMetadataError {
    fn from(err: Error) -> Self {
        match err {
            Error::AlreadyExists { name } => ClusterMetadataError::AlreadyExists {
                resource: "table".to_string(),
                name: name.to_string(),
            },
            Error::NotFound { name } => ClusterMetadataError::NotFound {
                resource: "table".to_string(),
                name: name.to_string(),
            },
            Error::InvalidPartitionKey { .. } => ClusterMetadataError::InvalidArgument {
                resource: "table".to_string(),
                message: err.to_string(),
            },
            Error::InvalidCompactionConfiguration { .. } => ClusterMetadataError::InvalidArgument {
                resource: "table".to_string(),
                message: err.to_string(),
            },
            Error::Json { source } => ClusterMetadataError::Internal {
                message: format!("json error: {source}"),
            },
            Error::Entity { source } => source.into(),
            Error::Db { source } => ClusterMetadataError::Internal {
                message: format!("db error: {source}"),
            },
        }
    }
}
