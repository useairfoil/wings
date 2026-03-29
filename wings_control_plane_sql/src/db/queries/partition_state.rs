use std::time::SystemTime;

use sea_orm::{
    ActiveValue::{NotSet, Set},
    DatabaseTransaction, DbErr, EntityTrait, PaginatorTrait, QueryFilter, QuerySelect,
    sea_query::OnConflict,
};
use snafu::Snafu;
use tracing::debug;
use wings_control_plane_core::log_metadata::{
    AcceptedBatchInfo, CommitPageRequest, CommitPageResponse, CommittedBatch,
    ListPartitionsRequest, ListPartitionsResponse, LogMetadataError, RejectedBatchInfo,
    timestamp::{ValidateRequestResult, validate_timestamp_in_request},
    validate_pages_to_commit,
};
use wings_resources::NamespaceName;

use crate::{
    Database,
    db::{PartitionKey, entities},
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("page token '{token}' is invalid"))]
    InvalidPageToken { token: String },
    #[snafu(transparent)]
    Entity { source: entities::Error },
    #[snafu(transparent)]
    Db { source: DbErr },
}

impl Database {
    pub async fn commit_folio(
        &self,
        namespace: NamespaceName,
        file_ref: String,
        pages: &[CommitPageRequest],
    ) -> Result<Vec<CommitPageResponse>, Error> {
        // TODO: decide how to forward validation errors.
        validate_pages_to_commit(&namespace, pages).unwrap();

        // Assign the same timestamp to all batches across pages.
        let now_ts = SystemTime::now();
        let pages = pages.to_vec();

        self.with_transaction(|tx| {
            Box::pin(async move {
                let mut committed_pages = Vec::new();

                for page in pages {
                    let response = commit_page(tx, file_ref.clone(), page, now_ts).await?;
                    committed_pages.push(response);
                }

                Ok(committed_pages)
            })
        })
        .await
    }

    pub async fn list_partitions(
        &self,
        request: ListPartitionsRequest,
    ) -> Result<ListPartitionsResponse, Error> {
        let ListPartitionsRequest {
            topic_name,
            page_size,
            page_token,
        } = request;

        let page_size = page_size.unwrap_or(100);

        // TODO: we should use a more robust pagination strategy
        let page = page_token
            .map(|t| {
                t.parse::<usize>()
                    .map_err(|_| Error::InvalidPageToken { token: t })
            })
            .transpose()?
            .unwrap_or(0);

        self.with_transaction(|tx| {
            Box::pin(async move {
                let paginator = entities::partition_state::Entity::find()
                    .filter(entities::partition_state::topic_condition(&topic_name))
                    .paginate(tx, page_size as _);

                let partitions = paginator
                    .fetch_page(page as _)
                    .await?
                    .into_iter()
                    .map(|m| m.try_into())
                    .collect::<Result<Vec<_>, _>>()?;

                let next_page_token = if partitions.len() == page_size {
                    Some((page + 1).to_string())
                } else {
                    None
                };

                Ok(ListPartitionsResponse {
                    partitions,
                    next_page_token,
                })
            })
        })
        .await
    }
}

async fn commit_page(
    tx: &DatabaseTransaction,
    file_ref: String,
    page: CommitPageRequest,
    now_ts: SystemTime,
) -> Result<CommitPageResponse, Error> {
    let partition_key = PartitionKey::new(&page.topic_name, page.partition_value.clone());

    let state = entities::partition_state::Entity::find_by_id(partition_key.clone())
        .lock_exclusive()
        .one(tx)
        .await?;

    let start_offset = state
        .map(|state| state.next_log_offset())
        .unwrap_or_default();

    let mut batches = Vec::new();

    let mut current_offset = start_offset;

    // TODO: check that the timestamp is assigned correctly
    // we want the state to have the timestamp of the most recently assigned batch
    for batch in page.batches.iter() {
        match validate_timestamp_in_request(&current_offset, batch) {
            ValidateRequestResult::Reject { reason } => {
                let rejected = RejectedBatchInfo {
                    num_rows: batch.num_rows,
                    reason: reason.to_string(),
                };
                batches.push(CommittedBatch::Rejected(rejected));
            }
            ValidateRequestResult::Accept {
                start_offset,
                end_offset,
                timestamp,
                next_offset,
            } => {
                let accepted = AcceptedBatchInfo {
                    start_offset,
                    end_offset,
                    timestamp: timestamp.unwrap_or(now_ts),
                };
                current_offset = next_offset;
                batches.push(CommittedBatch::Accepted(accepted));
            }
        }
    }

    // Update state only if we accepted any data.
    if current_offset != start_offset {
        current_offset = current_offset.with_timestamp(now_ts);

        let end_offset = current_offset.previous();

        debug!(
            topic = %page.topic_name,
            partition_value = ?page.partition_value,
            start_offset = ?start_offset,
            end_offset = ?end_offset,
            file_ref = %file_ref,
            "Inserting partition folio"
        );

        let location = {
            use entities::partition_location::LocationType;
            use prost::Message;
            use wings_control_plane_core::pb::CommittedBatches;

            let partition_key = partition_key.clone();
            let folio_batches_pb = CommittedBatches::new(batches.clone()).encode_to_vec();

            entities::partition_location::ActiveModel {
                id: NotSet,
                tenant_id: Set(partition_key.tenant_id),
                namespace_id: Set(partition_key.namespace_id),
                topic_id: Set(partition_key.topic_id),
                partition_value: Set(partition_key.partition_value),
                start_offset: Set(start_offset.offset as _),
                end_offset: Set(end_offset.offset as _),
                file_ref: Set(file_ref),
                num_rows: Set(page.num_rows as _),
                location_type: Set(LocationType::Folio),
                folio_offset_bytes: Set(Some(page.offset_bytes as _)),
                folio_size_bytes: Set(Some(page.batch_size_bytes as _)),
                folio_batches_pb: Set(Some(folio_batches_pb)),
                parquet_metadata_pb: NotSet,
            }
        };

        entities::partition_location::Entity::insert(location)
            .exec(tx)
            .await?;

        debug!(
            topic = %page.topic_name,
            partition_value = ?page.partition_value,
            next_offset = ?current_offset,
            "Updating partition state"
        );

        let last_time_ms = current_offset
            .timestamp
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("timestamp to epoch");

        let new_state = entities::partition_state::ActiveModel {
            tenant_id: Set(partition_key.tenant_id),
            namespace_id: Set(partition_key.namespace_id),
            topic_id: Set(partition_key.topic_id),
            partition_value: Set(partition_key.partition_value),
            next_offset: Set(current_offset.offset as _),
            last_timestamp_ms: Set(last_time_ms.as_millis() as _),
        };

        entities::partition_state::Entity::insert(new_state)
            .on_conflict(
                OnConflict::columns([
                    entities::partition_state::Column::TenantId,
                    entities::partition_state::Column::NamespaceId,
                    entities::partition_state::Column::TopicId,
                    entities::partition_state::Column::PartitionValue,
                ])
                .update_columns([
                    entities::partition_state::Column::NextOffset,
                    entities::partition_state::Column::LastTimestampMs,
                ])
                .to_owned(),
            )
            .exec(tx)
            .await?;
    }

    let response = CommitPageResponse {
        topic_name: page.topic_name.clone(),
        partition_value: page.partition_value.clone(),
        batches,
    };

    Ok(response)
}

impl From<Error> for LogMetadataError {
    fn from(err: Error) -> Self {
        match err {
            Error::InvalidPageToken { .. } => LogMetadataError::InvalidArgument {
                message: err.to_string(),
            },
            Error::Entity { source } => source.into(),
            Error::Db { source } => LogMetadataError::Internal {
                message: format!("db error: {source}"),
            },
        }
    }
}
