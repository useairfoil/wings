use std::{collections::HashMap, time::SystemTime};

use sea_orm::{
    ActiveValue::{NotSet, Set},
    DatabaseTransaction, DbErr, EntityTrait, PaginatorTrait, QueryFilter, QuerySelect,
    sea_query::OnConflict,
};
use snafu::Snafu;
use tracing::debug;
use wings_control_plane_core::table_metadata::{
    AcceptedBatchInfo, CommitBatchRequest, CommittedBatch, ListPartitionsRequest,
    ListPartitionsResponse, RejectedBatchInfo, SeqNum, TableMetadataError,
    timestamp::{ValidateRequestResult, validate_timestamp_in_request},
    validate_batches_to_commit,
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
    pub async fn commit(
        &self,
        namespace: NamespaceName,
        batches: Vec<CommitBatchRequest>,
    ) -> Result<Vec<CommittedBatch>, Error> {
        // TODO: decide how to forward validation errors.
        validate_batches_to_commit(&namespace, &batches).unwrap();

        // Assign the same timestamp to all batches across pages.
        let now_ts = SystemTime::now();

        self.with_transaction(|tx| {
            Box::pin(async move {
                let mut committed_batches = vec![None; batches.len()];
                let mut partitions =
                    HashMap::<PartitionKey, Vec<(usize, CommitBatchRequest)>>::new();

                for (index, batch) in batches.into_iter().enumerate() {
                    partitions
                        .entry(PartitionKey::new(
                            &batch.table_name,
                            batch.partition_value.clone(),
                        ))
                        .or_default()
                        .push((index, batch));
                }

                for (partition_key, batches) in partitions {
                    commit_partition_batches(
                        tx,
                        partition_key,
                        batches,
                        now_ts,
                        &mut committed_batches,
                    )
                    .await?;
                }

                committed_batches
                    .into_iter()
                    .map(|batch| {
                        batch.ok_or_else(|| Error::Db {
                            source: DbErr::Custom(
                                "failed to collect committed batch response".to_string(),
                            ),
                        })
                    })
                    .collect()
            })
        })
        .await
    }

    pub async fn list_partitions(
        &self,
        request: ListPartitionsRequest,
    ) -> Result<ListPartitionsResponse, Error> {
        let ListPartitionsRequest {
            table_name,
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
                    .filter(entities::partition_state::table_condition(&table_name))
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

async fn commit_partition_batches(
    tx: &DatabaseTransaction,
    partition_key: PartitionKey,
    batches: Vec<(usize, CommitBatchRequest)>,
    now_ts: SystemTime,
    committed_batches: &mut [Option<CommittedBatch>],
) -> Result<(), Error> {
    let state = entities::partition_state::Entity::find_by_id(partition_key.clone())
        .lock_exclusive()
        .one(tx)
        .await?;

    let start_seqnum = state.map(|state| state.next_seqnum()).unwrap_or_default();

    let mut current_seqnum = start_seqnum;
    let mut locations = HashMap::<FolioBatchLocationKey, PendingFolioLocation>::new();

    // TODO: check that the timestamp is assigned correctly
    // we want the state to have the timestamp of the most recently assigned batch
    for (response_index, batch) in batches.iter() {
        match validate_timestamp_in_request(&current_seqnum, batch) {
            ValidateRequestResult::Reject { reason } => {
                let rejected = RejectedBatchInfo {
                    batch_id: batch.batch_id,
                    num_rows: batch.num_rows,
                    reason: reason.to_string(),
                };
                let committed = CommittedBatch::Rejected(rejected);
                locations
                    .entry(FolioBatchLocationKey::from(batch))
                    .or_insert_with(|| PendingFolioLocation::new(batch))
                    .push(batch.num_rows, committed.clone(), None);
                committed_batches[*response_index] = Some(committed);
            }
            ValidateRequestResult::Accept {
                start_seqnum,
                end_seqnum,
                timestamp,
                next_seqnum,
            } => {
                let accepted = AcceptedBatchInfo {
                    batch_id: batch.batch_id,
                    start_seqnum,
                    end_seqnum,
                    timestamp: timestamp.unwrap_or(now_ts),
                };
                current_seqnum = next_seqnum;
                let committed = CommittedBatch::Accepted(accepted);
                locations
                    .entry(FolioBatchLocationKey::from(batch))
                    .or_insert_with(|| PendingFolioLocation::new(batch))
                    .push(
                        batch.num_rows,
                        committed.clone(),
                        Some((start_seqnum, end_seqnum)),
                    );
                committed_batches[*response_index] = Some(committed);
            }
        }
    }

    // Update state only if we accepted any data.
    if current_seqnum != start_seqnum {
        current_seqnum = current_seqnum.with_timestamp(now_ts);

        for location in locations
            .into_values()
            .filter(|location| location.has_accepted())
        {
            insert_folio_location(tx, &partition_key, location, now_ts).await?;
        }

        debug!(
            table = %partition_key.table_id,
            partition_value = ?partition_key.partition_value,
            next_seqnum = ?current_seqnum,
            "Updating partition state"
        );

        let last_time_ms = current_seqnum
            .timestamp
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("timestamp to epoch");

        let new_state = entities::partition_state::ActiveModel {
            tenant_id: Set(partition_key.tenant_id),
            namespace_id: Set(partition_key.namespace_id),
            table_id: Set(partition_key.table_id),
            partition_value: Set(partition_key.partition_value),
            next_seqnum: Set(current_seqnum.seqnum as _),
            last_timestamp_ms: Set(last_time_ms.as_millis() as _),
        };

        entities::partition_state::Entity::insert(new_state)
            .on_conflict(
                OnConflict::columns([
                    entities::partition_state::Column::TenantId,
                    entities::partition_state::Column::NamespaceId,
                    entities::partition_state::Column::TableId,
                    entities::partition_state::Column::PartitionValue,
                ])
                .update_columns([
                    entities::partition_state::Column::NextSeqnum,
                    entities::partition_state::Column::LastTimestampMs,
                ])
                .to_owned(),
            )
            .exec(tx)
            .await?;
    }

    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct FolioBatchLocationKey {
    file_ref: String,
    offset_bytes: u64,
    batch_size_bytes: u64,
}

impl From<&CommitBatchRequest> for FolioBatchLocationKey {
    fn from(batch: &CommitBatchRequest) -> Self {
        Self {
            file_ref: batch.file_ref.clone(),
            offset_bytes: batch.page_offset_bytes,
            batch_size_bytes: batch.page_size_bytes,
        }
    }
}

struct PendingFolioLocation {
    file_ref: String,
    offset_bytes: u64,
    batch_size_bytes: u64,
    num_rows: u32,
    start_seqnum: Option<u64>,
    end_seqnum: Option<u64>,
    batches: Vec<CommittedBatch>,
}

impl PendingFolioLocation {
    fn new(batch: &CommitBatchRequest) -> Self {
        Self {
            file_ref: batch.file_ref.clone(),
            offset_bytes: batch.page_offset_bytes,
            batch_size_bytes: batch.page_size_bytes,
            num_rows: 0,
            start_seqnum: None,
            end_seqnum: None,
            batches: Vec::new(),
        }
    }

    fn push(&mut self, num_rows: u32, batch: CommittedBatch, accepted_offsets: Option<(u64, u64)>) {
        self.num_rows += num_rows;
        if let Some((start_seqnum, end_seqnum)) = accepted_offsets {
            self.start_seqnum.get_or_insert(start_seqnum);
            self.end_seqnum = Some(end_seqnum);
        }
        self.batches.push(batch);
    }

    fn has_accepted(&self) -> bool {
        self.start_seqnum.is_some() && self.end_seqnum.is_some()
    }
}

async fn insert_folio_location(
    tx: &DatabaseTransaction,
    partition_key: &PartitionKey,
    location: PendingFolioLocation,
    now_ts: SystemTime,
) -> Result<(), Error> {
    let start_seqnum = SeqNum {
        seqnum: location.start_seqnum.expect("accepted folio start seqnum"),
        timestamp: now_ts,
    };
    let end_seqnum = SeqNum {
        seqnum: location.end_seqnum.expect("accepted folio end seqnum"),
        timestamp: now_ts,
    };

    debug!(
        table = %partition_key.table_id,
        partition_value = ?partition_key.partition_value,
        start_seqnum = ?start_seqnum,
        end_seqnum = ?end_seqnum,
        file_ref = %location.file_ref,
        "Inserting partition folio"
    );

    let location = {
        use entities::partition_location::LocationType;
        use prost::Message;
        use wings_control_plane_core::pb::CommittedBatches;

        let folio_batches_pb = CommittedBatches::new(location.batches).encode_to_vec();

        entities::partition_location::ActiveModel {
            id: NotSet,
            tenant_id: Set(partition_key.tenant_id.clone()),
            namespace_id: Set(partition_key.namespace_id.clone()),
            table_id: Set(partition_key.table_id.clone()),
            partition_value: Set(partition_key.partition_value.clone()),
            start_seqnum: Set(start_seqnum.seqnum as _),
            end_seqnum: Set(end_seqnum.seqnum as _),
            file_ref: Set(location.file_ref),
            num_rows: Set(location.num_rows as _),
            location_type: Set(LocationType::Folio),
            folio_offset_bytes: Set(Some(location.offset_bytes as _)),
            folio_size_bytes: Set(Some(location.batch_size_bytes as _)),
            folio_batches_pb: Set(Some(folio_batches_pb)),
            parquet_metadata_pb: NotSet,
        }
    };

    entities::partition_location::Entity::insert(location)
        .exec(tx)
        .await?;

    Ok(())
}

impl From<Error> for TableMetadataError {
    fn from(err: Error) -> Self {
        match err {
            Error::InvalidPageToken { .. } => TableMetadataError::InvalidArgument {
                message: err.to_string(),
            },
            Error::Entity { source } => source.into(),
            Error::Db { source } => TableMetadataError::Internal {
                message: format!("db error: {source}"),
            },
        }
    }
}
