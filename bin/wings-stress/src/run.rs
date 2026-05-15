use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use arrow::array::{UInt64Array, UInt64Builder};
use datafusion::common::arrow::{
    datatypes::SchemaRef as ArrowSchemaRef, record_batch::RecordBatch,
};
use snafu::ResultExt;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use wings_client::{WingsClient, WriteRequest};
use wings_control_plane_core::table_metadata::CommittedBatch;
use wings_resources::{PartitionValue, Table};

use crate::{
    error::{FetchSnafu, PushSnafu, Result},
    log::{Event, OperationEvent},
};

#[derive(Debug, Clone, Copy)]
enum Operation {
    Push,
    Fetch,
}

#[derive(Debug, Clone)]
pub struct RunContext {
    event_id: Arc<AtomicU64>,
    pub batch_size: usize,
    pub iterations: usize,
    num_partitions: Option<u64>,
    partition: Arc<AtomicU64>,
}

pub async fn run_test(
    ctx: RunContext,
    client_id: u64,
    tx: mpsc::Sender<Event>,
    client: WingsClient,
    table: Table,
    ct: CancellationToken,
) -> Result<()> {
    let arrow_schema = table.arrow_schema_without_partition_field();
    let push_client = client
        .push_client(table.name.clone())
        .await
        .context(PushSnafu {})?;
    let fetch_client = client
        .fetch_client(table.name.clone())
        .await
        .context(FetchSnafu {})?;

    let mut end_seqnum = 0;

    for _ in 0..ctx.iterations {
        if ct.is_cancelled() {
            break;
        }

        let id = ctx.next_event_id();

        match random_operation() {
            Operation::Push => {
                let (batch, last_value) = random_batch(arrow_schema.clone(), ctx.batch_size)?;

                let partition_value = ctx.next_partition_value();

                tx.send(Event {
                    id,
                    client_id,
                    event: OperationEvent::PushStart {
                        num_rows: batch.num_rows(),
                        last_value,
                    },
                })
                .await?;

                let response = push_client
                    .push(WriteRequest {
                        table_name: table.name.clone(),
                        data: batch,
                        partition_value,
                        timestamp: None,
                    })
                    .await
                    .context(PushSnafu {})?
                    .wait_for_response()
                    .await
                    .context(PushSnafu {})?;

                match response {
                    CommittedBatch::Accepted(info) => {
                        tx.send(Event {
                            id,
                            client_id,
                            event: OperationEvent::PushEnd {
                                end_seqnum: info.end_seqnum,
                            },
                        })
                        .await?;

                        end_seqnum = info.end_seqnum;
                    }
                    CommittedBatch::Rejected(info) => {
                        eprintln!("[{client_id}] batch rejected: {:?}", info);
                    }
                }
            }
            Operation::Fetch => {
                let partition_value = ctx.next_partition_value();

                tx.send(Event {
                    id,
                    client_id,
                    event: OperationEvent::FetchStart { seqnum: end_seqnum },
                })
                .await?;

                let response = fetch_client
                    .clone()
                    .with_max_batch_size(usize::MAX)
                    .with_min_batch_size(0)
                    .with_seqnum(end_seqnum)
                    .with_partition(partition_value)
                    .fetch_next()
                    .await
                    .context(FetchSnafu {})?;

                let Some(batch) = response.first() else {
                    continue;
                };

                let value_col = batch
                    .column_by_name("col")
                    .expect("missing value col")
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .expect("value col is not UInt64Array");
                let seqnumt_col = batch
                    .column_by_name("__seqnum__")
                    .expect("missing seqnum col")
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .expect("seqnum col is not UInt64Array");

                let value = value_col.value(0);
                let seqnum = seqnumt_col.value(0);

                tx.send(Event {
                    id,
                    client_id,
                    event: OperationEvent::FetchEnd { seqnum, value },
                })
                .await?;
            }
        }
    }

    Ok(())
}

impl RunContext {
    pub fn new(batch_size: usize, iterations: usize, num_partitions: Option<usize>) -> Self {
        Self {
            event_id: Arc::new(AtomicU64::new(0)),
            batch_size,
            iterations,
            num_partitions: num_partitions.map(|n| n as u64),
            partition: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn next_event_id(&self) -> u64 {
        self.event_id.fetch_add(1, Ordering::Relaxed)
    }

    pub fn next_partition_value(&self) -> Option<PartitionValue> {
        let num_parts = self.num_partitions?;
        let mut current = self.partition.load(Ordering::Relaxed);

        loop {
            let new_val = (current + 1) % num_parts;
            match self.partition.compare_exchange(
                current,
                new_val,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Some(PartitionValue::UInt64(new_val)),
                Err(old) => current = old,
            }
        }
    }
}

fn random_batch(schema: ArrowSchemaRef, batch_size: usize) -> Result<(RecordBatch, u64)> {
    let mut c = UInt64Builder::with_capacity(batch_size);
    let mut last_value = 0;

    for _ in 0..batch_size {
        last_value = rand::random::<u64>();
        c.append_value(last_value);
    }

    let batch = RecordBatch::try_new(schema, vec![Arc::new(c.finish())])?;

    Ok((batch, last_value))
}

fn random_operation() -> Operation {
    match rand::random::<u8>() % 2 {
        0 => Operation::Push,
        _ => Operation::Fetch,
    }
}
