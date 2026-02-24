use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    usize,
};

use arrow::array::{UInt64Array, UInt64Builder};
use datafusion::common::arrow::{
    datatypes::SchemaRef as ArrowSchemaRef, record_batch::RecordBatch,
};
use snafu::ResultExt;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use wings_client::{WingsClient, WriteRequest};
use wings_control_plane_core::log_metadata::CommittedBatch;
use wings_resources::Topic;

use crate::{
    error::{FetchSnafu, PushSnafu, Result},
    log::{Event, OperationEvent},
};

#[derive(Debug, Clone, Copy)]
enum Operation {
    Push,
    Fetch,
}

pub async fn run_test(
    client_id: u64,
    event_id: Arc<AtomicU64>,
    batch_size: usize,
    iterations: usize,
    tx: mpsc::Sender<Event>,
    client: WingsClient,
    topic: Topic,
    ct: CancellationToken,
) -> Result<()> {
    let arrow_schema = topic.arrow_schema_without_partition_field();
    let push_client = client
        .push_client(topic.name.clone())
        .await
        .context(PushSnafu {})?;
    let fetch_client = client
        .fetch_client(topic.name.clone())
        .await
        .context(FetchSnafu {})?;

    let mut end_offset = 0;

    for _ in 0..iterations {
        if ct.is_cancelled() {
            break;
        }

        let id = event_id.fetch_add(1, Ordering::Relaxed);

        match random_operation() {
            Operation::Push => {
                let (batch, last_value) = random_batch(arrow_schema.clone(), batch_size)?;

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
                        data: batch,
                        partition_value: None,
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
                                end_offset: info.end_offset,
                            },
                        })
                        .await?;

                        end_offset = info.end_offset;
                    }
                    CommittedBatch::Rejected(info) => {
                        eprintln!("[{client_id}] batch rejected: {:?}", info);
                    }
                }
            }
            Operation::Fetch => {
                tx.send(Event {
                    id,
                    client_id,
                    event: OperationEvent::FetchStart { offset: end_offset },
                })
                .await?;

                let response = fetch_client
                    .clone()
                    .with_max_batch_size(usize::MAX)
                    .with_min_batch_size(0)
                    .with_offset(end_offset)
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
                let offset_col = batch
                    .column_by_name("__offset__")
                    .expect("missing offset col")
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .expect("offset col is not UInt64Array");

                let value = value_col.value(0);
                let offset = offset_col.value(0);

                tx.send(Event {
                    id,
                    client_id,
                    event: OperationEvent::FetchEnd { offset, value },
                })
                .await?;
            }
        }
    }

    Ok(())
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
