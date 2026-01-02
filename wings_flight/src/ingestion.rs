//! Ingestion handler for the Wings Flight server.
//!
//! ## Input stream
//!
//! ### Schema and flight descriptor message
//!
//! The first message in the input stream must contain the schema and the
//! `FlightDescriptor` that describes the data being ingested.
//! The descriptor must contain a single-element `path` with the topic name (the full name, including tenant and namespace).
//! The topic namespace must match the namespace in the request.
//! The schema describes the structure of the data being ingested.
//!
//! ### Batch messages
//!
//! All messages after the schema messages must contain data. Notice that the same stream can ingest multiple batches.
//!
//! All messages must contain the following `IngestionRequestMetadata` information in the `FlightData.app_metadata` field:
//!
//!  - `request_id: u64`: the unique request identifier used to correlate the request with the response.
//!  - `partition_value: Option<PartitionValue>`: the partition value used to partition the data, if any.
//!  - `timestamp: Option<SystemTime>`: the timestamp to assign to the data, if any.
//!
//! Notice that `request_id = 0` is reserved for the schema message.
//!
//! If the client wants to ingest large batches (> 2MiB), it must split the batch into smaller batches. At the moment,
//! these batches must have different `request_id`s, but we plan to support joining them into a single batch in the future.
//!
//! ## Response stream
//!
//! The response stream contains the results of the ingestion requests in the `PutResult.app_metadata` field. This field
//! contains a `IngestionResponseMetadata`:
//!
//!  - `request_id: u64`: the request id.
//!  - `result: CommittedBatch`: the committed batch. The batch can be either accepted or rejected. If accepted, the information about the batch is included.
//!
//! ## Diagram
//!
//! ```txt
//! Client                             Server
//!   |                                  |
//!   |-id=0-------- Schema ------------>|     # The client sends the schema message with the topic name
//!   |<----------------------------id=0-|
//!   |                                  |
//!   |                                  |
//!   |-id=1-------- Batch ------------->|     # The client sends batches of data
//!   |-id=2-------- Batch ------------->|
//!   |                                  |
//!   |<---------- Accepted --------id=1-|     # The server sends the response as it finishes ingesting the batch
//!   |-id=3-------- Batch ------------->|
//!   |                                  |
//!   |<---------- Accepted --------id=3-|     # Responses are not guaranteed to be in order
//!   |-id=4-------- Batch ------------->|
//!   |                                  |
//!   |<---------- Accepted --------id=2-|
//!   |<---------- Rejected --------id=4-|     # Batches can be rejected for various reasons
//!   |                                  |
//! ```
use std::sync::Arc;

use arrow_flight::{
    PutResult,
    decode::{DecodedPayload, FlightDataDecoder},
};
use futures::stream::FuturesUnordered;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tonic::Status;
use tracing::debug;
use wings_control_plane::{
    log_metadata::{CommittedBatch, RejectedBatchInfo},
    resources::{Namespace, Topic},
};
use wings_ingestor_core::{BatchIngestorClient, WriteBatchRequest};

use crate::{IngestionRequestMetadata, IngestionResponseMetadata, error::FlightServerError};

pub async fn process_ingestion_stream(
    namespace: Arc<Namespace>,
    topic: Arc<Topic>,
    mut request_stream: FlightDataDecoder,
    ingestor: BatchIngestorClient,
    tx: mpsc::Sender<Result<PutResult, Status>>,
) -> Result<(), FlightServerError> {
    // Send a message to the channel in response to the first request.
    let Ok(_) = tx
        .send(Ok(PutResult {
            app_metadata: Default::default(),
        }))
        .await
    else {
        return Ok(());
    };

    let mut ingestion_fut = FuturesUnordered::new();

    loop {
        tokio::select! {
            ingestion_result = ingestion_fut.next(), if !ingestion_fut.is_empty() => {
                let Some((request_id, num_messages, response)) = ingestion_result else {
                    continue;
                };

                let response = match response {
                    Ok(info) => CommittedBatch::Accepted(info),
                    // TODO: this is a more general write error and not a rejected batch
                    Err(err) => {
                        debug!(err = ?err, "failed to commit batch");
                        CommittedBatch::Rejected(RejectedBatchInfo { num_messages, reason: "INTERNAL_ERROR".to_string() })
                    },
                };

                let Ok(_) = tx
                    .send(Ok(PutResult {
                        app_metadata: IngestionResponseMetadata::new(
                            request_id,
                            response,
                        )
                        .encode(),
                    }))
                    .await
                else {
                    break;
                };
            }
            flight_data = request_stream.try_next() => {
                let Some(flight_data) = flight_data? else {
                    break;
                };

                let metadata =
                    IngestionRequestMetadata::try_decode(flight_data.app_metadata()).unwrap();

                let DecodedPayload::RecordBatch(batch) = flight_data.payload else {
                    continue;
                };

                let request_id = metadata.request_id;
                let num_messages = batch.num_rows() as u32;
                let namespace = namespace.clone();
                let topic = topic.clone();
                let ingestor = ingestor.clone();

                debug!(topic = %topic.name, size = num_messages, "Writing batch to topic");

                ingestion_fut.push(async move {
                    let response = ingestor
                        .write(WriteBatchRequest {
                            namespace,
                            topic,
                            partition: metadata.partition_value,
                            timestamp: metadata.timestamp,
                            records: batch,
                        })
                        .await;
                    (request_id, num_messages, response)
                });
            }
        }
    }

    debug!(topic = %topic.name, "Ingestion complete");

    Ok(())
}
