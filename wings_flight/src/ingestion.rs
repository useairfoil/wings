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
                    Err(_err) => CommittedBatch::Rejected(RejectedBatchInfo { num_messages }),
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
