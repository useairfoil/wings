//! ## Arrow Flight SQL ingestion
//!
//! This module implements ingestion on top of the Arrow Flight SQL protocol.
//!
//! ## Input stream
//!
//! The input stream is a sequence of schema messages, followed by one or more batch messages.
//! To terminate the input stream, the client sends a schema message with an
//! empty schema, this signal to the server that data must be committed.
//! Schema messages signal the start of a new batch and contain the metadata necessary for ingestion.
//!
//!  - the batch id (required)
//!  - the topic name (required)
//!  - the partition value (required if the topic is partitioned)
//!  - a timestamp for the batch (optional)
//!
//! This metadata is contained in the `FlightData.app_metadata` field.
//!
//! ## Response stream
//!
//! After the server receives the empty schema message, it commits the data to the log metadata service.
//! After that, it sends a single response message with the commit status
//! (either accepted or rejected) of each batch, identified by their batch id.
//!
//! ## Error handling
//!
//! If, for any reason, the server encounters an error while processing the
//! input stream, it should 1) abort the ingestion and 2) return an error
//! response to the client.
//!
//! ## Diagram
//!
//! ```txt
//! Client                             Server
//!   |                                  |
//!   |-id=0-------- Schema ------------>|     # The client sends the schema message with the batch metadata
//!   |------------- Batch ------------->|     # The client sends batches of data
//!   |------------- Batch ------------->|     # The batches belong to the same batch id
//!   |                                  |
//!   |-id=1-------- Schema ------------>|     # This batch can have different topic/partition than the previous one
//!   |------------- Batch ------------->|
//!   |------------- Batch ------------->|
//!   |                                  |
//!   |------------- Schema ------------>|     # Empty schema message, server can commit batches
//!   |                                  |
//!   |<--[id=0:accepted;id=1:rejected]--|     # Server responds with the commit status of each batch
//!   |                                  |
//! ```
use std::{sync::Arc, time::SystemTime};

use arrow::{compute::concat_batches, datatypes::SchemaRef, record_batch::RecordBatch};
use arrow_flight::{
    PutResult,
    decode::{DecodedPayload, FlightDataDecoder},
};
use futures::TryStreamExt;
use prost::bytes::Bytes;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;
use tracing::debug;
use wings_control_plane_core::cluster_metadata::cache::TopicCache;
use wings_ingestor_core::{IngestionRequest, IngestorClient, WriteBatchRequest};
use wings_resources::{Namespace, PartitionValue, Topic};

use crate::{IngestionRequestMetadata, IngestionResponseMetadata, error::FlightServerError};

struct PendingBatch {
    batch_id: u32,
    topic: Arc<Topic>,
    partition: Option<PartitionValue>,
    timestamp: Option<SystemTime>,
    schema: SchemaRef,
    records: Vec<RecordBatch>,
}

pub async fn process_ingestion_stream(
    namespace: Arc<Namespace>,
    topic_cache: TopicCache,
    mut request_stream: FlightDataDecoder,
    ingestor: IngestorClient,
    tx: mpsc::Sender<Result<PutResult, Status>>,
) -> Result<(), FlightServerError> {
    let (write_tx, write_rx) = mpsc::channel(64);
    let ingestion_task = tokio::spawn({
        let namespace = namespace.clone();
        async move {
            ingestor
                .ingest(namespace, ReceiverStream::new(write_rx))
                .await
        }
    });

    let input_result = stream_ingestion_requests(topic_cache, &mut request_stream, &write_tx).await;

    if let Err(err) = input_result {
        let _ = write_tx.send(IngestionRequest::Abort).await;
        drop(write_tx);
        let _ = ingestion_task.await;
        return Err(err);
    }

    drop(write_tx);

    let committed = ingestion_task
        .await
        .map_err(|_| FlightServerError::internal("ingestion task failed to join".to_string()))??;

    let _ = tx
        .send(Ok(PutResult {
            app_metadata: IngestionResponseMetadata::new(committed).encode(),
        }))
        .await;

    debug!("Ingestion complete");

    Ok(())
}

async fn stream_ingestion_requests(
    topic_cache: TopicCache,
    request_stream: &mut FlightDataDecoder,
    write_tx: &mpsc::Sender<IngestionRequest>,
) -> Result<(), FlightServerError> {
    let mut pending = None;

    while let Some(flight_data) = request_stream.try_next().await? {
        let app_metadata = flight_data.app_metadata();

        match flight_data.payload {
            DecodedPayload::Schema(schema) if schema.fields().is_empty() => {
                flush_pending_batch(&mut pending, write_tx).await?;
                return Ok(());
            }
            DecodedPayload::Schema(schema) => {
                flush_pending_batch(&mut pending, write_tx).await?;
                pending = Some(PendingBatch::new(topic_cache.clone(), app_metadata, schema).await?);
            }
            DecodedPayload::RecordBatch(batch) => {
                let Some(pending) = pending.as_mut() else {
                    return Err(FlightServerError::invalid_ticket(
                        "record batch received before schema",
                    ));
                };
                pending.records.push(batch);
            }
            DecodedPayload::None => {}
        }
    }

    Ok(())
}

async fn flush_pending_batch(
    pending: &mut Option<PendingBatch>,
    write_tx: &mpsc::Sender<IngestionRequest>,
) -> Result<(), FlightServerError> {
    let Some(pending) = pending.take() else {
        return Ok(());
    };

    let request = pending.into_write_batch_request()?;

    debug!(
        topic = %request.topic.name,
        batch_id = request.batch_id,
        size = request.records.num_rows(),
        "Writing batch to topic",
    );

    write_tx
        .send(request.into())
        .await
        .map_err(|_| FlightServerError::internal("ingestion task closed"))
}

impl PendingBatch {
    async fn new(
        topic_cache: TopicCache,
        app_metadata: Bytes,
        schema: SchemaRef,
    ) -> Result<Self, FlightServerError> {
        let metadata =
            IngestionRequestMetadata::try_decode(app_metadata.as_ref()).map_err(|_| {
                FlightServerError::invalid_ticket("failed to decode ingestion metadata".to_string())
            })?;

        let topic_name = metadata.topic_name;
        let batch_id = metadata.batch_id;

        let topic = topic_cache.get(topic_name).await?;

        Ok(Self {
            batch_id,
            topic,
            partition: metadata.partition_value,
            timestamp: metadata.timestamp,
            schema,
            records: Vec::new(),
        })
    }

    fn into_write_batch_request(self) -> Result<WriteBatchRequest, FlightServerError> {
        if self.records.is_empty() {
            return Err(FlightServerError::invalid_ticket(
                "schema message was not followed by a record batch",
            ));
        }

        let records = if self.records.len() == 1 {
            self.records
                .into_iter()
                .next()
                .expect("records length checked above")
        } else {
            concat_batches(&self.schema, self.records.iter())?
        };

        Ok(WriteBatchRequest {
            batch_id: self.batch_id,
            topic: self.topic,
            partition: self.partition,
            timestamp: self.timestamp,
            records,
        })
    }
}
