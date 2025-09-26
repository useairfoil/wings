use std::{
    collections::HashMap,
    pin::Pin,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime},
};

use arrow::array::RecordBatch;
use arrow_flight::{FlightData, PutResult};
use futures::{Stream, StreamExt, TryStreamExt};
use snafu::{ResultExt, Snafu};
use tokio::sync::{Mutex, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    Request, Status,
    metadata::{Ascii, MetadataValue},
};
use tracing::debug;
use wings_control_plane::{
    log_metadata::{AcceptedBatchInfo, CommittedBatch, RejectedBatchInfo},
    resources::{PartitionValue, Topic, TopicName},
};
use wings_flight::{IngestionResponseMetadata, TicketDecodeError};

use crate::{WingsClient, encode::IngestionFlightDataEncoder, error::Result};

const DEFAULT_CHANNEL_SIZE: usize = 1024;

/// A client to read and write data for a specific topic.
pub struct TopicClient {
    tx: mpsc::Sender<FlightData>,
    next_request_id: AtomicU64,
    // TODO: if the lock is not held across async, replace the implementation
    encoder: Mutex<IngestionFlightDataEncoder>,
    inner: Mutex<InnerClient>,
}

#[derive(Debug, Snafu)]
pub enum WriteError {
    #[snafu(display("Stream closed"))]
    StreamClosed,
    #[snafu(display("Timeout"))]
    Timeout,
    #[snafu(display("Tonic error"))]
    Tonic { source: Status },
    #[snafu(display("Rejected batch"))]
    Batch { info: RejectedBatchInfo },
    #[snafu(display("Ticket decode error"))]
    TicketDecode { source: TicketDecodeError },
}

pub struct WriteRequest {
    pub partition_value: Option<PartitionValue>,
    pub timestamp: Option<SystemTime>,
    pub data: RecordBatch,
}

pub struct WriteResponse<'a> {
    client: &'a TopicClient,
    request_id: u64,
}

struct InnerClient {
    response_stream: Pin<Box<dyn Stream<Item = Result<PutResult, Status>> + Send>>,
    completed: HashMap<u64, CommittedBatch>,
}

impl TopicClient {
    pub(crate) async fn new(client: &WingsClient, topic: Topic) -> Result<Self> {
        debug!(topic = ?topic, "connecting to flight push endpoint");
        let mut inner = client.flight.clone();

        // TODO: make it configurable.
        let (tx, rx) = mpsc::channel::<FlightData>(DEFAULT_CHANNEL_SIZE);

        let mut encoder = IngestionFlightDataEncoder::new();

        // The Arrow Flight server expects a message with a command to decide
        // what action to take and sending a response to the client. Enqueue the
        // schema now to avoid hanging when calling `do_put`.
        let schema_message = encoder.encode_schema(&topic.name, topic.schema());
        let _ = tx.send(schema_message).await;

        let request = prepare_request(&topic.name, rx);
        let response_stream = inner.do_put(request).await?.into_inner().boxed();

        let inner = InnerClient {
            response_stream,
            completed: Default::default(),
        };

        Ok(Self {
            tx,
            encoder: Mutex::new(encoder),
            next_request_id: AtomicU64::new(1),
            inner: Mutex::new(inner),
        })
    }

    pub async fn push(&self, request: WriteRequest) -> Result<WriteResponse<'_>> {
        let request_id = self.next_request_id.fetch_add(1, Ordering::SeqCst);
        let flight_data = self.encoder.lock().await.encode(request_id, request)?;

        debug!(
            request_id,
            num_messages = flight_data.len(),
            "sending flight data"
        );

        // TODO: since we're sending multiple messages, we should add a timeout.
        for message in flight_data {
            let _ = self.tx.send(message).await;
        }

        Ok(WriteResponse {
            client: self,
            request_id,
        })
    }

    async fn wait_for_response(&self, request_id: u64) -> Result<AcceptedBatchInfo, WriteError> {
        let mut inner = self.inner.lock().await;
        inner.wait_for_response(request_id).await
    }
}

impl WriteResponse<'_> {
    pub async fn wait_for_response(self) -> Result<AcceptedBatchInfo, WriteError> {
        self.client.wait_for_response(self.request_id).await
    }
}

impl InnerClient {
    async fn wait_for_response(
        &mut self,
        request_id: u64,
    ) -> Result<AcceptedBatchInfo, WriteError> {
        if let Some(response) = self.completed.remove(&request_id) {
            match response {
                CommittedBatch::Accepted(info) => return Ok(info),
                CommittedBatch::Rejected(reason) => return Err(WriteError::Batch { info: reason }),
            }
        };

        loop {
            let response =
                tokio::time::timeout(Duration::from_secs(1), self.response_stream.try_next());
            let put_result = match response.await {
                Err(_) => return Err(WriteError::Timeout),
                Ok(Ok(None)) => return Err(WriteError::StreamClosed),
                Ok(Ok(Some(put_result))) => put_result,
                Ok(Err(err)) => return Err(WriteError::Tonic { source: err }),
            };

            // TODO: this is shite. the issue is that request_id has no result so it would fail.
            // we need to improve on this.
            let response =
                IngestionResponseMetadata::try_decode_partial(put_result.app_metadata.clone())
                    .context(TicketDecodeSnafu {})?;

            if response.request_id == 0 {
                continue;
            }

            let response = IngestionResponseMetadata::try_decode(put_result.app_metadata)
                .context(TicketDecodeSnafu {})?;

            if response.request_id == request_id {
                match response.result {
                    CommittedBatch::Accepted(info) => return Ok(info),
                    CommittedBatch::Rejected(reason) => {
                        return Err(WriteError::Batch { info: reason });
                    }
                }
            } else {
                self.completed.insert(response.request_id, response.result);
            }
        }
    }
}

fn prepare_request(
    topic_name: &TopicName,
    rx: mpsc::Receiver<FlightData>,
) -> Request<ReceiverStream<FlightData>> {
    let input_stream = ReceiverStream::new(rx);
    let mut request = Request::new(input_stream);

    // PANIC: we know that the namespace name must be a valid ASCII string
    let namespace_ascii: MetadataValue<Ascii> = topic_name
        .parent()
        .to_string()
        .parse()
        .expect("non-ascii namespace name");

    request
        .metadata_mut()
        .insert("x-wings-namespace", namespace_ascii);

    request
}

impl From<Status> for WriteError {
    fn from(status: Status) -> Self {
        WriteError::Tonic { source: status }
    }
}
