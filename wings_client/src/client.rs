use std::{
    collections::HashMap,
    pin::Pin,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime},
};

use arrow::array::RecordBatch;
use arrow_flight::{FlightData, PutResult};
use futures::{Stream, StreamExt, TryStreamExt};
use snafu::ResultExt;
use tokio::sync::{Mutex, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    Request, Status,
    metadata::{Ascii, MetadataValue},
};
use tracing::debug;
use wings_control_plane::{
    log_metadata::CommittedBatch,
    resources::{PartitionValue, Topic, TopicName},
};
use wings_flight::IngestionResponseMetadata;

use crate::{
    WingsClient,
    encode::IngestionFlightDataEncoder,
    error::{
        Result, StreamClosedSnafu, TicketDecodeSnafu, TimeoutSnafu, TonicSnafu,
        UnexpectedRequestIdSnafu,
    },
};

const DEFAULT_CHANNEL_SIZE: usize = 1024;

/// A client to read and write data for a specific topic.
pub struct TopicClient {
    topic_name: TopicName,
    tx: mpsc::Sender<FlightData>,
    next_request_id: AtomicU64,
    // TODO: if the lock is not held across async, replace the implementation
    encoder: Mutex<IngestionFlightDataEncoder>,
    inner: Mutex<InnerClient>,
    timeout_duration: Duration,
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
        let schema_message =
            encoder.encode_schema(&topic.name, topic.schema_without_partition_field());
        let _ = tx.send(schema_message).await;

        let request = prepare_request(&topic.name, rx);
        let mut response_stream = inner.do_put(request).await?.into_inner().boxed();

        let Some(put_result) = response_stream.try_next().await? else {
            return StreamClosedSnafu.fail();
        };

        let request_id =
            IngestionResponseMetadata::try_decode_schema_message(put_result.app_metadata)
                .context(TicketDecodeSnafu {})?;

        if request_id != 0 {
            return UnexpectedRequestIdSnafu {
                expected: 0u64,
                actual: request_id,
            }
            .fail();
        }

        let inner = InnerClient {
            response_stream,
            completed: Default::default(),
        };

        Ok(Self {
            topic_name: topic.name.clone(),
            tx,
            encoder: Mutex::new(encoder),
            next_request_id: AtomicU64::new(1),
            inner: Mutex::new(inner),
            timeout_duration: Duration::from_secs(3),
        })
    }

    pub fn topic_name(&self) -> &TopicName {
        &self.topic_name
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

    async fn wait_for_response(&self, request_id: u64) -> Result<CommittedBatch> {
        let mut inner = self.inner.lock().await;
        inner
            .wait_for_response(request_id, self.timeout_duration)
            .await
    }
}

impl WriteResponse<'_> {
    pub async fn wait_for_response(self) -> Result<CommittedBatch> {
        self.client.wait_for_response(self.request_id).await
    }
}

impl InnerClient {
    async fn wait_for_response(
        &mut self,
        request_id: u64,
        timeout_duration: Duration,
    ) -> Result<CommittedBatch> {
        if let Some(response) = self.completed.remove(&request_id) {
            return Ok(response);
        };

        loop {
            let response = tokio::time::timeout(timeout_duration, self.response_stream.try_next());

            let put_result = match response.await {
                Err(_) => return TimeoutSnafu {}.fail(),
                Ok(Ok(None)) => return StreamClosedSnafu {}.fail(),
                Ok(Ok(Some(put_result))) => put_result,
                Ok(Err(err)) => return Err(err).context(TonicSnafu {}),
            };

            let response = IngestionResponseMetadata::try_decode(put_result.app_metadata.clone())
                .context(TicketDecodeSnafu {})?;

            assert_ne!(response.request_id, 0, "received invalid request id");

            if response.request_id == request_id {
                return Ok(response.result);
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
