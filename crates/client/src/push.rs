use std::{
    collections::HashMap,
    pin::Pin,
    sync::atomic::{AtomicU32, Ordering},
    time::{Duration, SystemTime},
};

use arrow::array::RecordBatch;
use arrow_flight::{FlightData, PutResult, flight_service_client::FlightServiceClient};
use futures::{Stream, StreamExt, TryStreamExt};
use snafu::ResultExt;
use tokio::sync::{Mutex, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Status, transport::Channel};
use tracing::debug;
use wings_control_plane_core::table_metadata::CommittedBatch;
use wings_flight::IngestionResponseMetadata;
use wings_resources::{NamespaceName, PartitionValue, Table, TableName};

use crate::{
    WingsClient,
    encode::IngestionFlightDataEncoder,
    error::{Result, StreamClosedSnafu, TicketDecodeSnafu, TimeoutSnafu, TonicSnafu},
    metadata::new_request_for_namespace,
};

const DEFAULT_CHANNEL_SIZE: usize = 1024;

/// A client to push data to a specific namespace.
pub struct PushClient {
    namespace_name: NamespaceName,
    table_name: TableName,
    next_batch_id: AtomicU32,
    encoder: Mutex<IngestionFlightDataEncoder>,
    inner: Mutex<InnerClient>,
    timeout_duration: Duration,
}

pub struct WriteRequest {
    pub table_name: TableName,
    pub partition_value: Option<PartitionValue>,
    pub timestamp: Option<SystemTime>,
    pub data: RecordBatch,
}

pub struct WriteResponse<'a> {
    client: &'a PushClient,
    batch_id: u32,
}

struct InnerClient {
    flight: FlightServiceClient<Channel>,
    session: Option<PushSession>,
    completed: HashMap<u32, CommittedBatch>,
}

struct PushSession {
    tx: mpsc::Sender<FlightData>,
    response_stream: Pin<Box<dyn Stream<Item = Result<PutResult, Status>> + Send>>,
    committed: bool,
}

impl PushClient {
    pub(crate) async fn new(client: &WingsClient, table: Table) -> Result<Self> {
        debug!(table = ?table, "connecting to flight push endpoint");
        let inner = InnerClient {
            flight: client.flight.clone(),
            session: None,
            completed: Default::default(),
        };

        Ok(Self {
            namespace_name: table.name.parent.clone(),
            table_name: table.name.clone(),
            encoder: Mutex::new(IngestionFlightDataEncoder::new()),
            next_batch_id: AtomicU32::new(0),
            inner: Mutex::new(inner),
            timeout_duration: Duration::from_secs(3),
        })
    }

    pub fn table_name(&self) -> &TableName {
        &self.table_name
    }

    pub async fn push(&self, request: WriteRequest) -> Result<WriteResponse<'_>> {
        let batch_id = self.next_batch_id.fetch_add(1, Ordering::SeqCst);
        let flight_data = self.encoder.lock().await.encode(batch_id, request)?;

        debug!(
            batch_id,
            num_messages = flight_data.len(),
            "sending flight data"
        );

        self.inner
            .lock()
            .await
            .send_batch(&self.namespace_name, flight_data)
            .await?;

        Ok(WriteResponse {
            client: self,
            batch_id,
        })
    }

    async fn wait_for_response(&self, batch_id: u32) -> Result<CommittedBatch> {
        let commit_message = self.encoder.lock().await.encode_commit();
        let mut inner = self.inner.lock().await;
        inner
            .wait_for_response(batch_id, commit_message, self.timeout_duration)
            .await
    }
}

impl WriteResponse<'_> {
    pub async fn wait_for_response(self) -> Result<CommittedBatch> {
        self.client.wait_for_response(self.batch_id).await
    }
}

impl InnerClient {
    async fn send_batch(
        &mut self,
        namespace_name: &NamespaceName,
        flight_data: Vec<FlightData>,
    ) -> Result<()> {
        if self.session.is_none() {
            let (tx, rx) = mpsc::channel(DEFAULT_CHANNEL_SIZE);
            let input_stream = ReceiverStream::new(rx);
            let request = new_request_for_namespace(namespace_name, input_stream);
            let response_stream = self.flight.do_put(request).await?.into_inner().boxed();

            self.session = Some(PushSession {
                tx,
                response_stream,
                committed: false,
            });
        }

        let session = self.session.as_mut().expect("session was checked above");

        if session.committed {
            return StreamClosedSnafu.fail();
        }

        for message in flight_data {
            session
                .tx
                .send(message)
                .await
                .map_err(|_| StreamClosedSnafu.build())?;
        }

        Ok(())
    }

    async fn wait_for_response(
        &mut self,
        batch_id: u32,
        commit_message: FlightData,
        timeout_duration: Duration,
    ) -> Result<CommittedBatch> {
        if let Some(response) = self.completed.remove(&batch_id) {
            return Ok(response);
        };

        self.commit(commit_message).await?;
        self.read_commit_response(timeout_duration).await?;

        self.completed
            .remove(&batch_id)
            .ok_or_else(|| StreamClosedSnafu.build())
    }

    async fn commit(&mut self, commit_message: FlightData) -> Result<()> {
        let Some(session) = self.session.as_mut() else {
            return StreamClosedSnafu.fail();
        };

        if !session.committed {
            session
                .tx
                .send(commit_message)
                .await
                .map_err(|_| StreamClosedSnafu.build())?;
            session.committed = true;
        }

        Ok(())
    }

    async fn read_commit_response(&mut self, timeout_duration: Duration) -> Result<()> {
        let Some(mut session) = self.session.take() else {
            return StreamClosedSnafu.fail();
        };

        let response = tokio::time::timeout(timeout_duration, session.response_stream.try_next());
        let put_result = match response.await {
            Err(_) => return TimeoutSnafu {}.fail(),
            Ok(Ok(None)) => return StreamClosedSnafu {}.fail(),
            Ok(Ok(Some(put_result))) => put_result,
            Ok(Err(err)) => return Err(err).context(TonicSnafu {}),
        };

        let response = IngestionResponseMetadata::try_decode(put_result.app_metadata.as_ref())
            .context(TicketDecodeSnafu {})?;

        for batch in response.batches {
            self.completed.insert(batch.batch_id(), batch);
        }

        Ok(())
    }
}
