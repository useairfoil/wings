use std::time::Duration;

use arrow::array::RecordBatch;
use arrow_flight::{
    decode::FlightDataDecoder, error::FlightError, flight_service_client::FlightServiceClient,
};
use futures::TryStreamExt;
use snafu::ResultExt;
use tokio_stream::StreamExt;
use tonic::transport::Channel;
use tracing::debug;
use wings_flight::FetchTicket;
use wings_resources::{PartitionValue, Topic, TopicName};
use wings_schema::{DataType, Field};

use crate::{
    WingsClient,
    error::{FlightSnafu, MissingPartitionValueSnafu, Result, TicketEncodeSnafu, TonicSnafu},
    metadata::new_request_for_namespace,
};

#[derive(Clone)]
pub struct FetchClient {
    inner: FlightServiceClient<Channel>,
    topic_name: TopicName,
    partition_field: Option<Field>,
    partition_value: Option<PartitionValue>,
    current_offset: u64,
    min_batch_size: usize,
    max_batch_size: usize,
    timeout: Duration,
}

impl FetchClient {
    pub(crate) fn new(client: &WingsClient, topic: Topic) -> Self {
        let partition_field = topic.partition_field().cloned();
        let topic_name = topic.name;
        let inner = client.flight.clone();

        Self {
            inner,
            topic_name,
            partition_field,
            partition_value: None,
            current_offset: 0,
            min_batch_size: 1,
            max_batch_size: 1000,
            timeout: Duration::from_millis(250),
        }
    }

    pub fn partition_data_type(&self) -> Option<DataType> {
        self.partition_field
            .as_ref()
            .map(|field| field.data_type.clone())
    }

    pub fn with_offset(mut self, offset: u64) -> Self {
        self.current_offset = offset;
        self
    }

    pub fn with_partition(mut self, partition: Option<PartitionValue>) -> Self {
        self.partition_value = partition;
        self
    }

    pub fn with_min_batch_size(mut self, min_batch_size: usize) -> Self {
        self.min_batch_size = min_batch_size;
        self
    }

    pub fn with_max_batch_size(mut self, max_batch_size: usize) -> Self {
        self.max_batch_size = max_batch_size;
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub async fn fetch_next(&mut self) -> Result<Vec<RecordBatch>> {
        debug!(topic = ?self.topic_name, "fetching data from flight do_get endpoint");

        if self.partition_field.is_some() && self.partition_value.is_none() {
            return MissingPartitionValueSnafu {}.fail();
        }

        let ticket = FetchTicket::new(self.topic_name.clone())
            .with_partition_value(self.partition_value.clone())
            .with_offset(self.current_offset)
            .with_min_batch_size(self.min_batch_size)
            .with_max_batch_size(self.max_batch_size)
            .with_timeout(self.timeout)
            .into_ticket()
            .context(TicketEncodeSnafu {})?;

        let request = new_request_for_namespace(&self.topic_name.parent, ticket);
        let response = self.inner.do_get(request).await.context(TonicSnafu {})?;

        let decoded = FlightDataDecoder::new(response.into_inner().map_err(FlightError::from));
        let batches = decoded
            .filter_map(|data| {
                use arrow_flight::decode::DecodedPayload;
                let data = match data {
                    Ok(data) => data,
                    Err(err) => return Some(Err(err)),
                };

                let DecodedPayload::RecordBatch(batch) = data.payload else {
                    return None;
                };

                Some(Ok(batch))
            })
            .try_collect::<Vec<_>>()
            .await
            .context(FlightSnafu {})?;

        let num_rows: u64 = batches.iter().map(|batch| batch.num_rows() as u64).sum();
        self.current_offset += num_rows;

        Ok(batches)
    }
}
