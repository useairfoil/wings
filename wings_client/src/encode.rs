//! Wings ingestion encoder
//!
//! This encoder is modeled after the [`arrow_flight::encode::FlightDataEncoder`] struct
//! but adapted for ingesting wings data.
//!
//! **Wire protocol**
//!
//! The stream must start with a `Schema` message. This message's metadata must have
//! `request_id = 0` and contain the topic name. The schema in the message must
//! be compatible with the topic schema.
//!
//! All other messages must have `request_id > 0`. The metadata must contain the partition
//! value and timestamp, if any.
//! Notice that the user may push batches that are larger than the maximum message size,
//! for this reason, the encoder will split the batch into smaller batches.
//! These batches must have the same `request_id` and metadata, the server uses the
//! metadata to recompose the original batch.
//!
//! This code is a modified version of the [original arrow flight encoder](https://github.com/apache/arrow-rs/blob/main/arrow-flight/src/encode.rs).
//! All copyright to the original authors is reserved.

use arrow::{
    array::RecordBatch,
    datatypes::{Schema, SchemaRef},
    ipc::writer::{CompressionContext, DictionaryTracker, IpcDataGenerator, IpcWriteOptions},
};
use arrow_flight::{FlightData, FlightDescriptor, SchemaAsIpc, flight_descriptor::DescriptorType};
use snafu::ResultExt;
use wings_flight::IngestionRequestMetadata;
use wings_resources::TopicName;

use crate::{
    WriteRequest,
    error::{ArrowSnafu, Result},
};

pub struct IngestionFlightDataEncoder {
    inner: FlightIpcEncoder,
}

impl IngestionFlightDataEncoder {
    pub fn new() -> Self {
        Self {
            inner: FlightIpcEncoder::new(IpcWriteOptions::default(), true),
        }
    }

    pub fn encode_schema(&mut self, topic_name: &TopicName, schema: SchemaRef) -> FlightData {
        self.inner
            .encode_schema(&schema)
            .with_descriptor(FlightDescriptor {
                r#type: DescriptorType::Path as _,
                path: vec![topic_name.to_string()],
                ..Default::default()
            })
    }

    pub fn encode(&mut self, request_id: u64, request: WriteRequest) -> Result<Vec<FlightData>> {
        let metadata =
            IngestionRequestMetadata::new(request_id, request.partition_value, request.timestamp);
        let (dictionaries, data) = self.inner.encode_batch(&request.data)?;
        assert!(
            dictionaries.is_empty(),
            "dictionary support not implemented yet"
        );
        let flight_data = data.with_app_metadata(metadata.encode());
        Ok(vec![flight_data])
    }
}

struct FlightIpcEncoder {
    options: IpcWriteOptions,
    data_gen: IpcDataGenerator,
    dictionary_tracker: DictionaryTracker,
    compression_context: CompressionContext,
}

impl FlightIpcEncoder {
    fn new(options: IpcWriteOptions, error_on_replacement: bool) -> Self {
        Self {
            options,
            data_gen: IpcDataGenerator::default(),
            dictionary_tracker: DictionaryTracker::new(error_on_replacement),
            compression_context: CompressionContext::default(),
        }
    }

    /// Encode a schema as a FlightData
    fn encode_schema(&self, schema: &Schema) -> FlightData {
        SchemaAsIpc::new(schema, &self.options).into()
    }

    /// Convert a `RecordBatch` to a Vec of `FlightData` representing
    /// dictionaries and a `FlightData` representing the batch
    fn encode_batch(&mut self, batch: &RecordBatch) -> Result<(Vec<FlightData>, FlightData)> {
        let (encoded_dictionaries, encoded_batch) = self
            .data_gen
            .encode(
                batch,
                &mut self.dictionary_tracker,
                &self.options,
                &mut self.compression_context,
            )
            .context(ArrowSnafu {})?;

        let flight_dictionaries = encoded_dictionaries.into_iter().map(Into::into).collect();
        let flight_batch = encoded_batch.into();

        Ok((flight_dictionaries, flight_batch))
    }
}
