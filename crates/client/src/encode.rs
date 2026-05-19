//! Wings ingestion encoder
//!
//! This encoder is modeled after the [`arrow_flight::encode::FlightDataEncoder`] struct
//! but adapted for ingesting wings data.
//!
//! **Wire protocol**
//!
//! Each batch starts with a `Schema` message. This message's metadata contains
//! the batch id, table name, partition value, and timestamp. The schema in the
//! message must be compatible with the table schema.
//!
//! Notice that the user may push batches that are larger than the maximum message size,
//! for this reason, the encoder will split the batch into smaller batches.
//! These batches belong to the latest schema message, and the server uses the
//! schema metadata to recompose the original batch.
//!
//! This code is a modified version of the [original arrow flight encoder](https://github.com/apache/arrow-rs/blob/main/arrow-flight/src/encode.rs).
//! All copyright to the original authors is reserved.

use arrow::{
    array::RecordBatch,
    datatypes::Schema,
    ipc::writer::{CompressionContext, DictionaryTracker, IpcDataGenerator, IpcWriteOptions},
};
use arrow_flight::{FlightData, FlightDescriptor, SchemaAsIpc, flight_descriptor::DescriptorType};
use snafu::ResultExt;
use wings_flight::IngestionRequestMetadata;

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

    pub fn encode_commit(&self) -> FlightData {
        self.inner.encode_schema(&Schema::empty())
    }

    fn encode_schema(&mut self, batch_id: u32, request: &WriteRequest) -> FlightData {
        let schema = request.data.schema();
        let metadata = IngestionRequestMetadata::new(
            batch_id,
            request.table_name.clone(),
            request.partition_value.clone(),
            request.timestamp,
        );

        self.inner
            .encode_schema(schema.as_ref())
            .with_descriptor(FlightDescriptor {
                r#type: DescriptorType::Path as _,
                path: vec![request.table_name.to_string()],
                ..Default::default()
            })
            .with_app_metadata(metadata.encode())
    }

    pub fn encode(&mut self, batch_id: u32, request: WriteRequest) -> Result<Vec<FlightData>> {
        let schema = self.encode_schema(batch_id, &request);
        let (dictionaries, data) = self.inner.encode_batch(&request.data)?;
        assert!(
            dictionaries.is_empty(),
            "dictionary support not implemented yet"
        );
        Ok(vec![schema, data])
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
