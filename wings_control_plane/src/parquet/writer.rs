use std::sync::Arc;

use arrow::{datatypes::Schema as ArrowSchema, record_batch::RecordBatch};
use bytesize::ByteSize;
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};

use crate::{
    parquet::stats::{FileMetadata, parquet_metadata_to_file_metadata},
    schema::SchemaRef,
};

use super::error::Result;

const DEFAULT_BUFFER_CAPACITY: usize = 32 * 1024 * 1024;

pub struct ParquetWriter {
    schema: SchemaRef,
    arrow_schema: Arc<ArrowSchema>,
    writer_properties: WriterProperties,
    inner_writer: Option<ArrowWriter<Vec<u8>>>,
}

impl ParquetWriter {
    pub fn new(schema: SchemaRef, writer_properties: WriterProperties) -> Self {
        Self {
            arrow_schema: Arc::new(schema.arrow_schema()),
            schema,
            writer_properties,
            inner_writer: None,
        }
    }

    fn ensure_writer(&mut self) -> Result<&mut ArrowWriter<Vec<u8>>> {
        if self.inner_writer.is_none() {
            let buffer = Vec::with_capacity(DEFAULT_BUFFER_CAPACITY);
            let writer = ArrowWriter::try_new(
                buffer,
                self.arrow_schema.clone(),
                self.writer_properties.clone().into(),
            )?;

            self.inner_writer = Some(writer);
        }

        Ok(self.inner_writer.as_mut().expect("inner parquet writer"))
    }

    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        let writer = self.ensure_writer()?;
        writer.write(batch)?;

        Ok(())
    }

    pub fn finish(&mut self) -> Result<(Vec<u8>, FileMetadata)> {
        if let Some(mut writer) = self.inner_writer.take() {
            let metadata = writer.finish()?;
            let data = std::mem::take(&mut *writer.inner_mut());
            let file_size = ByteSize::b(data.len() as _);

            let file_metadata =
                parquet_metadata_to_file_metadata(self.schema.clone(), file_size, metadata)?;

            Ok((data, file_metadata))
        } else {
            // No data was written, return empty result
            Ok((Vec::new(), FileMetadata::default()))
        }
    }

    pub fn current_file_size(&self) -> u64 {
        self.inner_writer
            .as_ref()
            .map(|w| w.in_progress_size() + w.bytes_written())
            .unwrap_or_default() as _
    }
}
