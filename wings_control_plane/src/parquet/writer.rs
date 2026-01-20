use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use bytesize::ByteSize;
use parquet::{arrow::ArrowWriter, errors::ParquetError, file::properties::WriterProperties};
use snafu::Snafu;

use crate::error_kind::ErrorKind;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(transparent)]
    Parquet { source: ParquetError },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

const DEFAULT_BUFFER_CAPACITY: usize = 32 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct FileMetadata {
    pub file_size: bytesize::ByteSize,
    pub num_rows: usize,
}

pub struct ParquetWriter {
    schema: SchemaRef,
    writer_properties: WriterProperties,
    inner_writer: Option<ArrowWriter<Vec<u8>>>,
}

impl ParquetWriter {
    pub fn new(schema: SchemaRef, writer_properties: WriterProperties) -> Self {
        Self {
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
                self.schema.clone(),
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

            let file_metadata = FileMetadata {
                file_size,
                num_rows: metadata.file_metadata().num_rows() as usize,
            };

            Ok((data, file_metadata))
        } else {
            // No data was written, return empty result
            Ok((
                Vec::new(),
                FileMetadata {
                    file_size: bytesize::ByteSize(0),
                    num_rows: 0,
                },
            ))
        }
    }

    pub fn current_file_size(&self) -> u64 {
        self.inner_writer
            .as_ref()
            .map(|w| w.in_progress_size() + w.bytes_written())
            .unwrap_or_default() as _
    }
}

impl Error {
    pub fn kind(&self) -> ErrorKind {
        match self {
            Self::Parquet { .. } => ErrorKind::Internal,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn create_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    fn create_test_batch() -> RecordBatch {
        let id_array = Int32Array::from(vec![1, 2, 3]);
        let name_array = StringArray::from(vec!["alice", "bob", "charlie"]);

        RecordBatch::try_new(
            create_test_schema(),
            vec![Arc::new(id_array), Arc::new(name_array)],
        )
        .unwrap()
    }

    #[test]
    fn test_parquet_writer_write_and_flush() {
        let schema = create_test_schema();
        let writer_properties = WriterProperties::builder().build();
        let mut writer = ParquetWriter::new(schema, writer_properties);

        let batch = create_test_batch();
        writer.write(&batch).unwrap();

        let (data, metadata) = writer.finish().unwrap();

        assert!(!data.is_empty());
        assert_eq!(metadata.num_rows, 3);
        assert!(metadata.file_size.as_u64() > 0);
    }

    #[test]
    fn test_current_file_size() {
        let schema = create_test_schema();
        let writer_properties = WriterProperties::builder().build();
        let mut writer = ParquetWriter::new(schema, writer_properties);

        let initial_size = writer.current_file_size();
        assert_eq!(initial_size, 0);

        let batch = create_test_batch();
        writer.write(&batch).unwrap();

        let size_after_write = writer.current_file_size();
        assert!(size_after_write > 0);
    }

    #[test]
    fn test_flush_resets_writer() {
        let schema = create_test_schema();
        let writer_properties = WriterProperties::builder().build();
        let mut writer = ParquetWriter::new(schema, writer_properties);

        let batch = create_test_batch();
        writer.write(&batch).unwrap();

        let (data1, metadata1) = writer.finish().unwrap();
        assert!(!data1.is_empty());
        assert_eq!(metadata1.num_rows, 3);

        // Write another batch to the reset writer
        writer.write(&batch).unwrap();
        let (data2, metadata2) = writer.finish().unwrap();
        assert!(!data2.is_empty());
        assert_eq!(metadata2.num_rows, 3);
    }
}
