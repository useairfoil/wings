use std::{ops::Range, sync::Arc};

use datafusion::{
    common::Result,
    datasource::{
        listing::PartitionedFile,
        physical_plan::{ParquetFileMetrics, ParquetFileReaderFactory},
    },
    error::DataFusionError,
    physical_plan::metrics::ExecutionPlanMetricsSet,
};
use futures::future::BoxFuture;
use object_store::ObjectStore;
use parquet::{
    arrow::{
        arrow_reader::ArrowReaderOptions,
        async_reader::{AsyncFileReader, ParquetObjectReader},
    },
    file::metadata::{ParquetMetaData, ParquetMetaDataReader},
};
use tokio_util::bytes::Bytes;
use wings_control_plane::log_metadata::FolioLocation;

#[derive(Debug)]
pub struct FolioParquetFileReaderFactory {
    store: Arc<dyn ObjectStore>,
    location: FolioLocation,
}

#[derive(Debug)]
pub struct FolioParquetFileReader {
    offset_bytes: u64,
    size_bytes: u64,
    file_metrics: ParquetFileMetrics,
    inner: ParquetObjectReader,
}

impl FolioParquetFileReaderFactory {
    pub fn new(store: Arc<dyn ObjectStore>, location: FolioLocation) -> Self {
        FolioParquetFileReaderFactory { store, location }
    }

    pub fn partitioned_file(&self) -> PartitionedFile {
        PartitionedFile::new(&self.location.file_ref, self.location.size_bytes)
    }
}

impl ParquetFileReaderFactory for FolioParquetFileReaderFactory {
    fn create_reader(
        &self,
        partition_index: usize,
        partitioned_file: PartitionedFile,
        _metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Box<dyn AsyncFileReader + Send>> {
        let file_location = partitioned_file.object_meta.location;
        if file_location != self.location.path() {
            return Err(DataFusionError::Internal(
                "reader file_meta and folio location mismatch".to_string(),
            ));
        }

        let file_metrics =
            ParquetFileMetrics::new(partition_index, file_location.as_ref(), metrics);

        let store = Arc::clone(&self.store);
        let inner = ParquetObjectReader::new(store, self.location.path().clone())
            .with_file_size(self.location.size_bytes);

        Ok(Box::new(FolioParquetFileReader {
            offset_bytes: self.location.offset_bytes,
            size_bytes: self.location.size_bytes,
            file_metrics,
            inner,
        }))
    }
}

impl AsyncFileReader for FolioParquetFileReader {
    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        let total: u64 = ranges.iter().map(|r| r.end - r.start).sum();
        self.file_metrics.bytes_scanned.add(total as usize);
        let ranges = ranges
            .iter()
            .map(|r| add_offset(r, self.offset_bytes))
            .collect();
        self.inner.get_byte_ranges(ranges)
    }

    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        let bytes_scanned = range.end - range.start;
        self.file_metrics.bytes_scanned.add(bytes_scanned as usize);
        self.inner.get_bytes(add_offset(&range, self.offset_bytes))
    }

    fn get_metadata<'a>(
        &'a mut self,
        _options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        // We cannot use the inner reader directly because it will read data at
        // the wrong location.
        // So we just inline the code from `ParquetObjectReader`.
        Box::pin(async move {
            let reader = ParquetMetaDataReader::new();

            let file_size = self.size_bytes;
            let metadata = reader.load_and_finish(self, file_size).await?;

            Ok(Arc::new(metadata))
        })
    }
}

fn add_offset(range: &Range<u64>, offset: u64) -> Range<u64> {
    (range.start + offset)..(range.end + offset)
}
