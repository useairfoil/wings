use std::{
    any::Any,
    collections::VecDeque,
    fmt,
    pin::Pin,
    sync::Arc,
    task::{self, Poll},
    time::SystemTime,
};

use datafusion::{
    catalog::{Session, memory::DataSourceExec},
    common::arrow::{
        array::{ArrayBuilder, RecordBatch, TimestampMicrosecondBuilder, UInt64Builder},
        datatypes::{FieldRef, SchemaRef},
    },
    datasource::physical_plan::{FileScanConfigBuilder, ParquetSource},
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext, object_store::ObjectStoreUrl},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
    },
    scalar::ScalarValue,
};
use futures::{Stream, StreamExt};
use tracing::trace;
use wings_control_plane_core::log_metadata::{CommittedBatch, FolioLocation};
use wings_resources::PartitionValue;
use wings_schema::Field;

use crate::folio_reader::FolioParquetFileReaderFactory;

pub struct FolioExec {
    partition_value_column: Option<(FieldRef, PartitionValue)>,
    location: FolioLocation,
    inner: Arc<dyn ExecutionPlan>,
    output_schema: SchemaRef,
    properties: PlanProperties,
}

struct FolioRecordBatchStream {
    inner: SendableRecordBatchStream,
    partition_value_column: Option<(FieldRef, PartitionValue)>,
    batches: VecDeque<CommittedBatch>,
    output_schema: SchemaRef,
}

impl FolioExec {
    pub fn try_new_exec(
        state: &dyn Session,
        output_schema: SchemaRef,
        file_schema: SchemaRef,
        partition_value: Option<PartitionValue>,
        partition_column: Option<Field>,
        location: FolioLocation,
        object_store_url: ObjectStoreUrl,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let properties = Self::compute_properties(&output_schema);

        let partition_value_column = match (partition_column, partition_value) {
            (None, None) => None,
            (Some(column), Some(value)) => {
                let column: Arc<_> = column.to_arrow_field().into();
                Some((column, value))
            }
            _ => {
                return Err(DataFusionError::Internal(
                    "Invalid partitioning".to_string(),
                ));
            }
        };

        let object_store = state.runtime_env().object_store(&object_store_url)?;
        let folio_reader_factory: Arc<_> =
            FolioParquetFileReaderFactory::new(object_store, location.clone()).into();
        let partitioned_file = folio_reader_factory.partitioned_file();

        let file_source: Arc<_> = ParquetSource::new(file_schema)
            .with_parquet_file_reader_factory(folio_reader_factory)
            .into();
        let config = FileScanConfigBuilder::new(object_store_url, file_source)
            .with_file(partitioned_file)
            .build();
        let inner = DataSourceExec::from_data_source(config);

        let exec = FolioExec {
            partition_value_column,
            location,
            inner,
            output_schema,
            properties,
        };

        Ok(Arc::new(exec))
    }

    fn compute_properties(schema: &SchemaRef) -> PlanProperties {
        let eq_properties = EquivalenceProperties::new(schema.clone());
        let partitioning = Partitioning::UnknownPartitioning(1);
        let emission_type = EmissionType::Incremental;
        let boundedness = Boundedness::Bounded;

        PlanProperties::new(eq_properties, partitioning, emission_type, boundedness)
    }
}

impl ExecutionPlan for FolioExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let inner = self.inner.execute(partition, context)?;
        let stream = FolioRecordBatchStream {
            inner,
            partition_value_column: self.partition_value_column.clone(),
            batches: self.location.batches.clone().into(),
            output_schema: self.output_schema.clone(),
        };

        let stream_with_partition_and_offset =
            RecordBatchStreamAdapter::new(self.output_schema.clone(), stream);
        Ok(Box::pin(stream_with_partition_and_offset))
    }
}

impl FolioRecordBatchStream {
    fn gen_next_batch(&mut self, data: RecordBatch) -> Result<RecordBatch, DataFusionError> {
        let (_, mut columns, num_rows) = data.into_parts();

        if let Some((_field, value)) = self.partition_value_column.as_ref() {
            let scalar_value: ScalarValue = value.clone().into();
            let array = scalar_value.to_array_of_size(num_rows)?;
            columns.push(array);
        }

        let mut offset_arr = UInt64Builder::new();
        let mut timestamp_arr = TimestampMicrosecondBuilder::new();

        let mut rows_to_fill = num_rows as u32;
        while rows_to_fill > 0 {
            let Some(current_batch) = self.batches.front_mut() else {
                return Err(DataFusionError::Internal(
                    "Metadata batch rows do not match file rows".to_string(),
                ));
            };

            match current_batch {
                CommittedBatch::Rejected(info) => {
                    if info.num_messages > rows_to_fill {
                        offset_arr.append_nulls(rows_to_fill as _);
                        timestamp_arr.append_nulls(rows_to_fill as _);

                        info.num_messages -= rows_to_fill;
                        rows_to_fill = 0;
                    } else {
                        offset_arr.append_nulls(info.num_messages as _);
                        timestamp_arr.append_nulls(info.num_messages as _);

                        rows_to_fill -= info.num_messages;
                        self.batches.pop_front();
                    }
                }
                CommittedBatch::Accepted(info) => {
                    let num_messages = info.num_messages();
                    let ts_micros = info
                        .timestamp
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .expect("timestamp")
                        .as_micros();

                    if num_messages > rows_to_fill {
                        let end_offset = info.start_offset + rows_to_fill as u64 - 1;

                        offset_arr.extend((info.start_offset..=end_offset).map(Some));
                        timestamp_arr.append_value_n(ts_micros as _, rows_to_fill as _);

                        info.start_offset = end_offset + 1;
                        rows_to_fill = 0;
                    } else {
                        offset_arr.extend((info.start_offset..=info.end_offset).map(Some));
                        timestamp_arr.append_value_n(ts_micros as _, num_messages as _);

                        rows_to_fill -= num_messages;
                        self.batches.pop_front();
                    }
                }
            }
        }

        trace!(
            data_num_rows = num_rows,
            offset_num_rows = offset_arr.len(),
            timestamp_num_rows = timestamp_arr.len(),
            "FolioExec::execute add batch"
        );

        columns.push(Arc::new(offset_arr.finish()));
        columns.push(Arc::new(timestamp_arr.finish()));

        let output = RecordBatch::try_new(self.output_schema.clone(), columns)?;
        Ok(output)
    }
}

impl Stream for FolioRecordBatchStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => Poll::Ready(Some(self.gen_next_batch(batch))),
            other => other,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl DisplayAs for FolioExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        let file_start = self.location.offset_bytes;
        let file_end = file_start + self.location.size_bytes;
        write!(
            f,
            "FolioExec: location=[{}[{}..{}]] start_offset=[{:?}], end_offset=[{:?}]",
            &self.location.file_ref,
            file_start,
            file_end,
            self.location.start_offset(),
            self.location.end_offset()
        )
    }
}

impl fmt::Debug for FolioExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FolioExec")
            .field("location", &self.location)
            .finish()
    }
}
