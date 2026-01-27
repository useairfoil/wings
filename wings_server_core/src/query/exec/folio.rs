use std::{any::Any, fmt, sync::Arc, time::SystemTime};

use datafusion::{
    catalog::{Session, memory::DataSourceExec},
    common::arrow::{
        array::{RecordBatch, TimestampMillisecondBuilder, UInt64Builder},
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
use futures::StreamExt;
use wings_control_plane::{
    log_metadata::{CommittedBatch, FolioLocation},
    resources::{OFFSET_COLUMN_NAME, PartitionValue, TIMESTAMP_COLUMN_NAME},
    schema::Field,
};

use crate::folio_reader::FolioParquetFileReaderFactory;

pub struct FolioExec {
    partition_value_column: Option<(FieldRef, PartitionValue)>,
    location: FolioLocation,
    inner: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl FolioExec {
    pub fn try_new_exec(
        state: &dyn Session,
        schema: SchemaRef,
        file_schema: SchemaRef,
        partition_value: Option<PartitionValue>,
        partition_column: Option<Field>,
        location: FolioLocation,
        object_store_url: ObjectStoreUrl,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let properties = Self::compute_properties(&schema);

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
            schema,
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
        self.schema.clone()
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let stream = self.inner.execute(partition, context)?;
        let stream_with_partition_and_offset = RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream.map({
                let partition_value_column = self.partition_value_column.clone();
                let location = self.location.clone();
                let output_schema = self.schema.clone();
                move |batch| {
                    let batch = batch?;
                    let mut columns = Vec::with_capacity(batch.num_columns() + 2);
                    let mut partition_col_offset = 0;

                    for (col_index, output_col) in output_schema.fields().iter().enumerate() {
                        if output_col.name() == OFFSET_COLUMN_NAME
                            || output_col.name() == TIMESTAMP_COLUMN_NAME
                        {
                            continue;
                        }

                        if let Some((column, value)) = partition_value_column.clone() {
                            if column.name() == output_col.name() {
                                let scalar_value: ScalarValue = value.into();
                                let array = scalar_value.to_array_of_size(batch.num_rows())?;
                                columns.push(array);
                                partition_col_offset = 1;
                            } else {
                                columns
                                    .push(batch.column(col_index - partition_col_offset).clone());
                            }
                        } else {
                            columns.push(batch.column(col_index - partition_col_offset).clone());
                        }
                    }

                    let mut offset_arr = UInt64Builder::new();
                    let mut timestamp_arr = TimestampMillisecondBuilder::new();
                    for batch in location.batches.iter() {
                        match batch {
                            CommittedBatch::Rejected(info) => {
                                offset_arr.append_nulls(info.num_messages as _);
                                timestamp_arr.append_nulls(info.num_messages as _);
                            }
                            CommittedBatch::Accepted(info) => {
                                let ts_millis = info
                                    .timestamp
                                    .duration_since(SystemTime::UNIX_EPOCH)
                                    .expect("timestamp")
                                    .as_millis();
                                offset_arr.extend((info.start_offset..=info.end_offset).map(Some));
                                timestamp_arr
                                    .append_value_n(ts_millis as _, info.num_messages() as _);
                            }
                        }
                    }

                    columns.push(Arc::new(offset_arr.finish()));
                    columns.push(Arc::new(timestamp_arr.finish()));

                    let output = RecordBatch::try_new(output_schema.clone(), columns)?;
                    Ok(output)
                }
            }),
        );

        Ok(Box::pin(stream_with_partition_and_offset))
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
