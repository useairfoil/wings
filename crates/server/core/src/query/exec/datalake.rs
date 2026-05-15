use std::{any::Any, fmt, sync::Arc};

use datafusion::{
    catalog::{Session, memory::DataSourceExec},
    common::arrow::{array::RecordBatch, datatypes::SchemaRef},
    datasource::{
        listing::PartitionedFile,
        physical_plan::{
            FileScanConfigBuilder, ParquetSource, parquet::DefaultParquetFileReaderFactory,
        },
    },
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
use wings_control_plane_core::log_metadata::DataLakeLocation;
use wings_resources::PartitionValue;

/// Read the topic's content from a Parquet file in the datalake.
pub struct DataLakeExec {
    location: DataLakeLocation,
    partition_value: Option<PartitionValue>,
    inner: Arc<dyn ExecutionPlan>,
    output_schema: SchemaRef,
    properties: PlanProperties,
}

impl DataLakeExec {
    pub fn try_new_exec(
        state: &dyn Session,
        output_schema: SchemaRef,
        file_schema: SchemaRef,
        partition_value: Option<PartitionValue>,
        location: DataLakeLocation,
        object_store_url: ObjectStoreUrl,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let properties = Self::compute_properties(&output_schema);

        let object_store = state.runtime_env().object_store(&object_store_url)?;
        let parquet_reader = DefaultParquetFileReaderFactory::new(object_store);
        let partitioned_file = PartitionedFile::new(location.file_ref.clone(), location.size_bytes);

        let file_source: Arc<_> = ParquetSource::new(file_schema)
            .with_parquet_file_reader_factory(Arc::new(parquet_reader))
            .into();
        let config = FileScanConfigBuilder::new(object_store_url, file_source)
            .with_file(partitioned_file)
            .build();

        let inner = DataSourceExec::from_data_source(config);

        let exec = DataLakeExec {
            properties,
            partition_value,
            location,
            output_schema,
            inner,
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

impl ExecutionPlan for DataLakeExec {
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
        let output_schema = self.schema();
        let stream_with_partition = RecordBatchStreamAdapter::new(
            output_schema.clone(),
            inner.map({
                let output_schema = output_schema.clone();
                let partition_value = self.partition_value.clone();
                move |batch| {
                    let batch = batch?;
                    let (_, mut columns, num_rows) = batch.into_parts();

                    if let Some(value) = partition_value.as_ref() {
                        let scalar_value: ScalarValue = value.clone().into();
                        let array = scalar_value.to_array_of_size(num_rows)?;
                        // Put the partition column before the metadata columns.
                        // This is ugly.
                        columns.insert(columns.len() - 2, array);
                    }

                    let batch = RecordBatch::try_new(output_schema.clone(), columns)?;
                    Ok(batch)
                }
            }),
        );

        Ok(Box::pin(stream_with_partition))
    }
}

impl DisplayAs for DataLakeExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "DataLakeExec: location=[{}] start_offset=[{:?}], end_offset=[{:?}]",
            &self.location.file_ref, self.location.start_offset, self.location.end_offset
        )
    }
}

impl fmt::Debug for DataLakeExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataLakeExec")
            .field("location", &self.location)
            .finish()
    }
}
