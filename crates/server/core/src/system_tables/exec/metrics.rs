use std::{any::Any, fmt, sync::Arc};

use datafusion::{
    common::arrow::{
        array::{ArrayRef, Float64Builder, MapBuilder, RecordBatch, StringBuilder},
        datatypes::{DataType, Field, Schema, SchemaRef},
        error::ArrowError,
    },
    error::Result,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
    },
};
use futures::stream;
use opentelemetry_sdk::metrics::data::{
    AggregatedMetrics, Gauge, Metric, MetricData, ResourceMetrics, ScopeMetrics, Sum,
};
use wings_observability::KeyValue;

pub struct MetricsExec {
    rm: ResourceMetrics,
    properties: PlanProperties,
}

struct DataBuilder {
    name: StringBuilder,
    value: Float64Builder,
    unit: StringBuilder,
    description: StringBuilder,
    attributes: MapBuilder<StringBuilder, StringBuilder>,
}

impl MetricsExec {
    pub fn new(rm: ResourceMetrics) -> Self {
        let schema = Self::schema();
        let properties = Self::compute_properties(&schema);

        Self { rm, properties }
    }

    pub fn schema() -> SchemaRef {
        let fields = vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, true),
            Field::new("unit", DataType::Utf8, false),
            Field::new("description", DataType::Utf8, false),
            Field::new("attributes", attributes_datatype(), true),
        ];

        Arc::new(Schema::new(fields))
    }

    fn compute_properties(schema: &SchemaRef) -> PlanProperties {
        let eq_properties = EquivalenceProperties::new(schema.clone());
        let partitioning = Partitioning::UnknownPartitioning(1);
        let emission_type = EmissionType::Incremental;
        let boundedness = Boundedness::Bounded;

        PlanProperties::new(eq_properties, partitioning, emission_type, boundedness)
    }

    fn data_for_scope_metric(&self, scope_metrics: &ScopeMetrics) -> Result<RecordBatch> {
        let mut builder = DataBuilder::default();
        for metric in scope_metrics.metrics() {
            self.add_metric(&mut builder, metric)?;
        }

        RecordBatch::try_new(Self::schema(), builder.finish()).map_err(Into::into)
    }

    fn add_metric(&self, builder: &mut DataBuilder, metric: &Metric) -> Result<()> {
        match metric.data() {
            AggregatedMetrics::U64(MetricData::Gauge(gauge)) => {
                self.add_gauge_metric(builder, metric, gauge)
            }
            AggregatedMetrics::I64(MetricData::Gauge(gauge)) => {
                self.add_gauge_metric(builder, metric, gauge)
            }
            AggregatedMetrics::F64(MetricData::Gauge(gauge)) => {
                self.add_gauge_metric(builder, metric, gauge)
            }
            AggregatedMetrics::U64(MetricData::Sum(sum)) => {
                self.add_sum_metric(builder, metric, sum)
            }
            AggregatedMetrics::I64(MetricData::Sum(sum)) => {
                self.add_sum_metric(builder, metric, sum)
            }
            AggregatedMetrics::F64(MetricData::Sum(sum)) => {
                self.add_sum_metric(builder, metric, sum)
            }
            AggregatedMetrics::U64(MetricData::Histogram(_hist)) => {
                todo!();
            }
            AggregatedMetrics::I64(MetricData::Histogram(_hist)) => {
                todo!();
            }
            AggregatedMetrics::F64(MetricData::Histogram(_hist)) => {
                todo!();
            }
            AggregatedMetrics::U64(MetricData::ExponentialHistogram(_)) => Ok(()),
            AggregatedMetrics::I64(MetricData::ExponentialHistogram(_)) => Ok(()),
            AggregatedMetrics::F64(MetricData::ExponentialHistogram(_)) => Ok(()),
        }
    }

    fn add_gauge_metric<T: DataPointValue>(
        &self,
        builder: &mut DataBuilder,
        metric: &Metric,
        gauge: &Gauge<T>,
    ) -> Result<()> {
        let name = metric.name();
        let unit = metric.unit();
        let description = metric.description();
        for dp in gauge.data_points() {
            let value = dp.value().to_f64();
            builder.add_row(name, value, unit, description, dp.attributes())?;
        }

        Ok(())
    }

    fn add_sum_metric<T: DataPointValue>(
        &self,
        builder: &mut DataBuilder,
        metric: &Metric,
        sum: &Sum<T>,
    ) -> Result<()> {
        let name = metric.name();
        let unit = metric.unit();
        let description = metric.description();

        for dp in sum.data_points() {
            let value = dp.value().to_f64();
            builder.add_row(name, value, unit, description, dp.attributes())?;
        }

        Ok(())
    }
}

impl ExecutionPlan for MetricsExec {
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
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn schema(&self) -> SchemaRef {
        Self::schema()
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let mut data = Vec::new();
        for scope_metric in self.rm.scope_metrics() {
            data.push(self.data_for_scope_metric(scope_metric));
        }

        let stream = RecordBatchStreamAdapter::new(self.schema(), stream::iter(data));

        Ok(Box::pin(stream))
    }
}

impl Default for DataBuilder {
    fn default() -> Self {
        let attributes = {
            let k = StringBuilder::new();
            let v = StringBuilder::new();
            MapBuilder::new(None, k, v)
        };

        Self {
            name: StringBuilder::new(),
            value: Float64Builder::new(),
            unit: StringBuilder::new(),
            description: StringBuilder::new(),
            attributes,
        }
    }
}

impl DataBuilder {
    pub fn add_row<'a>(
        &mut self,
        name: &str,
        value: f64,
        unit: &str,
        description: &str,
        attributes: impl Iterator<Item = &'a KeyValue>,
    ) -> Result<(), ArrowError> {
        self.name.append_value(name);
        self.value.append_value(value);
        self.unit.append_value(unit);
        self.description.append_value(description);

        for kv in attributes {
            self.attributes.keys().append_value(&kv.key);
            self.attributes.values().append_value(kv.value.to_string());
        }
        self.attributes.append(true)?;

        Ok(())
    }

    pub fn finish(mut self) -> Vec<ArrayRef> {
        vec![
            Arc::new(self.name.finish()),
            Arc::new(self.value.finish()),
            Arc::new(self.unit.finish()),
            Arc::new(self.description.finish()),
            Arc::new(self.attributes.finish()),
        ]
    }
}

trait DataPointValue: Copy {
    fn to_f64(&self) -> f64;
}

impl DataPointValue for f64 {
    fn to_f64(&self) -> f64 {
        *self
    }
}

impl DataPointValue for u64 {
    fn to_f64(&self) -> f64 {
        *self as f64
    }
}

impl DataPointValue for i64 {
    fn to_f64(&self) -> f64 {
        *self as f64
    }
}

impl DisplayAs for MetricsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Metrics")
    }
}

impl fmt::Debug for MetricsExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetricsExec").finish()
    }
}

fn attributes_datatype() -> DataType {
    let entries = Field::new(
        "entries",
        DataType::Struct(
            vec![
                Field::new("keys", DataType::Utf8, false),
                Field::new("values", DataType::Utf8, true),
            ]
            .into(),
        ),
        false,
    );

    DataType::Map(Arc::new(entries), false)
}
