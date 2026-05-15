use std::{
    sync::{Arc, Weak},
    time::Duration,
};

use opentelemetry_sdk::{
    error::OTelSdkResult,
    metrics::{
        data::ResourceMetrics, reader::MetricReader, InstrumentKind, ManualReader, Pipeline,
        Temporality,
    },
};

#[derive(Clone, Debug)]
pub struct MetricsExporter {
    reader: Arc<ManualReader>,
}

impl MetricReader for MetricsExporter {
    fn register_pipeline(&self, pipeline: Weak<Pipeline>) {
        self.reader.register_pipeline(pipeline);
    }

    fn collect(&self, rm: &mut ResourceMetrics) -> OTelSdkResult {
        self.reader.collect(rm)
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.reader.force_flush()
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.reader.shutdown_with_timeout(timeout)
    }

    fn temporality(&self, kind: InstrumentKind) -> Temporality {
        self.reader.temporality(kind)
    }
}

impl Default for MetricsExporter {
    fn default() -> Self {
        let reader = ManualReader::builder().build();
        Self {
            reader: Arc::new(reader),
        }
    }
}
