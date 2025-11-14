use arrow::datatypes::FieldRef;
use snafu::Snafu;

use crate::resources::{NamespaceName, PartitionValue};

pub fn format_folio_path(namespace: &NamespaceName, folio_id: &str) -> String {
    format!("{}/folio/{}.folio", namespace, folio_id)
}

#[derive(Default, Debug)]
pub struct ParquetPathBuilder {
    namespace: String,
    offset_range: Option<(u64, u64)>,
    partition_key: Option<String>,
    partition_value: Option<String>,
}

#[derive(Snafu, Debug)]
pub enum ParquetPathError {
    #[snafu(display("Missing offset range"))]
    MissingOffsetRange,
    #[snafu(display("Partition value is present, but partition key is missing"))]
    MissingPartitionKey,
    #[snafu(display("Partition key is present, but partition value is missing"))]
    MissingPartitionValue,
}

pub fn format_parquet_path(namespace: &NamespaceName) -> ParquetPathBuilder {
    ParquetPathBuilder {
        namespace: namespace.to_string(),
        ..Default::default()
    }
}

impl ParquetPathBuilder {
    pub fn with_offset_range(mut self, start: u64, end: u64) -> Self {
        self.offset_range = Some((start, end));
        self
    }

    pub fn with_partition(
        mut self,
        field: Option<&FieldRef>,
        value: Option<&PartitionValue>,
    ) -> Self {
        self.partition_key = field.map(|f| f.name().to_string());
        self.partition_value = value.map(|v| v.to_string());
        self
    }

    pub fn build(self) -> Result<String, ParquetPathError> {
        let Some((start, end)) = self.offset_range else {
            return Err(ParquetPathError::MissingOffsetRange);
        };

        let partition_segment = match (self.partition_key, self.partition_value) {
            (Some(key), Some(value)) => format!("{}={}/", key, value),
            (None, None) => String::new(),
            (Some(_), None) => return Err(ParquetPathError::MissingPartitionValue),
            (None, Some(_)) => return Err(ParquetPathError::MissingPartitionKey),
        };

        let output = format!(
            "{}/data/{}{:0>10}-{:0>10}.parquet",
            self.namespace, partition_segment, start, end
        );

        Ok(output)
    }
}
