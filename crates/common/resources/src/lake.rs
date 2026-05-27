use serde::{Deserialize, Serialize};
use tracing::warn;

/// Data lake configuration.
///
/// Different data lake types require different configurations.
/// This enum represents the various supported data lake types.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Lake {
    /// Parquet data lake configuration.
    Parquet(ParquetConfiguration),
    /// Iceberg data lake configuration.
    Iceberg(IcebergConfiguration),
    /// Delta data lake configuration.
    Delta(DeltaConfiguration),
}

/// Parquet data lake configuration.
#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParquetConfiguration {}

/// Iceberg data lake configuration.
#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcebergConfiguration {}

/// DeltaLake data lake configuration.
#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeltaConfiguration {}

impl Lake {
    pub fn into_redacted(self) -> Self {
        warn!("lake redaction is not implemented yet");
        self
    }
}
