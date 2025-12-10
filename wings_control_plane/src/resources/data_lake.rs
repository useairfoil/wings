use std::sync::Arc;

use crate::resource_type;

use super::tenant::TenantName;

resource_type!(DataLake, "data-lakes", Tenant);

/// Data lake configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataLake {
    pub name: DataLakeName,
    pub data_lake: DataLakeConfiguration,
}

/// Data lake configuration.
///
/// Different data lake types require different configurations.
/// This enum represents the various supported data lake types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataLakeConfiguration {
    /// Iceberg data lake configuration.
    Iceberg(IcebergConfiguration),
    /// Parquet data lake configuration.
    Parquet(ParquetConfiguration),
}

/// Iceberg data lake configuration.
#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct IcebergConfiguration {}

/// Parquet data lake configuration.
#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct ParquetConfiguration {}

pub type DataLakeRef = Arc<DataLake>;
