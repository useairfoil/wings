use std::sync::Arc;

use super::tenant::TenantName;
use crate::{ObjectStoreName, resource_type};

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
    /// Parquet data lake configuration.
    Parquet(ParquetConfiguration),
    /// Iceberg data lake configuration.
    Iceberg(IcebergConfiguration),
    /// Delta data lake configuration.
    Delta(DeltaConfiguration),
}

/// Parquet data lake configuration.
#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct ParquetConfiguration {}

/// Iceberg data lake configuration.
#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct IcebergConfiguration {}

/// DeltaLake data lake configuration.
#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct DeltaConfiguration {
    /// The object store where to store the data lake data.
    ///
    /// If not specified, use the default object store for the namespace.
    pub object_store: Option<ObjectStoreName>,
}

pub type DataLakeRef = Arc<DataLake>;
