use std::sync::Arc;

use serde::{Deserialize, Serialize};

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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataLakeConfiguration {
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
pub struct DeltaConfiguration {
    /// The object store where to store the data lake data.
    ///
    /// If not specified, use the default object store for the namespace.
    pub object_store: Option<ObjectStoreName>,
}

pub type DataLakeRef = Arc<DataLake>;

impl DataLake {
    pub fn new(name: DataLakeName, data_lake: DataLakeConfiguration) -> Self {
        Self { name, data_lake }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_lake_name_creation() {
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let data_lake_name = DataLakeName::new("test-lake", tenant_name.clone()).unwrap();

        assert_eq!(data_lake_name.id(), "test-lake");
        assert_eq!(data_lake_name.parent(), &tenant_name);
        assert_eq!(
            data_lake_name.name(),
            "tenants/test-tenant/data-lakes/test-lake"
        );
        assert_eq!(
            data_lake_name.to_string(),
            "tenants/test-tenant/data-lakes/test-lake"
        );
    }

    #[test]
    fn test_data_lake_name_parse() {
        let data_lake_name =
            DataLakeName::parse("tenants/test-tenant/data-lakes/test-lake").unwrap();
        assert_eq!(data_lake_name.id(), "test-lake");

        // Test parse with invalid format
        let result = DataLakeName::parse("invalid-format");
        assert!(result.is_err());

        // Test parse with missing parent
        let result = DataLakeName::parse("data-lakes/test-lake");
        assert!(result.is_err());
    }

    #[test]
    fn test_data_lake_name_from_str() {
        let data_lake_name: DataLakeName =
            "tenants/test-tenant/data-lakes/test-lake".parse().unwrap();
        assert_eq!(data_lake_name.id(), "test-lake");

        let result: Result<DataLakeName, _> = "invalid".parse();
        assert!(result.is_err());
    }

    #[test]
    fn test_data_lake_name_new_unchecked() {
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let data_lake_name = DataLakeName::new_unchecked("test-lake", tenant_name);
        assert_eq!(data_lake_name.id(), "test-lake");
    }
}
