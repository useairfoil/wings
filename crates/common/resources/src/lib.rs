mod data_lake;
pub mod name;
mod namespace;
mod object_store;
mod partition;
mod table;
mod tenant;

pub use self::{
    data_lake::{
        DataLake, DataLakeConfiguration, DataLakeName, DataLakeRef, DeltaConfiguration,
        IcebergConfiguration, ParquetConfiguration,
    },
    name::{ResourceError, ResourceResult, validate_resource_id},
    namespace::{Namespace, NamespaceName, NamespaceOptions, NamespaceRef},
    object_store::{
        AwsConfiguration, AzureConfiguration, GoogleConfiguration, ObjectStore,
        ObjectStoreConfiguration, ObjectStoreName, ObjectStoreRef, S3CompatibleConfiguration,
    },
    partition::{PartitionValue, PartitionValueError, PartitionValueParseError},
    table::{
        CompactionConfiguration, PartitionPosition, SEQNUM_COLUMN_NAME, TIMESTAMP_COLUMN_NAME,
        Table, TableCondition, TableName, TableOptions, TableRef, TableStatus, validate_compaction,
    },
    tenant::{Tenant, TenantName, TenantRef},
};
