pub(crate) mod name;
mod namespace;
mod object_store;
mod partition;
mod tenant;
mod topic;

pub use self::name::{ResourceError, ResourceResult, validate_resource_id};
pub use self::namespace::{
    DataLakeConfig, IcebergRestCatalogConfig, Namespace, NamespaceName, NamespaceOptions,
    NamespaceRef,
};
pub use self::object_store::{
    AwsConfiguration, AzureConfiguration, GoogleConfiguration, ObjectStore,
    ObjectStoreConfiguration, ObjectStoreName, ObjectStoreRef, S3CompatibleConfiguration,
};
pub use self::partition::{PartitionValue, PartitionValueError, PartitionValueParseError};
pub use self::tenant::{Tenant, TenantName, TenantRef};
pub use self::topic::{Topic, TopicName, TopicOptions, TopicRef};
