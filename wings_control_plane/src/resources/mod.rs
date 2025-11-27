mod credential;
pub(crate) mod name;
mod namespace;
mod partition;
mod tenant;
mod topic;

pub use self::credential::{
    AwsConfiguration, AzureConfiguration, Credential, CredentialName, CredentialRef,
    GoogleConfiguration, ObjectStoreConfiguration, S3CompatibleConfiguration,
};
pub use self::name::{ResourceError, ResourceResult, validate_resource_id};
pub use self::namespace::{
    DataLakeConfig, IcebergRestCatalogConfig, Namespace, NamespaceName, NamespaceOptions,
    NamespaceRef,
};
pub use self::partition::{PartitionValue, PartitionValueError, PartitionValueParseError};
pub use self::tenant::{Tenant, TenantName, TenantRef};
pub use self::topic::{Topic, TopicName, TopicOptions, TopicRef};
