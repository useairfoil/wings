pub mod name;
mod namespace;
mod partition;
mod table;

pub use self::{
    name::{ResourceError, ResourceResult, validate_resource_id},
    namespace::{Namespace, NamespaceName, NamespaceOptions, NamespaceRef},
    partition::{PartitionValue, PartitionValueError, PartitionValueParseError},
    table::{Table, TableName, TableOptions, TableRef},
};
