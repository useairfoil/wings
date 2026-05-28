use std::sync::Arc;

use crate::{DataLakeConfiguration, ObjectStoreConfiguration, resource_type};

resource_type!(Namespace, "namespaces");

/// A root namespace.
///
/// A namespace is used to group related tables together.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Namespace {
    /// The namespace name.
    pub name: NamespaceName,
    /// The object store configuration.
    pub object_store: ObjectStoreConfiguration,
    /// The data lake configuration.
    pub lake: DataLakeConfiguration,
}

pub type NamespaceRef = Arc<Namespace>;

/// Options for creating a namespace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceOptions {
    /// The object store configuration.
    pub object_store: ObjectStoreConfiguration,
    /// The data lake configuration.
    pub lake: DataLakeConfiguration,
}

impl Namespace {
    pub fn into_redacted(self) -> Self {
        Self {
            name: self.name,
            object_store: self.object_store.into_redacted(),
            lake: self.lake.into_redacted(),
        }
    }
}
