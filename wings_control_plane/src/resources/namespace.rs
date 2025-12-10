use std::{sync::Arc, time::Duration};

use bytesize::ByteSize;

use crate::{
    resource_type,
    resources::{DataLakeName, ObjectStoreName, TenantName},
};

resource_type!(Namespace, "namespaces", Tenant);

/// A namespace belonging to a tenant.
///
/// A namespace is used to group related topics together.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Namespace {
    /// The namespace name.
    pub name: NamespaceName,
    /// The size at which the current segment is flushed to object storage.
    pub flush_size: ByteSize,
    /// The maximum interval at which the current segment is flushed to object storage.
    pub flush_interval: Duration,
    /// The object store configuration for the namespace.
    pub object_store: ObjectStoreName,
    /// DataLake configuration.
    pub data_lake: DataLakeName,
}

pub type NamespaceRef = Arc<Namespace>;

impl Namespace {
    /// Create a new namespace with the given name and options.
    pub fn new(name: NamespaceName, options: NamespaceOptions) -> Self {
        Self {
            name,
            flush_size: options.flush_size,
            flush_interval: options.flush_interval,
            object_store: options.object_store,
            data_lake: options.data_lake,
        }
    }
}

/// Options for creating a namespace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceOptions {
    /// The size at which the current segment is flushed to object storage.
    pub flush_size: ByteSize,
    /// The maximum interval at which the current segment is flushed to object storage.
    pub flush_interval: Duration,
    /// The object store configuration for the namespace.
    pub object_store: ObjectStoreName,
    /// DataLake configuration.
    pub data_lake: DataLakeName,
}

impl NamespaceOptions {
    /// Create new namespace options with the given object store and data lake configurations.
    pub fn new(object_store: ObjectStoreName, data_lake: DataLakeName) -> Self {
        Self {
            flush_size: ByteSize::mb(8),
            flush_interval: Duration::from_millis(250),
            object_store,
            data_lake,
        }
    }

    /// Change the flush size for the namespace.
    pub fn with_flush_size(mut self, flush_size: ByteSize) -> Self {
        self.flush_size = flush_size;
        self
    }

    /// Change the flush interval for the namespace.
    pub fn with_flush_interval(mut self, flush_interval: Duration) -> Self {
        self.flush_interval = flush_interval;
        self
    }

    /// Change the default object store configuration for the namespace.
    pub fn with_default_object_store_config(
        mut self,
        default_object_store: ObjectStoreName,
    ) -> Self {
        self.object_store = default_object_store;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::{ObjectStoreName, TenantName};

    #[test]
    fn test_namespace_creation() {
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name.clone()).unwrap();
        let options = NamespaceOptions::new(
            ObjectStoreName::new_unchecked("test-config", tenant_name.clone()),
            DataLakeName::new_unchecked("test-lake", tenant_name.clone()),
        );
        let namespace = Namespace::new(namespace_name.clone(), options.clone());

        assert_eq!(namespace.name, namespace_name);
        assert_eq!(namespace.name.id(), "test-namespace");
        assert_eq!(namespace.name.parent(), &tenant_name);
        assert_eq!(
            namespace.name.name(),
            "tenants/test-tenant/namespaces/test-namespace"
        );
        assert_eq!(namespace.flush_size, options.flush_size);
        assert_eq!(namespace.flush_interval, options.flush_interval);
        assert_eq!(namespace.object_store, options.object_store);
        assert_eq!(namespace.data_lake, options.data_lake);
    }
}
