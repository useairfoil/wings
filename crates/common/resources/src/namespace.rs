use std::{sync::Arc, time::Duration};

use bytesize::ByteSize;

use crate::{DataLakeName, ObjectStoreName, TenantName, resource_type};

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

    /// Change the object store configuration for the namespace.
    pub fn with_object_store(mut self, object_store: ObjectStoreName) -> Self {
        self.object_store = object_store;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ObjectStoreName, TenantName};

    #[test]
    fn test_namespace_name_creation() {
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name.clone()).unwrap();

        assert_eq!(namespace_name.id(), "test-namespace");
        assert_eq!(namespace_name.parent(), &tenant_name);
        assert_eq!(
            namespace_name.name(),
            "tenants/test-tenant/namespaces/test-namespace"
        );
        assert_eq!(
            namespace_name.to_string(),
            "tenants/test-tenant/namespaces/test-namespace"
        );
    }

    #[test]
    fn test_namespace_name_parse() {
        let namespace_name =
            NamespaceName::parse("tenants/test-tenant/namespaces/test-namespace").unwrap();
        assert_eq!(namespace_name.id(), "test-namespace");

        // Test parse with invalid format
        let result = NamespaceName::parse("invalid-format");
        assert!(result.is_err());

        // Test parse with missing parent
        let result = NamespaceName::parse("namespaces/test-namespace");
        assert!(result.is_err());
    }

    #[test]
    fn test_namespace_name_from_str() {
        let namespace_name: NamespaceName = "tenants/test-tenant/namespaces/test-namespace"
            .parse()
            .unwrap();
        assert_eq!(namespace_name.id(), "test-namespace");

        let result: Result<NamespaceName, _> = "invalid".parse();
        assert!(result.is_err());
    }

    #[test]
    fn test_namespace_name_new_unchecked() {
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new_unchecked("test-namespace", tenant_name);
        assert_eq!(namespace_name.id(), "test-namespace");
    }

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

    #[test]
    fn test_namespace_options_default() {
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let object_store = ObjectStoreName::new_unchecked("test-store", tenant_name.clone());
        let data_lake = DataLakeName::new_unchecked("test-lake", tenant_name);

        let options = NamespaceOptions::new(object_store, data_lake);

        // Verify default values
        assert_eq!(options.flush_size, ByteSize::mb(8));
        assert_eq!(options.flush_interval, Duration::from_millis(250));
    }

    #[test]
    fn test_namespace_options_with_flush_size() {
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let object_store = ObjectStoreName::new_unchecked("test-store", tenant_name.clone());
        let data_lake = DataLakeName::new_unchecked("test-lake", tenant_name);

        let options =
            NamespaceOptions::new(object_store, data_lake).with_flush_size(ByteSize::mb(16));

        assert_eq!(options.flush_size, ByteSize::mb(16));
    }

    #[test]
    fn test_namespace_options_with_flush_interval() {
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let object_store = ObjectStoreName::new_unchecked("test-store", tenant_name.clone());
        let data_lake = DataLakeName::new_unchecked("test-lake", tenant_name);

        let options = NamespaceOptions::new(object_store, data_lake)
            .with_flush_interval(Duration::from_secs(1));

        assert_eq!(options.flush_interval, Duration::from_secs(1));
    }

    #[test]
    fn test_namespace_options_with_object_store() {
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let object_store1 = ObjectStoreName::new_unchecked("store1", tenant_name.clone());
        let object_store2 = ObjectStoreName::new_unchecked("store2", tenant_name.clone());
        let data_lake = DataLakeName::new_unchecked("test-lake", tenant_name);

        let options = NamespaceOptions::new(object_store1, data_lake)
            .with_object_store(object_store2.clone());

        assert_eq!(options.object_store, object_store2);
    }

    #[test]
    fn test_namespace_options_chaining() {
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let object_store = ObjectStoreName::new_unchecked("test-store", tenant_name.clone());
        let data_lake = DataLakeName::new_unchecked("test-lake", tenant_name);

        let options = NamespaceOptions::new(object_store, data_lake)
            .with_flush_size(ByteSize::mb(32))
            .with_flush_interval(Duration::from_secs(5));

        assert_eq!(options.flush_size, ByteSize::mb(32));
        assert_eq!(options.flush_interval, Duration::from_secs(5));
    }
}
