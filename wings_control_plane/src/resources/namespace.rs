use std::{sync::Arc, time::Duration};

use bytesize::ByteSize;

use crate::{
    resource_type,
    resources::{CredentialName, TenantName},
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
    /// The default object store configuration for the namespace.
    pub default_object_store_credentials: CredentialName,
    /// DataLake configuration.
    pub data_lake_config: DataLakeConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IcebergRestCatalogConfig {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataLakeConfig {
    IcebergInMemoryCatalog,
    IcebergRestCatalog(IcebergRestCatalogConfig),
}

pub type NamespaceRef = Arc<Namespace>;

impl Namespace {
    /// Create a new namespace with the given name and options.
    pub fn new(name: NamespaceName, options: NamespaceOptions) -> Self {
        Self {
            name,
            flush_size: options.flush_size,
            flush_interval: options.flush_interval,
            default_object_store_credentials: options.default_object_store_credential,
            data_lake_config: options.data_lake_config,
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
    /// The default object store configuration for the namespace.
    pub default_object_store_credential: CredentialName,
    /// DataLake configuration.
    pub data_lake_config: DataLakeConfig,
}

impl NamespaceOptions {
    /// Create new namespace options with the given default object store config.
    pub fn new(default_object_store_credential: CredentialName) -> Self {
        Self {
            flush_size: ByteSize::mb(8),
            flush_interval: Duration::from_millis(250),
            default_object_store_credential,
            data_lake_config: DataLakeConfig::IcebergInMemoryCatalog,
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
        default_object_store_credential: CredentialName,
    ) -> Self {
        self.default_object_store_credential = default_object_store_credential;
        self
    }

    /// Change the data lake configuration.
    pub fn with_data_lake_config(mut self, data_lake_config: DataLakeConfig) -> Self {
        self.data_lake_config = data_lake_config;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::{CredentialName, TenantName};

    #[test]
    fn test_namespace_creation() {
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name.clone()).unwrap();
        let options =
            NamespaceOptions::new(CredentialName::new("test-config", tenant_name.clone()).unwrap());
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
        assert_eq!(
            namespace.default_object_store_credentials,
            options.default_object_store_credential
        );
        assert_eq!(namespace.data_lake_config, options.data_lake_config);
    }
}
