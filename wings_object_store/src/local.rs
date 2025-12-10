//! Local file system object store factory implementation.
//!
//! This module provides a `LocalFileSystemFactory` that creates object stores
//! backed by the local file system. Each credential gets its own subdirectory
//! within the configured root path using the format `<credential_type>-<tenant_id>-<credential_id>`.
//!
//! We also provide a `TemporaryFileSystemFactory` that creates the root directory
//! in a temporary location that is cleaned up when the factory is dropped.
//! This is useful for testing and development environments where you don't want
//! to persist data permanently.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use object_store::{Error as ObjectStoreError, ObjectStore, local::LocalFileSystem};
use tempfile::TempDir;
use wings_control_plane::resources::ObjectStoreConfiguration;
use wings_control_plane::{cluster_metadata::ClusterMetadata, resources::ObjectStoreName};

use crate::ObjectStoreFactory;

/// Factory for creating local file system object stores.
///
/// This factory creates object store instances backed by the local file system.
/// Each credential is mapped to a subdirectory within the root path using the format
/// `<credential_type>-<tenant_id>-<credential_id>`, providing isolation between
/// different object store configurations.
pub struct LocalFileSystemFactory {
    root_path: PathBuf,
    cluster_metadata: Arc<dyn ClusterMetadata>,
}

impl LocalFileSystemFactory {
    pub fn new(
        root_path: impl AsRef<Path>,
        cluster_metadata: Arc<dyn ClusterMetadata>,
    ) -> Result<Self, ObjectStoreError> {
        let canonical_path =
            std::fs::canonicalize(root_path.as_ref()).map_err(|e| ObjectStoreError::Generic {
                store: "LocalFileSystem",
                source: Box::new(e),
            })?;

        Ok(Self {
            root_path: canonical_path,
            cluster_metadata,
        })
    }

    pub fn root_path(&self) -> &Path {
        &self.root_path
    }
}

#[async_trait::async_trait]
impl ObjectStoreFactory for LocalFileSystemFactory {
    async fn create_object_store(
        &self,
        object_store_name: ObjectStoreName,
    ) -> Result<Arc<dyn ObjectStore>, ObjectStoreError> {
        // Fetch the object store configuration to determine its type
        let object_store = self
            .cluster_metadata
            .get_object_store(object_store_name.clone())
            .await
            .map_err(|e| ObjectStoreError::Generic {
                store: "LocalFileSystem",
                source: Box::new(e),
            })?;

        // Create store path using format: <object_store_type>-<tenant_id>-<object_store_id>
        let object_store_type = match object_store.object_store {
            ObjectStoreConfiguration::Aws(_) => "aws",
            ObjectStoreConfiguration::Azure(_) => "azure",
            ObjectStoreConfiguration::Google(_) => "google",
            ObjectStoreConfiguration::S3Compatible(_) => "s3",
        };

        let store_path = self.root_path.join(format!(
            "{}-{}-{}",
            object_store_type,
            object_store_name.parent.id(),
            object_store_name.id()
        ));

        std::fs::create_dir_all(&store_path).map_err(|e| ObjectStoreError::Generic {
            store: "LocalFileSystem",
            source: Box::new(e),
        })?;

        let local_fs = LocalFileSystem::new_with_prefix(store_path)?;

        Ok(Arc::new(local_fs))
    }
}

/// Factory for creating temporary file system object stores.
///
/// This factory creates object store instances backed by a temporary directory
/// that is automatically cleaned up when the factory is dropped. This is ideal
/// for development, testing, and scenarios where you don't want to persist data.
///
/// Each credential is mapped to a subdirectory within the temporary directory
/// using the format `<credential_type>-<tenant_id>-<credential_id>`, providing
/// isolation between different object store configurations.
pub struct TemporaryFileSystemFactory {
    _temp_dir: TempDir,
    local_factory: LocalFileSystemFactory,
}

impl TemporaryFileSystemFactory {
    pub fn new(cluster_metadata: Arc<dyn ClusterMetadata>) -> Result<Self, ObjectStoreError> {
        let temp_dir = TempDir::new().map_err(|e| ObjectStoreError::Generic {
            store: "TemporaryFileSystem",
            source: Box::new(e),
        })?;

        let local_factory = LocalFileSystemFactory::new(temp_dir.path(), cluster_metadata)?;

        Ok(Self {
            _temp_dir: temp_dir,
            local_factory,
        })
    }

    pub fn root_path(&self) -> &Path {
        self.local_factory.root_path()
    }
}

#[async_trait::async_trait]
impl ObjectStoreFactory for TemporaryFileSystemFactory {
    async fn create_object_store(
        &self,
        object_store_name: ObjectStoreName,
    ) -> Result<Arc<dyn ObjectStore>, ObjectStoreError> {
        self.local_factory
            .create_object_store(object_store_name)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tempfile::TempDir;
    use wings_control_plane::{
        cluster_metadata::{ClusterMetadata, InMemoryClusterMetadata},
        resources::{AwsConfiguration, ObjectStoreName, TenantName},
    };

    fn new_test_config() -> ObjectStoreConfiguration {
        ObjectStoreConfiguration::Aws(AwsConfiguration {
            bucket_name: "test-bucket".into(),
            prefix: None,
            access_key_id: Default::default(),
            secret_access_key: Default::default(),
            region: None,
        })
    }

    #[test]
    fn test_factory_creation() {
        let temp_dir = TempDir::new().unwrap();
        let cluster_metadata = Arc::new(InMemoryClusterMetadata::new());
        let factory = LocalFileSystemFactory::new(temp_dir.path(), cluster_metadata).unwrap();

        assert_eq!(factory.root_path(), temp_dir.path().canonicalize().unwrap());
    }

    #[test]
    fn test_factory_creation_invalid_path() {
        let cluster_metadata = Arc::new(InMemoryClusterMetadata::new());
        let result = LocalFileSystemFactory::new("/this/path/does/not/exist", cluster_metadata);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_create_object_store() {
        let temp_dir = TempDir::new().unwrap();
        let cluster_metadata = Arc::new(InMemoryClusterMetadata::new());
        // Create a tenant and credential for testing
        let tenant_name = TenantName::new("test-tenant").unwrap();
        cluster_metadata
            .create_tenant(tenant_name.clone())
            .await
            .unwrap();

        let factory =
            LocalFileSystemFactory::new(temp_dir.path(), cluster_metadata.clone()).unwrap();

        // Create the object store in cluster metadata first
        let object_store_name = ObjectStoreName::new("test-bucket", tenant_name.clone()).unwrap();
        cluster_metadata
            .create_object_store(object_store_name.clone(), new_test_config())
            .await
            .unwrap();

        let store = factory
            .create_object_store(object_store_name)
            .await
            .unwrap();

        // Verify that the store is created successfully
        assert!(store.as_ref() as *const _ as *const () != std::ptr::null());
    }

    #[tokio::test]
    async fn test_multiple_object_stores() {
        let temp_dir = TempDir::new().unwrap();
        let cluster_metadata = Arc::new(InMemoryClusterMetadata::new());
        // Create a tenant and credentials for testing
        let tenant_name = TenantName::new("test-tenant").unwrap();
        cluster_metadata
            .create_tenant(tenant_name.clone())
            .await
            .unwrap();

        let factory =
            LocalFileSystemFactory::new(temp_dir.path(), cluster_metadata.clone()).unwrap();

        let object_store1 = ObjectStoreName::new("bucket-1", tenant_name.clone()).unwrap();
        let object_store2 = ObjectStoreName::new("bucket-2", tenant_name).unwrap();

        // Create the object stores in cluster metadata first
        cluster_metadata
            .create_object_store(object_store1.clone(), new_test_config())
            .await
            .unwrap();
        cluster_metadata
            .create_object_store(object_store2.clone(), new_test_config())
            .await
            .unwrap();

        let store1 = factory.create_object_store(object_store1).await.unwrap();
        let store2 = factory.create_object_store(object_store2).await.unwrap();

        // Both stores should be created successfully and be different instances
        assert!(!std::ptr::addr_eq(store1.as_ref(), store2.as_ref()));
    }

    #[test]
    fn test_temporary_factory_creation() {
        let cluster_metadata = Arc::new(InMemoryClusterMetadata::new());
        let factory = TemporaryFileSystemFactory::new(cluster_metadata).unwrap();

        // The root path should exist and be a valid directory
        assert!(factory.root_path().exists());
        assert!(factory.root_path().is_dir());
    }

    #[tokio::test]
    async fn test_temporary_factory_create_object_store() {
        let cluster_metadata = Arc::new(InMemoryClusterMetadata::new());
        // Create a tenant and credential for testing
        let tenant_name = TenantName::new("test-tenant").unwrap();
        cluster_metadata
            .create_tenant(tenant_name.clone())
            .await
            .unwrap();

        let factory = TemporaryFileSystemFactory::new(cluster_metadata.clone()).unwrap();

        // Create the object store in cluster metadata first
        let object_store_name = ObjectStoreName::new("temp-bucket", tenant_name.clone()).unwrap();
        cluster_metadata
            .create_object_store(object_store_name.clone(), new_test_config())
            .await
            .unwrap();

        let store = factory
            .create_object_store(object_store_name)
            .await
            .unwrap();

        // Verify that the store is created successfully
        assert!(store.as_ref() as *const _ as *const () != std::ptr::null());
    }

    #[tokio::test]
    async fn test_temporary_factory_cleanup() {
        let cluster_metadata = Arc::new(InMemoryClusterMetadata::new());

        let root_path = {
            let factory = TemporaryFileSystemFactory::new(cluster_metadata.clone()).unwrap();

            // Create a tenant and credential for testing
            let tenant_name = TenantName::new("test-tenant").unwrap();
            cluster_metadata
                .create_tenant(tenant_name.clone())
                .await
                .unwrap();

            let object_store_name =
                ObjectStoreName::new("cleanup-test", tenant_name.clone()).unwrap();
            cluster_metadata
                .create_object_store(object_store_name.clone(), new_test_config())
                .await
                .unwrap();
            let _store = factory
                .create_object_store(object_store_name)
                .await
                .unwrap();

            let path = factory.root_path().to_path_buf();
            assert!(path.exists());
            path
        }; // factory is dropped here

        // Give a moment for cleanup to occur
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // The temporary directory should be cleaned up
        assert!(!root_path.exists());
    }

    #[tokio::test]
    async fn test_temporary_factory_multiple_stores() {
        let cluster_metadata = Arc::new(InMemoryClusterMetadata::new());
        let factory = TemporaryFileSystemFactory::new(cluster_metadata.clone()).unwrap();

        // Create a tenant and credentials for testing
        let tenant_name = TenantName::new("test-tenant").unwrap();
        cluster_metadata
            .create_tenant(tenant_name.clone())
            .await
            .unwrap();

        let object_store1 = ObjectStoreName::new("temp-bucket-1", tenant_name.clone()).unwrap();
        let object_store2 = ObjectStoreName::new("temp-bucket-2", tenant_name).unwrap();

        // Create the object stores in cluster metadata first
        cluster_metadata
            .create_object_store(object_store1.clone(), new_test_config())
            .await
            .unwrap();
        cluster_metadata
            .create_object_store(object_store2.clone(), new_test_config())
            .await
            .unwrap();

        let store1 = factory.create_object_store(object_store1).await.unwrap();
        let store2 = factory.create_object_store(object_store2).await.unwrap();

        // Both stores should be created successfully and be different instances
        assert!(!std::ptr::addr_eq(store1.as_ref(), store2.as_ref()));

        // Both should use the same root temporary directory
        let root_path = factory.root_path();
        assert!(root_path.join("aws-test-tenant-temp-bucket-1").exists());
        assert!(root_path.join("aws-test-tenant-temp-bucket-2").exists());
    }
}
