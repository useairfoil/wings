//! Local file system object store factory implementation.
//!
//! This module provides a `LocalFileSystemFactory` that creates object stores
//! backed by the local file system. Each secret name gets its own subdirectory
//! within the configured root path.
//!
//! We also provide a `TemporaryFileSystemFactory` that creates the root directory
//! in a temporary location that is cleaned up when the factory is dropped.
//! This is useful for testing and development environments where you don't want
//! to persist data permanently.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use object_store::{Error as ObjectStoreError, ObjectStore, local::LocalFileSystem};
use tempfile::TempDir;

use wings_control_plane::admin::SecretName;

use crate::ObjectStoreFactory;

/// Factory for creating local file system object stores.
///
/// This factory creates object store instances backed by the local file system.
/// Each secret name is mapped to a subdirectory within the root path, providing
/// isolation between different object store configurations.
pub struct LocalFileSystemFactory {
    root_path: PathBuf,
}

impl LocalFileSystemFactory {
    pub fn new(root_path: impl AsRef<Path>) -> Result<Self, ObjectStoreError> {
        let canonical_path =
            std::fs::canonicalize(root_path.as_ref()).map_err(|e| ObjectStoreError::Generic {
                store: "LocalFileSystem",
                source: Box::new(e),
            })?;

        Ok(Self {
            root_path: canonical_path,
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
        secret_name: SecretName,
    ) -> Result<Arc<dyn ObjectStore>, ObjectStoreError> {
        let store_path = self.root_path.join(secret_name.id());

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
/// Each secret name is mapped to a subdirectory within the temporary directory,
/// providing isolation between different object store configurations.
pub struct TemporaryFileSystemFactory {
    _temp_dir: TempDir,
    local_factory: LocalFileSystemFactory,
}

impl TemporaryFileSystemFactory {
    pub fn new() -> Result<Self, ObjectStoreError> {
        let temp_dir = TempDir::new().map_err(|e| ObjectStoreError::Generic {
            store: "TemporaryFileSystem",
            source: Box::new(e),
        })?;

        let local_factory = LocalFileSystemFactory::new(temp_dir.path())?;

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
        secret_name: SecretName,
    ) -> Result<Arc<dyn ObjectStore>, ObjectStoreError> {
        self.local_factory.create_object_store(secret_name).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use wings_control_plane::admin::SecretName;

    #[test]
    fn test_factory_creation() {
        let temp_dir = TempDir::new().unwrap();
        let factory = LocalFileSystemFactory::new(temp_dir.path()).unwrap();

        assert_eq!(factory.root_path(), temp_dir.path().canonicalize().unwrap());
    }

    #[test]
    fn test_factory_creation_invalid_path() {
        let result = LocalFileSystemFactory::new("/this/path/does/not/exist");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_create_object_store() {
        let temp_dir = TempDir::new().unwrap();
        let factory = LocalFileSystemFactory::new(temp_dir.path()).unwrap();

        let secret_name = SecretName::new("test-bucket").unwrap();
        let store = factory.create_object_store(secret_name).await.unwrap();

        // Verify that the store is created successfully
        assert!(store.as_ref() as *const _ as *const () != std::ptr::null());
    }

    #[tokio::test]
    async fn test_multiple_object_stores() {
        let temp_dir = TempDir::new().unwrap();
        let factory = LocalFileSystemFactory::new(temp_dir.path()).unwrap();

        let secret1 = SecretName::new("bucket-1").unwrap();
        let secret2 = SecretName::new("bucket-2").unwrap();

        let store1 = factory.create_object_store(secret1).await.unwrap();
        let store2 = factory.create_object_store(secret2).await.unwrap();

        // Both stores should be created successfully and be different instances
        assert!(!std::ptr::addr_eq(store1.as_ref(), store2.as_ref()));
    }

    #[test]
    fn test_temporary_factory_creation() {
        let factory = TemporaryFileSystemFactory::new().unwrap();

        // The root path should exist and be a valid directory
        assert!(factory.root_path().exists());
        assert!(factory.root_path().is_dir());
    }

    #[tokio::test]
    async fn test_temporary_factory_create_object_store() {
        let factory = TemporaryFileSystemFactory::new().unwrap();

        let secret_name = SecretName::new("temp-bucket").unwrap();
        let store = factory.create_object_store(secret_name).await.unwrap();

        // Verify that the store is created successfully
        assert!(store.as_ref() as *const _ as *const () != std::ptr::null());
    }

    #[tokio::test]
    async fn test_temporary_factory_cleanup() {
        let root_path = {
            let factory = TemporaryFileSystemFactory::new().unwrap();
            let secret_name = SecretName::new("cleanup-test").unwrap();
            let _store = factory.create_object_store(secret_name).await.unwrap();

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
        let factory = TemporaryFileSystemFactory::new().unwrap();

        let secret1 = SecretName::new("temp-bucket-1").unwrap();
        let secret2 = SecretName::new("temp-bucket-2").unwrap();

        let store1 = factory.create_object_store(secret1).await.unwrap();
        let store2 = factory.create_object_store(secret2).await.unwrap();

        // Both stores should be created successfully and be different instances
        assert!(!std::ptr::addr_eq(store1.as_ref(), store2.as_ref()));

        // Both should use the same root temporary directory
        let root_path = factory.root_path();
        assert!(root_path.join("temp-bucket-1").exists());
        assert!(root_path.join("temp-bucket-2").exists());
    }
}
