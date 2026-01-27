//! Object store factory for creating ObjectStore instances from runtime configuration.
//!
//! This module provides the `ObjectStoreFactory` trait that allows components to create
//! `ObjectStore` clients dynamically based on secret configurations loaded at runtime.
//!
//! The factory abstracts away the details of how to instantiate the object store from
//! just the secret name.
//!
//! An implementation may, for example, load the secret configuration from an external
//! vault service and then create the appropriate object store client.

pub mod cloud;
pub mod local;

use std::sync::Arc;

use object_store::ObjectStore;

use crate::resources::ObjectStoreName;

pub use self::cloud::{CloudObjectStoreFactory, Error as CloudObjectStoreError};
pub use self::local::{LocalFileSystemFactory, TemporaryFileSystemFactory};

/// Factory trait for creating ObjectStore instances from object store configurations.
#[async_trait::async_trait]
pub trait ObjectStoreFactory: Send + Sync {
    /// Create an ObjectStore instance from the configuration referenced by the object store name.
    async fn create_object_store(
        &self,
        object_store_name: ObjectStoreName,
    ) -> Result<Arc<dyn ObjectStore>, object_store::Error>;
}
