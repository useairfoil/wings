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

pub mod local;

use std::sync::Arc;

use object_store::ObjectStore;
use wings_control_plane::admin::SecretName;

pub use local::{LocalFileSystemFactory, TemporaryFileSystemFactory};

/// Factory trait for creating ObjectStore instances from secret configurations.
#[async_trait::async_trait]
pub trait ObjectStoreFactory: Send + Sync {
    /// Create an ObjectStore instance from the configuration referenced by the secret name.
    async fn create_object_store(
        &self,
        secret_name: SecretName,
    ) -> Result<Arc<dyn ObjectStore>, object_store::Error>;
}
