//! Production cloud object store factory implementation.
//!
//! This module provides a `CloudObjectStoreFactory` that creates object store instances
//! for cloud providers (AWS S3, Azure Blob Storage, Google Cloud Storage, and
//! S3-compatible storage) using the official object_store crate builders.

use std::sync::Arc;

use object_store::{
    Error as ObjectStoreError, ObjectStore, aws::S3CopyIfNotExists, prefix::PrefixStore,
};
use wings_control_plane_core::ClusterMetadata;
use wings_resources::{ObjectStoreConfiguration, ObjectStoreName};

use crate::ObjectStoreFactory;

/// Factory for creating cloud object store instances.
///
/// This factory creates object store instances for cloud providers using the official
/// object_store crate builders. It supports AWS S3, Azure Blob Storage,
/// Google Cloud Storage, and S3-compatible storage providers.
pub struct CloudObjectStoreFactory {
    cluster_metadata: Arc<dyn ClusterMetadata>,
}

impl CloudObjectStoreFactory {
    pub fn new(cluster_metadata: Arc<dyn ClusterMetadata>) -> Self {
        Self { cluster_metadata }
    }
}

#[async_trait::async_trait]
impl ObjectStoreFactory for CloudObjectStoreFactory {
    async fn create_object_store(
        &self,
        object_store_name: ObjectStoreName,
    ) -> Result<Arc<dyn ObjectStore>, ObjectStoreError> {
        // Fetch the object store configuration from cluster metadata
        let object_store = self
            .cluster_metadata
            .get_object_store(object_store_name.clone())
            .await
            .map_err(|e| ObjectStoreError::Generic {
                store: "CloudObjectStoreFactory",
                source: Box::new(e),
            })?;

        // Create the appropriate object store based on configuration
        let store: Arc<dyn ObjectStore> = match &object_store.object_store {
            ObjectStoreConfiguration::Aws(config) => create_aws_s3_store(config).await?,
            ObjectStoreConfiguration::Azure(config) => create_azure_blob_store(config).await?,
            ObjectStoreConfiguration::Google(config) => create_google_cloud_store(config).await?,
            ObjectStoreConfiguration::S3Compatible(config) => {
                create_s3_compatible_store(config).await?
            }
        };

        Ok(store)
    }
}

/// Create AWS S3 object store
async fn create_aws_s3_store(
    config: &wings_resources::AwsConfiguration,
) -> Result<Arc<dyn ObjectStore>, ObjectStoreError> {
    use object_store::aws::AmazonS3Builder;

    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(&config.bucket_name)
        .with_access_key_id(&config.access_key_id)
        .with_secret_access_key(&config.secret_access_key)
        .with_copy_if_not_exists(S3CopyIfNotExists::Multipart);

    // Add optional region
    if let Some(region) = &config.region {
        builder = builder.with_region(region);
    }

    let store = builder.build()?;

    let Some(prefix) = &config.prefix else {
        return Ok(Arc::new(store));
    };

    let store = PrefixStore::new(store, prefix.as_str());
    Ok(Arc::new(store))
}

/// Create Azure Blob Storage object store
async fn create_azure_blob_store(
    config: &wings_resources::AzureConfiguration,
) -> Result<Arc<dyn ObjectStore>, ObjectStoreError> {
    use object_store::azure::MicrosoftAzureBuilder;

    let builder = MicrosoftAzureBuilder::new()
        .with_container_name(&config.container_name)
        .with_account(&config.storage_account_name)
        .with_access_key(&config.storage_account_key);

    let store = builder.build()?;

    let Some(prefix) = &config.prefix else {
        return Ok(Arc::new(store));
    };

    let store = PrefixStore::new(store, prefix.as_str());
    Ok(Arc::new(store))
}

/// Create Google Cloud Storage object store
async fn create_google_cloud_store(
    config: &wings_resources::GoogleConfiguration,
) -> Result<Arc<dyn ObjectStore>, ObjectStoreError> {
    use object_store::gcp::GoogleCloudStorageBuilder;

    let builder = GoogleCloudStorageBuilder::new()
        .with_bucket_name(&config.bucket_name)
        .with_service_account_key(&config.service_account_key);

    let store = builder.build()?;

    Ok(Arc::new(store))
}

/// Create S3-compatible object store
async fn create_s3_compatible_store(
    config: &wings_resources::S3CompatibleConfiguration,
) -> Result<Arc<dyn ObjectStore>, ObjectStoreError> {
    use object_store::aws::AmazonS3Builder;

    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(&config.bucket_name)
        .with_access_key_id(&config.access_key_id)
        .with_secret_access_key(&config.secret_access_key)
        .with_endpoint(&config.endpoint)
        .with_copy_if_not_exists(S3CopyIfNotExists::Multipart);

    // Add optional region
    if let Some(region) = &config.region {
        builder = builder.with_region(region);
    }

    // Allow HTTP for S3-compatible storage (like MinIO)
    builder = builder.with_allow_http(config.allow_http);

    let store = builder.build()?;

    let Some(prefix) = &config.prefix else {
        return Ok(Arc::new(store));
    };

    let store = PrefixStore::new(store, prefix.as_str());
    Ok(Arc::new(store))
}
