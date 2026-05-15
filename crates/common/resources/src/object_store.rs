use std::sync::Arc;

use datafusion::{error::DataFusionError, execution::object_store::ObjectStoreUrl};
use serde::{Deserialize, Serialize};

use super::tenant::TenantName;
use crate::resource_type;

resource_type!(ObjectStore, "object-stores", Tenant);

/// Object store configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectStore {
    pub name: ObjectStoreName,
    pub object_store: ObjectStoreConfiguration,
}

/// Object store configuration.
///
/// Different cloud providers require different object store configurations.
/// This enum represents the various supported object store types.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ObjectStoreConfiguration {
    /// AWS S3 object store configuration.
    Aws(AwsConfiguration),
    /// Azure Blob Storage object store configuration.
    Azure(AzureConfiguration),
    /// Google Cloud Storage object store configuration.
    Google(GoogleConfiguration),
    /// S3-compatible storage object store configuration.
    S3Compatible(S3CompatibleConfiguration),
}

/// AWS S3 object store configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AwsConfiguration {
    /// Bucket name.
    pub bucket_name: String,
    /// Bucket prefix.
    pub prefix: Option<String>,
    /// `AWS_ACCESS_KEY_ID`
    pub access_key_id: String,
    /// `AWS_SECRET_ACCESS_KEY`
    pub secret_access_key: String,
    /// `AWS_DEFAULT_REGION`
    pub region: Option<String>,
}

/// Azure Blob Storage object store configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AzureConfiguration {
    /// Azure container name.
    pub container_name: String,
    /// Container prefix.
    pub prefix: Option<String>,
    /// `AZURE_STORAGE_ACCOUNT_NAME`
    pub storage_account_name: String,
    /// `AZURE_STORAGE_ACCOUNT_KEY`
    pub storage_account_key: String,
}

/// Google Cloud Storage object store configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoogleConfiguration {
    /// Bucket name.
    pub bucket_name: String,
    /// Bucket prefix.
    pub prefix: Option<String>,
    /// `GOOGLE_SERVICE_ACCOUNT`
    pub service_account: String,
    /// `GOOGLE_SERVICE_ACCOUNT_KEY`
    pub service_account_key: String,
}

/// S3-compatible storage object store configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct S3CompatibleConfiguration {
    /// Bucket name.
    pub bucket_name: String,
    /// Bucket prefix.
    pub prefix: Option<String>,
    /// `AWS_ACCESS_KEY_ID`
    pub access_key_id: String,
    /// `AWS_SECRET_ACCESS_KEY`
    pub secret_access_key: String,
    /// `AWS_ENDPOINT`
    pub endpoint: String,
    /// `AWS_DEFAULT_REGION`
    pub region: Option<String>,
    /// Allow HTTP connections.
    pub allow_http: bool,
}

pub type ObjectStoreRef = Arc<ObjectStore>;

impl ObjectStoreName {
    /// Returns the URL of the object store associated with this object store configuration.
    ///
    /// Notice that this URL is only necessary for registering the client with DataFusion.
    pub fn wings_object_store_url(&self) -> Result<ObjectStoreUrl, DataFusionError> {
        ObjectStoreUrl::parse(format!("wings://{}", self.id))
    }
}

impl ObjectStore {
    pub fn new(name: ObjectStoreName, object_store: ObjectStoreConfiguration) -> Self {
        ObjectStore { name, object_store }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_object_store_name_creation() {
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let object_store_name = ObjectStoreName::new("test-store", tenant_name.clone()).unwrap();

        assert_eq!(object_store_name.id(), "test-store");
        assert_eq!(object_store_name.parent(), &tenant_name);
        assert_eq!(
            object_store_name.name(),
            "tenants/test-tenant/object-stores/test-store"
        );
        assert_eq!(
            object_store_name.to_string(),
            "tenants/test-tenant/object-stores/test-store"
        );
    }

    #[test]
    fn test_object_store_name_parse() {
        let object_store_name =
            ObjectStoreName::parse("tenants/test-tenant/object-stores/test-store").unwrap();
        assert_eq!(object_store_name.id(), "test-store");

        // Test parse with invalid format
        let result = ObjectStoreName::parse("invalid-format");
        assert!(result.is_err());

        // Test parse with missing parent
        let result = ObjectStoreName::parse("object-stores/test-store");
        assert!(result.is_err());
    }

    #[test]
    fn test_object_store_name_from_str() {
        let object_store_name: ObjectStoreName = "tenants/test-tenant/object-stores/test-store"
            .parse()
            .unwrap();
        assert_eq!(object_store_name.id(), "test-store");

        let result: Result<ObjectStoreName, _> = "invalid".parse();
        assert!(result.is_err());
    }

    #[test]
    fn test_object_store_name_new_unchecked() {
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let object_store_name = ObjectStoreName::new_unchecked("test-store", tenant_name);
        assert_eq!(object_store_name.id(), "test-store");
    }

    #[test]
    fn test_object_store_name_wings_object_store_url() {
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let object_store_name = ObjectStoreName::new("test-store", tenant_name).unwrap();

        let url = object_store_name.wings_object_store_url().unwrap();
        // ObjectStoreUrl always adds a trailing slash
        assert_eq!(url.as_str(), "wings://test-store/");
    }

    #[test]
    fn test_object_store_configuration_aws() {
        let aws_config = AwsConfiguration {
            bucket_name: "my-bucket".to_string(),
            prefix: None,
            access_key_id: "key".to_string(),
            secret_access_key: "secret".to_string(),
            region: None,
        };
        let config = ObjectStoreConfiguration::Aws(aws_config);

        match &config {
            ObjectStoreConfiguration::Aws(c) => assert_eq!(c.bucket_name, "my-bucket"),
            _ => panic!("Expected AWS configuration"),
        }
    }

    #[test]
    fn test_object_store_configuration_azure() {
        let azure_config = AzureConfiguration {
            container_name: "my-container".to_string(),
            prefix: None,
            storage_account_name: "account".to_string(),
            storage_account_key: "key".to_string(),
        };
        let config = ObjectStoreConfiguration::Azure(azure_config);

        match &config {
            ObjectStoreConfiguration::Azure(c) => assert_eq!(c.container_name, "my-container"),
            _ => panic!("Expected Azure configuration"),
        }
    }

    #[test]
    fn test_object_store_configuration_google() {
        let google_config = GoogleConfiguration {
            bucket_name: "my-bucket".to_string(),
            prefix: None,
            service_account: "account".to_string(),
            service_account_key: "key".to_string(),
        };
        let config = ObjectStoreConfiguration::Google(google_config);

        match &config {
            ObjectStoreConfiguration::Google(c) => assert_eq!(c.bucket_name, "my-bucket"),
            _ => panic!("Expected Google configuration"),
        }
    }

    #[test]
    fn test_object_store_configuration_s3_compatible() {
        let s3_config = S3CompatibleConfiguration {
            bucket_name: "my-bucket".to_string(),
            prefix: None,
            access_key_id: "key".to_string(),
            secret_access_key: "secret".to_string(),
            endpoint: "http://localhost:9000".to_string(),
            region: None,
            allow_http: true,
        };
        let config = ObjectStoreConfiguration::S3Compatible(s3_config);

        match &config {
            ObjectStoreConfiguration::S3Compatible(c) => {
                assert_eq!(c.endpoint, "http://localhost:9000")
            }
            _ => panic!("Expected S3 compatible configuration"),
        }
    }
}
