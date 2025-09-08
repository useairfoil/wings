use datafusion::{error::DataFusionError, execution::object_store::ObjectStoreUrl};

use crate::resource_type;

resource_type!(Secret, "secrets");

impl SecretName {
    pub fn to_object_store_url(&self) -> Result<ObjectStoreUrl, DataFusionError> {
        ObjectStoreUrl::parse(format!("wings://{}", self.id))
    }
}

#[cfg(test)]
mod tests {
    use crate::resources::SecretName;

    #[test]
    fn test_secret_name() {
        let secret_name = SecretName::new("test-secret").unwrap();

        assert_eq!(secret_name.id(), "test-secret");
        assert_eq!(secret_name.name(), "secrets/test-secret");
    }
}
