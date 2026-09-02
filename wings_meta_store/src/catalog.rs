use std::{collections::HashMap, fmt, sync::Arc};

use iceberg::{Catalog, CatalogBuilder};
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use wings_secret_store::{SecretName, SecretStore};

/// A unique identifier of a catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CatalogId(Uuid);

/// Configuration of an Iceberg catalog.
///
/// Currently, only REST catalogs are supported.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CatalogConfig {
    /// An Iceberg REST catalog.
    Rest(RestCatalogConfig),
}

/// Configuration of an Iceberg REST catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestCatalogConfig {
    /// URI of the REST catalog server.
    pub uri: String,
    /// Warehouse location, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warehouse: Option<String>,
    /// Additional properties for the REST catalog server.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub properties: HashMap<String, String>,
}

/// A catalog, as stored in the meta store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredCatalog {
    id: CatalogId,
    config: CatalogConfig,
}

/// The error type for catalog store operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Catalog already exists.
    #[error("catalog already exists: {0}")]
    AlreadyExists(CatalogId),
    /// Failed to serialize a catalog configuration.
    #[error("failed to serialize catalog config: {source}")]
    Serialize {
        #[source]
        source: serde_json::Error,
    },
    /// Failed to deserialize a catalog configuration.
    #[error("failed to deserialize catalog config: {source}")]
    Deserialize {
        #[source]
        source: serde_json::Error,
    },
    /// Secret store error.
    #[error("secret store error: {0}")]
    SecretStore(#[from] wings_secret_store::Error),
    /// Failed to create the iceberg catalog.
    #[error("failed to create iceberg catalog: {0}")]
    Iceberg(#[from] iceberg::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A store for catalog configurations.
///
/// The store persists catalog configurations in a [`SecretStore`].
#[derive(Debug, Clone)]
pub struct CatalogStore {
    secret_store: Arc<dyn SecretStore>,
}

impl CatalogId {
    /// Creates a new catalog id from a UUID.
    pub fn new(id: Uuid) -> Self {
        Self(id)
    }

    /// Returns the underlying UUID.
    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl fmt::Display for CatalogId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl StoredCatalog {
    /// Creates a new stored catalog with the given id and configuration.
    pub fn new(id: CatalogId, config: CatalogConfig) -> Self {
        Self { id, config }
    }

    /// Returns the id of the catalog.
    pub fn id(&self) -> CatalogId {
        self.id
    }

    /// Returns the configuration of the catalog.
    pub fn config(&self) -> &CatalogConfig {
        &self.config
    }

    /// Creates the iceberg [`Catalog`] described by this stored catalog.
    pub async fn to_catalog(&self) -> Result<Arc<dyn Catalog>> {
        match &self.config {
            CatalogConfig::Rest(config) => {
                let mut props = config.properties.clone();
                props.insert(REST_CATALOG_PROP_URI.to_string(), config.uri.clone());
                if let Some(warehouse) = &config.warehouse {
                    props.insert(REST_CATALOG_PROP_WAREHOUSE.to_string(), warehouse.clone());
                }

                let catalog = RestCatalogBuilder::default()
                    .load(self.id.to_string(), props)
                    .await?;
                Ok(Arc::new(catalog))
            }
        }
    }
}

impl CatalogStore {
    /// Creates a new catalog store on top of the given secret store.
    pub fn new(secret_store: Arc<dyn SecretStore>) -> Self {
        Self { secret_store }
    }

    /// Returns the catalog with the given id, or `None` if it does not exist.
    pub async fn get(&self, id: CatalogId) -> Result<Option<StoredCatalog>> {
        let name = secret_name(id);
        match self.secret_store.get(&name).await {
            Ok(secret) => {
                let config = serde_json::from_slice::<CatalogConfig>(secret.value().as_ref())
                    .map_err(|source| Error::Deserialize { source })?;
                Ok(Some(StoredCatalog::new(id, config)))
            }
            Err(wings_secret_store::Error::NotFound(_)) => Ok(None),
            Err(source) => Err(Error::SecretStore(source)),
        }
    }

    /// Creates a catalog with the given id and configuration.
    ///
    /// Returns an error if a catalog with the given id already exists.
    pub async fn create(&self, id: CatalogId, config: CatalogConfig) -> Result<()> {
        let name = secret_name(id);
        if self.secret_store.get(&name).await.is_ok() {
            return Err(Error::AlreadyExists(id));
        }

        let value = serde_json::to_vec(&config).map_err(|source| Error::Serialize { source })?;
        self.secret_store.put(&name, value.into()).await?;
        Ok(())
    }

    /// Deletes the catalog with the given id.
    pub async fn delete(&self, id: CatalogId) -> Result<()> {
        self.secret_store.delete(&secret_name(id)).await?;
        Ok(())
    }
}

/// Returns the name of the secret holding the configuration of the catalog
/// with the given id.
fn secret_name(id: CatalogId) -> SecretName {
    // PANIC: the name is never empty.
    SecretName::new_unchecked(format!("catalog/{id}"))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use wings_secret_store::memory::InMemorySecretStore;

    use super::*;

    fn store() -> CatalogStore {
        CatalogStore::new(Arc::new(InMemorySecretStore::new()))
    }

    fn id() -> CatalogId {
        CatalogId::new(Uuid::from_u128(42))
    }

    fn rest_config() -> CatalogConfig {
        CatalogConfig::Rest(RestCatalogConfig {
            uri: "https://rest.catalog.example.com".to_string(),
            warehouse: None,
            properties: HashMap::new(),
        })
    }

    #[test]
    fn config_serialization() {
        let config = CatalogConfig::Rest(RestCatalogConfig {
            uri: "https://rest.catalog.example.com".to_string(),
            warehouse: Some("s3://warehouse".to_string()),
            properties: HashMap::from([("token".to_string(), "hunter2".to_string())]),
        });

        let json = serde_json::to_string(&config).unwrap();
        insta::assert_snapshot!(
            json,
            @r#"{"type":"rest","uri":"https://rest.catalog.example.com","warehouse":"s3://warehouse","properties":{"token":"hunter2"}}"#
        );

        let deserialized: CatalogConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, config);
    }

    #[tokio::test]
    async fn catalog_lifecycle() {
        let store = store();
        let id = id();

        assert!(store.get(id).await.unwrap().is_none());

        store.create(id, rest_config()).await.unwrap();
        let catalog = store.get(id).await.unwrap().unwrap();
        assert_eq!(catalog.id(), id);
        assert_eq!(catalog.config(), &rest_config());

        insta::assert_compact_debug_snapshot!(
            store.create(id, rest_config()).await.unwrap_err(),
            @"AlreadyExists(CatalogId(00000000-0000-0000-0000-00000000002a))"
        );

        store.delete(id).await.unwrap();
        assert!(store.get(id).await.unwrap().is_none());

        store.delete(id).await.unwrap();
    }

    #[tokio::test]
    async fn to_catalog() {
        let store = store();
        let id = id();

        let config = CatalogConfig::Rest(RestCatalogConfig {
            uri: "https://rest.catalog.example.com".to_string(),
            warehouse: Some("s3://warehouse".to_string()),
            properties: HashMap::from([("token".to_string(), "hunter2".to_string())]),
        });
        store.create(id, config).await.unwrap();
        let catalog = store.get(id).await.unwrap().unwrap();

        let iceberg_catalog = catalog.to_catalog().await.unwrap();
        insta::assert_debug_snapshot!(iceberg_catalog, @r#"
        RestCatalog {
            user_config: RestCatalogConfig {
                name: Some(
                    "00000000-0000-0000-0000-00000000002a",
                ),
                uri: "https://rest.catalog.example.com",
                warehouse: Some(
                    "s3://warehouse",
                ),
                props: {
                    "token": "hunter2",
                },
                client: None,
            },
            ctx: OnceCell {
                value: None,
            },
            storage_factory: None,
            runtime: Runtime,
        }
        "#);

        let config = CatalogConfig::Rest(RestCatalogConfig {
            uri: String::new(),
            warehouse: None,
            properties: HashMap::new(),
        });
        let catalog = StoredCatalog::new(id, config);

        insta::assert_snapshot!(
            catalog.to_catalog().await.unwrap_err().to_string(),
            @"failed to create iceberg catalog: DataInvalid => Catalog uri is required"
        );
    }
}
