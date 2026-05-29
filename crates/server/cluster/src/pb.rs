use snafu::Snafu;

mod inner {
    tonic::include_proto!("wings.cluster");
}

const CLUSTER_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("wings_cluster");

pub fn cluster_file_descriptor_set() -> &'static [u8] {
    CLUSTER_DESCRIPTOR_SET
}

#[derive(Debug, Snafu)]
pub enum WireError {
    #[snafu(display("missing field: {field_name}"))]
    MissingField { field_name: String },
    #[snafu(transparent)]
    Resource {
        source: wings_resources::ResourceError,
    },
    #[snafu(transparent)]
    Schema { source: wings_schema::pb::WireError },
}

type Result<T, E = WireError> = ::std::result::Result<T, E>;

pub use self::inner::*;

mod conversion {
    use wings_resources::{
        AwsConfiguration as ResourceAwsConfiguration,
        AzureConfiguration as ResourceAzureConfiguration, DataLakeConfiguration as ResourceLake,
        DeltaConfiguration as ResourceDeltaConfiguration,
        GoogleConfiguration as ResourceGoogleConfiguration,
        IcebergConfiguration as ResourceIcebergConfiguration, Namespace as ResourceNamespace,
        NamespaceOptions, ObjectStoreConfiguration as ResourceObjectStore,
        ParquetConfiguration as ResourceParquetConfiguration,
        S3CompatibleConfiguration as ResourceS3CompatibleConfiguration, Table as ResourceTable,
        TableOptions,
    };

    use super::{Result, WireError, inner};

    impl From<&ResourceNamespace> for inner::Namespace {
        fn from(value: &ResourceNamespace) -> Self {
            Self {
                name: value.name.to_string(),
                object_store: Some((&value.object_store).into()),
                lake: Some((&value.lake).into()),
            }
        }
    }

    impl TryFrom<&inner::Namespace> for NamespaceOptions {
        type Error = WireError;

        fn try_from(value: &inner::Namespace) -> Result<Self> {
            Ok(Self {
                object_store: value
                    .object_store
                    .as_ref()
                    .required("object_store")?
                    .try_into()?,
                lake: value.lake.as_ref().required("lake")?.try_into()?,
            })
        }
    }

    impl From<&ResourceTable> for inner::Table {
        fn from(value: &ResourceTable) -> Self {
            Self {
                name: value.name.to_string(),
                schema: Some((&value.schema).into()),
                description: value.description.clone(),
                key_field_id: value.key_field_id,
                version_field_id: value.version_field_id,
                partition_field_id: value.partition_field_id,
                target_freshness_seconds: 0,
            }
        }
    }

    impl TryFrom<&inner::Table> for TableOptions {
        type Error = WireError;

        fn try_from(value: &inner::Table) -> Result<Self> {
            Ok(Self {
                schema: value.schema.as_ref().required("schema")?.try_into()?,
                description: value.description.clone(),
                key_field_id: value.key_field_id,
                version_field_id: value.version_field_id,
                partition_field_id: value.partition_field_id,
            })
        }
    }

    impl From<&ResourceObjectStore> for inner::ObjectStore {
        fn from(value: &ResourceObjectStore) -> Self {
            use inner::object_store::ObjectStoreConfig;

            let object_store_config = match value {
                ResourceObjectStore::Aws(config) => ObjectStoreConfig::Aws(config.into()),
                ResourceObjectStore::Azure(config) => ObjectStoreConfig::Azure(config.into()),
                ResourceObjectStore::Google(config) => ObjectStoreConfig::Google(config.into()),
                ResourceObjectStore::S3Compatible(config) => {
                    ObjectStoreConfig::S3Compatible(config.into())
                }
            };

            Self {
                object_store_config: Some(object_store_config),
            }
        }
    }

    impl TryFrom<&inner::ObjectStore> for ResourceObjectStore {
        type Error = WireError;

        fn try_from(value: &inner::ObjectStore) -> Result<Self> {
            use inner::object_store::ObjectStoreConfig;

            let config = value
                .object_store_config
                .as_ref()
                .required("object_store_config")?;
            Ok(match config {
                ObjectStoreConfig::Aws(config) => ResourceObjectStore::Aws(config.into()),
                ObjectStoreConfig::Azure(config) => ResourceObjectStore::Azure(config.into()),
                ObjectStoreConfig::Google(config) => ResourceObjectStore::Google(config.into()),
                ObjectStoreConfig::S3Compatible(config) => {
                    ResourceObjectStore::S3Compatible(config.into())
                }
            })
        }
    }

    impl From<&ResourceAwsConfiguration> for inner::AwsConfiguration {
        fn from(value: &ResourceAwsConfiguration) -> Self {
            Self {
                bucket_name: value.bucket_name.clone(),
                prefix: value.prefix.clone(),
                access_key_id: value.access_key_id.clone(),
                secret_access_key: value.secret_access_key.clone(),
                region: value.region.clone(),
            }
        }
    }

    impl From<&inner::AwsConfiguration> for ResourceAwsConfiguration {
        fn from(value: &inner::AwsConfiguration) -> Self {
            Self {
                bucket_name: value.bucket_name.clone(),
                prefix: value.prefix.clone(),
                access_key_id: value.access_key_id.clone(),
                secret_access_key: value.secret_access_key.clone(),
                region: value.region.clone(),
            }
        }
    }

    impl From<&ResourceAzureConfiguration> for inner::AzureConfiguration {
        fn from(value: &ResourceAzureConfiguration) -> Self {
            Self {
                container_name: value.container_name.clone(),
                prefix: value.prefix.clone(),
                storage_account_name: value.storage_account_name.clone(),
                storage_account_key: value.storage_account_key.clone(),
            }
        }
    }

    impl From<&inner::AzureConfiguration> for ResourceAzureConfiguration {
        fn from(value: &inner::AzureConfiguration) -> Self {
            Self {
                container_name: value.container_name.clone(),
                prefix: value.prefix.clone(),
                storage_account_name: value.storage_account_name.clone(),
                storage_account_key: value.storage_account_key.clone(),
            }
        }
    }

    impl From<&ResourceGoogleConfiguration> for inner::GoogleConfiguration {
        fn from(value: &ResourceGoogleConfiguration) -> Self {
            Self {
                bucket_name: value.bucket_name.clone(),
                prefix: value.prefix.clone(),
                service_account: value.service_account.clone(),
                service_account_key: value.service_account_key.clone(),
            }
        }
    }

    impl From<&inner::GoogleConfiguration> for ResourceGoogleConfiguration {
        fn from(value: &inner::GoogleConfiguration) -> Self {
            Self {
                bucket_name: value.bucket_name.clone(),
                prefix: value.prefix.clone(),
                service_account: value.service_account.clone(),
                service_account_key: value.service_account_key.clone(),
            }
        }
    }

    impl From<&ResourceS3CompatibleConfiguration> for inner::S3CompatibleConfiguration {
        fn from(value: &ResourceS3CompatibleConfiguration) -> Self {
            Self {
                bucket_name: value.bucket_name.clone(),
                prefix: value.prefix.clone(),
                access_key_id: value.access_key_id.clone(),
                secret_access_key: value.secret_access_key.clone(),
                region: value.region.clone(),
                endpoint: value.endpoint.clone(),
                allow_http: value.allow_http,
            }
        }
    }

    impl From<&inner::S3CompatibleConfiguration> for ResourceS3CompatibleConfiguration {
        fn from(value: &inner::S3CompatibleConfiguration) -> Self {
            Self {
                bucket_name: value.bucket_name.clone(),
                prefix: value.prefix.clone(),
                access_key_id: value.access_key_id.clone(),
                secret_access_key: value.secret_access_key.clone(),
                region: value.region.clone(),
                endpoint: value.endpoint.clone(),
                allow_http: value.allow_http,
            }
        }
    }

    impl From<&ResourceLake> for inner::Lake {
        fn from(value: &ResourceLake) -> Self {
            use inner::lake::LakeConfig;

            let lake_config = match value {
                ResourceLake::Parquet(config) => LakeConfig::Parquet(config.into()),
                ResourceLake::Iceberg(config) => LakeConfig::Iceberg(config.into()),
                ResourceLake::Delta(config) => LakeConfig::Delta(config.into()),
            };

            Self {
                lake_config: Some(lake_config),
            }
        }
    }

    impl TryFrom<&inner::Lake> for ResourceLake {
        type Error = WireError;

        fn try_from(value: &inner::Lake) -> Result<Self> {
            use inner::lake::LakeConfig;

            let config = value.lake_config.as_ref().required("lake_config")?;
            Ok(match config {
                LakeConfig::Parquet(config) => ResourceLake::Parquet(config.into()),
                LakeConfig::Iceberg(config) => ResourceLake::Iceberg(config.into()),
                LakeConfig::Delta(config) => ResourceLake::Delta(config.into()),
            })
        }
    }

    impl From<&ResourceParquetConfiguration> for inner::ParquetConfiguration {
        fn from(_: &ResourceParquetConfiguration) -> Self {
            Self {}
        }
    }

    impl From<&inner::ParquetConfiguration> for ResourceParquetConfiguration {
        fn from(_: &inner::ParquetConfiguration) -> Self {
            Self {}
        }
    }

    impl From<&ResourceIcebergConfiguration> for inner::IcebergConfiguration {
        fn from(_: &ResourceIcebergConfiguration) -> Self {
            Self {}
        }
    }

    impl From<&inner::IcebergConfiguration> for ResourceIcebergConfiguration {
        fn from(_: &inner::IcebergConfiguration) -> Self {
            Self {}
        }
    }

    impl From<&ResourceDeltaConfiguration> for inner::DeltaConfiguration {
        fn from(_: &ResourceDeltaConfiguration) -> Self {
            Self {}
        }
    }

    impl From<&inner::DeltaConfiguration> for ResourceDeltaConfiguration {
        fn from(_: &inner::DeltaConfiguration) -> Self {
            Self {}
        }
    }

    pub trait FromOptionalField<T> {
        fn required(self, field: &str) -> Result<T>;
    }

    impl<T> FromOptionalField<T> for Option<T> {
        fn required(self, field: &str) -> Result<T> {
            self.ok_or_else(|| WireError::MissingField {
                field_name: field.to_string(),
            })
        }
    }
}
