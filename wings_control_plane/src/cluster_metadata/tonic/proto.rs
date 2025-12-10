//! Conversions between admin domain types and protobuf types.

use std::time::Duration;

use bytesize::ByteSize;
use datafusion::common::arrow::{
    datatypes::{Fields, Schema},
    ipc::{
        convert::{IpcSchemaEncoder, fb_to_schema},
        root_as_schema,
    },
};
use snafu::ResultExt;

use crate::{
    cluster_metadata::{
        ClusterMetadataError, ListDataLakesRequest, ListDataLakesResponse, ListNamespacesRequest,
        ListNamespacesResponse, ListObjectStoresRequest, ListObjectStoresResponse,
        ListTenantsRequest, ListTenantsResponse, ListTopicsRequest, ListTopicsResponse,
        Result as AdminResult,
        error::{InternalSnafu, InvalidResourceNameSnafu},
    },
    resources::{
        AwsConfiguration, AzureConfiguration, DataLake, DataLakeConfiguration, DataLakeName,
        GoogleConfiguration, IcebergConfiguration, Namespace, NamespaceName, NamespaceOptions,
        ObjectStore, ObjectStoreConfiguration, ObjectStoreName, ParquetConfiguration,
        S3CompatibleConfiguration, Tenant, TenantName, Topic, TopicName, TopicOptions,
    },
};

use super::pb;

/*
 *  ███████████ ██████████ ██████   █████   █████████   ██████   █████ ███████████
 * ░█░░░███░░░█░░███░░░░░█░░██████ ░░███   ███░░░░░███ ░░██████ ░░███ ░█░░░███░░░█
 * ░   ░███  ░  ░███  █ ░  ░███░███ ░███  ░███    ░███  ░███░███ ░███ ░   ░███  ░
 *     ░███     ░██████    ░███░░███░███  ░███████████  ░███░░███░███     ░███
 *     ░███     ░███░░█    ░███ ░░██████  ░███░░░░░███  ░███ ░░██████     ░███
 *     ░███     ░███ ░   █ ░███  ░░█████  ░███    ░███  ░███  ░░█████     ░███
 *     █████    ██████████ █████  ░░█████ █████   █████ █████  ░░█████    █████
 *    ░░░░░    ░░░░░░░░░░ ░░░░░    ░░░░░ ░░░░░   ░░░░░ ░░░░░    ░░░░░    ░░░░░
 */

impl From<Tenant> for pb::Tenant {
    fn from(tenant: Tenant) -> Self {
        Self {
            name: tenant.name.name(),
        }
    }
}

impl TryFrom<pb::Tenant> for Tenant {
    type Error = ClusterMetadataError;

    fn try_from(tenant: pb::Tenant) -> AdminResult<Self> {
        let name = TenantName::parse(&tenant.name)
            .context(InvalidResourceNameSnafu { resource: "tenant" })?;

        Ok(Self { name })
    }
}

impl From<ListTenantsRequest> for pb::ListTenantsRequest {
    fn from(request: ListTenantsRequest) -> Self {
        Self {
            page_size: request.page_size,
            page_token: request.page_token.clone(),
        }
    }
}

impl From<pb::ListTenantsRequest> for ListTenantsRequest {
    fn from(request: pb::ListTenantsRequest) -> Self {
        Self {
            page_size: request.page_size,
            page_token: request.page_token.clone(),
        }
    }
}

impl From<ListTenantsResponse> for pb::ListTenantsResponse {
    fn from(response: ListTenantsResponse) -> Self {
        let tenants = response.tenants.into_iter().map(pb::Tenant::from).collect();

        Self {
            tenants,
            next_page_token: response.next_page_token.unwrap_or_default(),
        }
    }
}

impl TryFrom<pb::ListTenantsResponse> for ListTenantsResponse {
    type Error = ClusterMetadataError;

    fn try_from(response: pb::ListTenantsResponse) -> AdminResult<Self> {
        let tenants = response
            .tenants
            .into_iter()
            .map(Tenant::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            tenants,
            next_page_token: if response.next_page_token.is_empty() {
                None
            } else {
                Some(response.next_page_token)
            },
        })
    }
}

/*
 *   ██████   █████   █████████   ██████   ██████ ██████████  █████████  ███████████    █████████     █████████  ██████████
 *  ░░██████ ░░███   ███░░░░░███ ░░██████ ██████ ░░███░░░░░█ ███░░░░░███░░███░░░░░███  ███░░░░░███   ███░░░░░███░░███░░░░░█
 *   ░███░███ ░███  ░███    ░███  ░███░█████░███  ░███  █ ░ ░███    ░░░  ░███    ░███ ░███    ░███  ███     ░░░  ░███  █ ░
 *   ░███░░███░███  ░███████████  ░███░░███ ░███  ░██████   ░░█████████  ░██████████  ░███████████ ░███          ░██████
 *   ░███ ░░██████  ░███░░░░░███  ░███ ░░░  ░███  ░███░░█    ░░░░░░░░███ ░███░░░░░░   ░███░░░░░███ ░███          ░███░░█
 *   ░███  ░░█████  ░███    ░███  ░███      ░███  ░███ ░   █ ███    ░███ ░███         ░███    ░███ ░░███     ███ ░███ ░   █
 *   █████  ░░█████ █████   █████ █████     █████ ██████████░░█████████  █████        █████   █████ ░░█████████  ██████████
 *  ░░░░░    ░░░░░ ░░░░░   ░░░░░ ░░░░░     ░░░░░ ░░░░░░░░░░  ░░░░░░░░░  ░░░░░        ░░░░░   ░░░░░   ░░░░░░░░░  ░░░░░░░░░░
 */

impl From<Namespace> for pb::Namespace {
    fn from(namespace: Namespace) -> Self {
        Self {
            name: namespace.name.name(),
            flush_size_bytes: namespace.flush_size.as_u64(),
            flush_interval_millis: namespace.flush_interval.as_millis() as u64,
            object_store: namespace.object_store.name(),
            data_lake: namespace.data_lake.name(),
        }
    }
}

impl TryFrom<pb::Namespace> for Namespace {
    type Error = ClusterMetadataError;

    fn try_from(namespace: pb::Namespace) -> AdminResult<Self> {
        let name = NamespaceName::parse(&namespace.name).context(InvalidResourceNameSnafu {
            resource: "namespace",
        })?;
        let flush_size = ByteSize::b(namespace.flush_size_bytes);
        let flush_interval = Duration::from_millis(namespace.flush_interval_millis);
        let object_store =
            ObjectStoreName::parse(&namespace.object_store).context(InvalidResourceNameSnafu {
                resource: "object store",
            })?;
        let data_lake =
            DataLakeName::parse(&namespace.data_lake).context(InvalidResourceNameSnafu {
                resource: "data lake",
            })?;

        Ok(Self {
            name,
            flush_size,
            flush_interval,
            object_store,
            data_lake,
        })
    }
}

impl From<NamespaceOptions> for pb::Namespace {
    fn from(options: NamespaceOptions) -> Self {
        Self {
            name: String::new(),
            flush_size_bytes: options.flush_size.as_u64(),
            flush_interval_millis: options.flush_interval.as_millis() as u64,
            object_store: options.object_store.to_string(),
            data_lake: options.data_lake.to_string(),
        }
    }
}

impl TryFrom<pb::Namespace> for NamespaceOptions {
    type Error = ClusterMetadataError;

    fn try_from(namespace: pb::Namespace) -> AdminResult<Self> {
        let flush_size = ByteSize::b(namespace.flush_size_bytes);
        let flush_interval = Duration::from_millis(namespace.flush_interval_millis);

        let object_store =
            ObjectStoreName::parse(&namespace.object_store).context(InvalidResourceNameSnafu {
                resource: "object store",
            })?;

        let data_lake =
            DataLakeName::parse(&namespace.data_lake).context(InvalidResourceNameSnafu {
                resource: "data lake",
            })?;

        Ok(Self {
            flush_size,
            flush_interval,
            object_store,
            data_lake,
        })
    }
}

impl From<ListNamespacesRequest> for pb::ListNamespacesRequest {
    fn from(request: ListNamespacesRequest) -> Self {
        Self {
            parent: request.parent.name(),
            page_size: request.page_size,
            page_token: request.page_token.clone(),
        }
    }
}

impl TryFrom<pb::ListNamespacesRequest> for ListNamespacesRequest {
    type Error = ClusterMetadataError;

    fn try_from(request: pb::ListNamespacesRequest) -> AdminResult<Self> {
        let parent = TenantName::parse(&request.parent)
            .context(InvalidResourceNameSnafu { resource: "tenant" })?;

        Ok(Self {
            parent,
            page_size: request.page_size,
            page_token: request.page_token.clone(),
        })
    }
}

impl From<ListNamespacesResponse> for pb::ListNamespacesResponse {
    fn from(response: ListNamespacesResponse) -> Self {
        let namespaces = response
            .namespaces
            .into_iter()
            .map(pb::Namespace::from)
            .collect();

        Self {
            namespaces,
            next_page_token: response.next_page_token.unwrap_or_default(),
        }
    }
}

impl TryFrom<pb::ListNamespacesResponse> for ListNamespacesResponse {
    type Error = ClusterMetadataError;

    fn try_from(response: pb::ListNamespacesResponse) -> AdminResult<Self> {
        let namespaces = response
            .namespaces
            .into_iter()
            .map(Namespace::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            namespaces,
            next_page_token: if response.next_page_token.is_empty() {
                None
            } else {
                Some(response.next_page_token)
            },
        })
    }
}

/*
 *  ███████████    ███████    ███████████  █████   █████████
 * ░█░░░███░░░█  ███░░░░░███ ░░███░░░░░███░░███   ███░░░░░███
 * ░   ░███  ░  ███     ░░███ ░███    ░███ ░███  ███     ░░░
 *     ░███    ░███      ░███ ░██████████  ░███ ░███
 *     ░███    ░███      ░███ ░███░░░░░░   ░███ ░███
 *     ░███    ░░███     ███  ░███         ░███ ░░███     ███
 *     █████    ░░░███████░   █████        █████ ░░█████████
 *    ░░░░░       ░░░░░░░    ░░░░░        ░░░░░   ░░░░░░░░░
 */

impl From<Topic> for pb::Topic {
    fn from(topic: Topic) -> Self {
        let fields = serialize_fields(&topic.fields);
        let partition_key = topic.partition_key.map(|idx| idx as u32);

        Self {
            name: topic.name.name(),
            fields,
            partition_key,
        }
    }
}

impl TryFrom<pb::Topic> for Topic {
    type Error = ClusterMetadataError;

    fn try_from(topic: pb::Topic) -> AdminResult<Self> {
        let name = TopicName::parse(&topic.name)
            .context(InvalidResourceNameSnafu { resource: "topic" })?;
        let fields = deserialize_fields(&topic.fields)?;
        let partition_key = topic.partition_key.map(|idx| idx as usize);

        Ok(Self {
            name,
            fields,
            partition_key,
        })
    }
}

impl TryFrom<pb::Topic> for TopicOptions {
    type Error = ClusterMetadataError;

    fn try_from(topic: pb::Topic) -> AdminResult<Self> {
        let fields = deserialize_fields(&topic.fields)?;
        let partition_key = topic.partition_key.map(|idx| idx as usize);

        Ok(Self {
            fields,
            partition_key,
        })
    }
}

impl From<TopicOptions> for pb::Topic {
    fn from(options: TopicOptions) -> Self {
        pb::Topic {
            name: String::new(),
            fields: serialize_fields(&options.fields),
            partition_key: options.partition_key.map(|idx| idx as u32),
        }
    }
}

impl From<ListTopicsRequest> for pb::ListTopicsRequest {
    fn from(request: ListTopicsRequest) -> Self {
        Self {
            parent: request.parent.name(),
            page_size: request.page_size.map(|size| size as i32),
            page_token: request.page_token.clone(),
        }
    }
}

impl TryFrom<pb::ListTopicsRequest> for ListTopicsRequest {
    type Error = ClusterMetadataError;

    fn try_from(request: pb::ListTopicsRequest) -> AdminResult<Self> {
        let parent = NamespaceName::parse(&request.parent).context(InvalidResourceNameSnafu {
            resource: "namespace",
        })?;

        Ok(Self {
            parent,
            page_size: request.page_size.map(|size| size as usize),
            page_token: request.page_token.clone(),
        })
    }
}

impl From<ListTopicsResponse> for pb::ListTopicsResponse {
    fn from(response: ListTopicsResponse) -> Self {
        let topics = response
            .topics
            .into_iter()
            .map(pb::Topic::from)
            .collect::<Vec<_>>();

        Self {
            topics,
            next_page_token: response.next_page_token.unwrap_or_default(),
        }
    }
}

impl TryFrom<pb::ListTopicsResponse> for ListTopicsResponse {
    type Error = ClusterMetadataError;

    fn try_from(response: pb::ListTopicsResponse) -> AdminResult<Self> {
        let topics = response
            .topics
            .into_iter()
            .map(Topic::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            topics,
            next_page_token: if response.next_page_token.is_empty() {
                None
            } else {
                Some(response.next_page_token)
            },
        })
    }
}

fn serialize_fields(fields: &arrow::datatypes::Fields) -> Vec<u8> {
    let schema = Schema::new(fields.clone());
    let fb = IpcSchemaEncoder::new().schema_to_fb(&schema);
    fb.finished_data().to_vec()
}

fn deserialize_fields(data: &[u8]) -> AdminResult<Fields> {
    let ipc_schema =
        root_as_schema(data).map_err(|inner| ClusterMetadataError::InvalidArgument {
            resource: "topic",
            message: format!("invalid topic schema: {}", inner),
        })?;
    let schema = fb_to_schema(ipc_schema);
    Ok(schema.fields)
}

/*
 *     ███████    ███████████        █████     █████████  ███████████    ███████    ███████████   ██████████
 *   ███░░░░░███ ░░███░░░░░███      ░░███     ███░░░░░███░█░░░███░░░█  ███░░░░░███ ░░███░░░░░███ ░░███░░░░░█
 *  ███     ░░███ ░███    ░███       ░███    ░███    ░░░ ░   ░███  ░  ███     ░░███ ░███    ░███  ░███  █ ░
 * ░███      ░███ ░██████████        ░███    ░░█████████     ░███    ░███      ░███ ░██████████   ░██████
 * ░███      ░███ ░███░░░░░███       ░███     ░░░░░░░░███    ░███    ░███      ░███ ░███░░░░░███  ░███░░█
 * ░░███     ███  ░███    ░███ ███   ░███     ███    ░███    ░███    ░░███     ███  ░███    ░███  ░███ ░   █
 *  ░░░███████░   ███████████ ░░████████     ░░█████████     █████    ░░░███████░   █████   █████ ██████████
 *    ░░░░░░░    ░░░░░░░░░░░   ░░░░░░░░       ░░░░░░░░░     ░░░░░       ░░░░░░░    ░░░░░   ░░░░░ ░░░░░░░░░░
 */

impl From<ObjectStore> for pb::ObjectStore {
    fn from(object_store: ObjectStore) -> Self {
        let name = object_store.name.name();
        let config = object_store.object_store.into();

        Self {
            name,
            object_store_config: Some(config),
        }
    }
}

impl From<ObjectStoreConfiguration> for pb::ObjectStore {
    fn from(config: ObjectStoreConfiguration) -> Self {
        let config = config.into();

        Self {
            name: String::default(),
            object_store_config: Some(config),
        }
    }
}

impl TryFrom<pb::ObjectStore> for ObjectStore {
    type Error = ClusterMetadataError;

    fn try_from(object_store: pb::ObjectStore) -> AdminResult<Self> {
        let name =
            ObjectStoreName::parse(&object_store.name).context(InvalidResourceNameSnafu {
                resource: "object store",
            })?;
        let object_store_config = object_store
            .object_store_config
            .ok_or_else(|| {
                InternalSnafu {
                    message: "missing object_store_config field in ObjectStore proto".to_string(),
                }
                .build()
            })?
            .try_into()?;

        Ok(Self {
            name,
            object_store: object_store_config,
        })
    }
}

impl TryFrom<pb::ObjectStore> for ObjectStoreConfiguration {
    type Error = ClusterMetadataError;

    fn try_from(object_store: pb::ObjectStore) -> AdminResult<Self> {
        object_store
            .object_store_config
            .ok_or_else(|| {
                InternalSnafu {
                    message: "missing object_store_config field in ObjectStore proto".to_string(),
                }
                .build()
            })?
            .try_into()
    }
}

impl From<ObjectStoreConfiguration> for pb::object_store::ObjectStoreConfig {
    fn from(config: ObjectStoreConfiguration) -> Self {
        match config {
            ObjectStoreConfiguration::Aws(_) => todo!(),
            ObjectStoreConfiguration::Azure(_) => todo!(),
            ObjectStoreConfiguration::Google(_) => todo!(),
            ObjectStoreConfiguration::S3Compatible(_) => todo!(),
        }
    }
}

impl TryFrom<pb::object_store::ObjectStoreConfig> for ObjectStoreConfiguration {
    type Error = ClusterMetadataError;

    fn try_from(config: pb::object_store::ObjectStoreConfig) -> AdminResult<Self> {
        use pb::object_store::ObjectStoreConfig;
        match config {
            ObjectStoreConfig::Aws(_) => {
                Ok(ObjectStoreConfiguration::Aws(AwsConfiguration::default()))
            }
            ObjectStoreConfig::Azure(_) => Ok(ObjectStoreConfiguration::Azure(
                AzureConfiguration::default(),
            )),
            ObjectStoreConfig::Google(_) => Ok(ObjectStoreConfiguration::Google(
                GoogleConfiguration::default(),
            )),
            ObjectStoreConfig::S3Compatible(_) => Ok(ObjectStoreConfiguration::S3Compatible(
                S3CompatibleConfiguration::default(),
            )),
        }
    }
}

impl From<ListObjectStoresRequest> for pb::ListObjectStoresRequest {
    fn from(request: ListObjectStoresRequest) -> Self {
        Self {
            parent: request.parent.name(),
            page_size: request.page_size,
            page_token: request.page_token.clone(),
        }
    }
}

impl TryFrom<pb::ListObjectStoresRequest> for ListObjectStoresRequest {
    type Error = ClusterMetadataError;

    fn try_from(request: pb::ListObjectStoresRequest) -> AdminResult<Self> {
        let parent = TenantName::parse(&request.parent)
            .context(InvalidResourceNameSnafu { resource: "tenant" })?;

        Ok(Self {
            parent,
            page_size: request.page_size,
            page_token: request.page_token.clone(),
        })
    }
}

impl From<ListObjectStoresResponse> for pb::ListObjectStoresResponse {
    fn from(response: ListObjectStoresResponse) -> Self {
        let object_stores = response
            .object_stores
            .into_iter()
            .map(pb::ObjectStore::from)
            .collect();

        Self {
            object_stores,
            next_page_token: response.next_page_token.unwrap_or_default(),
        }
    }
}

impl TryFrom<pb::ListObjectStoresResponse> for ListObjectStoresResponse {
    type Error = ClusterMetadataError;

    fn try_from(response: pb::ListObjectStoresResponse) -> AdminResult<Self> {
        let object_stores = response
            .object_stores
            .into_iter()
            .map(ObjectStore::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            object_stores,
            next_page_token: if response.next_page_token.is_empty() {
                None
            } else {
                Some(response.next_page_token)
            },
        })
    }
}

/*
 * ██████████     █████████   ███████████   █████████   █████         █████████   █████   ████ ██████████
 * ░░███░░░░███   ███░░░░░███ ░█░░░███░░░█  ███░░░░░███ ░░███         ███░░░░░███ ░░███   ███░ ░░███░░░░░█
 * ░███   ░░███ ░███    ░███ ░   ░███  ░  ░███    ░███  ░███        ░███    ░███  ░███  ███    ░███  █ ░
 * ░███    ░███ ░███████████     ░███     ░███████████  ░███        ░███████████  ░███████     ░██████
 * ░███    ░███ ░███░░░░░███     ░███     ░███░░░░░███  ░███        ░███░░░░░███  ░███░░███    ░███░░█
 * ░███    ███  ░███    ░███     ░███     ░███    ░███  ░███      █ ░███    ░███  ░███ ░░███   ░███ ░   █
 * ██████████   █████   █████    █████    █████   █████ ███████████ █████   █████ █████ ░░████ ██████████
 * ░░░░░░░░░░   ░░░░░   ░░░░░    ░░░░░    ░░░░░   ░░░░░ ░░░░░░░░░░░ ░░░░░   ░░░░░ ░░░░░   ░░░░ ░░░░░░░░░░
 */

impl From<DataLake> for pb::DataLake {
    fn from(data_lake: DataLake) -> Self {
        let name = data_lake.name.name();
        let config = data_lake.data_lake.into();

        Self {
            name,
            data_lake_config: Some(config),
        }
    }
}

impl From<DataLakeConfiguration> for pb::DataLake {
    fn from(config: DataLakeConfiguration) -> Self {
        let config = config.into();

        Self {
            name: String::default(),
            data_lake_config: Some(config),
        }
    }
}

impl TryFrom<pb::DataLake> for DataLake {
    type Error = ClusterMetadataError;

    fn try_from(data_lake: pb::DataLake) -> AdminResult<Self> {
        let name = DataLakeName::parse(&data_lake.name).context(InvalidResourceNameSnafu {
            resource: "data lake",
        })?;
        let data_lake_config = data_lake
            .data_lake_config
            .ok_or_else(|| {
                InternalSnafu {
                    message: "missing data_lake_config field in DataLake proto".to_string(),
                }
                .build()
            })?
            .try_into()?;

        Ok(Self {
            name,
            data_lake: data_lake_config,
        })
    }
}

impl From<DataLakeConfiguration> for pb::data_lake::DataLakeConfig {
    fn from(config: DataLakeConfiguration) -> Self {
        match config {
            DataLakeConfiguration::Iceberg(_) => Self::Iceberg(pb::IcebergConfiguration {}),
            DataLakeConfiguration::Parquet(_) => Self::Parquet(pb::ParquetConfiguration {}),
        }
    }
}

impl TryFrom<pb::data_lake::DataLakeConfig> for DataLakeConfiguration {
    type Error = ClusterMetadataError;

    fn try_from(config: pb::data_lake::DataLakeConfig) -> AdminResult<Self> {
        use pb::data_lake::DataLakeConfig;
        match config {
            DataLakeConfig::Iceberg(_) => Ok(DataLakeConfiguration::Iceberg(
                IcebergConfiguration::default(),
            )),
            DataLakeConfig::Parquet(_) => Ok(DataLakeConfiguration::Parquet(
                ParquetConfiguration::default(),
            )),
        }
    }
}

impl TryFrom<pb::CreateDataLakeRequest> for (DataLakeName, DataLakeConfiguration) {
    type Error = ClusterMetadataError;

    fn try_from(request: pb::CreateDataLakeRequest) -> AdminResult<Self> {
        let tenant_name = TenantName::parse(&request.parent)
            .context(InvalidResourceNameSnafu { resource: "tenant" })?;
        let data_lake_name = DataLakeName::new(request.data_lake_id, tenant_name).context(
            InvalidResourceNameSnafu {
                resource: "data lake",
            },
        )?;
        let data_lake_config = request
            .data_lake
            .ok_or_else(|| {
                InternalSnafu {
                    message: "missing data_lake field in CreateDataLakeRequest".to_string(),
                }
                .build()
            })?
            .data_lake_config
            .ok_or_else(|| {
                InternalSnafu {
                    message: "missing data_lake_config field in DataLake".to_string(),
                }
                .build()
            })?
            .try_into()?;

        Ok((data_lake_name, data_lake_config))
    }
}

impl TryFrom<pb::GetDataLakeRequest> for DataLakeName {
    type Error = ClusterMetadataError;

    fn try_from(request: pb::GetDataLakeRequest) -> AdminResult<Self> {
        DataLakeName::parse(&request.name).context(InvalidResourceNameSnafu {
            resource: "data lake",
        })
    }
}

impl From<ListDataLakesRequest> for pb::ListDataLakesRequest {
    fn from(request: ListDataLakesRequest) -> Self {
        Self {
            parent: request.parent.to_string(),
            page_size: request.page_size,
            page_token: request.page_token,
        }
    }
}

impl TryFrom<pb::ListDataLakesRequest> for ListDataLakesRequest {
    type Error = ClusterMetadataError;

    fn try_from(request: pb::ListDataLakesRequest) -> AdminResult<Self> {
        let parent = TenantName::parse(&request.parent)
            .context(InvalidResourceNameSnafu { resource: "tenant" })?;

        Ok(Self {
            parent,
            page_size: request.page_size,
            page_token: request.page_token,
        })
    }
}

impl From<ListDataLakesResponse> for pb::ListDataLakesResponse {
    fn from(response: ListDataLakesResponse) -> Self {
        Self {
            data_lakes: response.data_lakes.into_iter().map(Into::into).collect(),
            next_page_token: response.next_page_token.unwrap_or_default(),
        }
    }
}

impl TryFrom<pb::ListDataLakesResponse> for ListDataLakesResponse {
    type Error = ClusterMetadataError;

    fn try_from(response: pb::ListDataLakesResponse) -> AdminResult<Self> {
        let data_lakes = response
            .data_lakes
            .into_iter()
            .map(TryInto::try_into)
            .collect::<AdminResult<Vec<_>>>()?;

        Ok(Self {
            data_lakes,
            next_page_token: if response.next_page_token.is_empty() {
                None
            } else {
                Some(response.next_page_token)
            },
        })
    }
}

impl TryFrom<pb::DeleteDataLakeRequest> for DataLakeName {
    type Error = ClusterMetadataError;

    fn try_from(request: pb::DeleteDataLakeRequest) -> AdminResult<Self> {
        DataLakeName::parse(&request.name).context(InvalidResourceNameSnafu {
            resource: "data lake",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::arrow::datatypes::{DataType, Field};

    #[test]
    fn test_tenant_conversion() {
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let domain_tenant = Tenant::new(tenant_name.clone());

        // Domain to protobuf
        let pb_tenant = pb::Tenant::from(domain_tenant.clone());
        assert_eq!(pb_tenant.name, "tenants/test-tenant");

        // Protobuf to domain
        let converted_tenant = Tenant::try_from(pb_tenant).unwrap();
        assert_eq!(converted_tenant, domain_tenant);
    }

    #[test]
    fn test_namespace_conversion() {
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name.clone()).unwrap();
        let object_store_name = ObjectStoreName::new("test-secret", tenant_name).unwrap();
        let options = NamespaceOptions::new(object_store_name.clone());
        let domain_namespace = Namespace::new(namespace_name.clone(), options);

        // Domain to protobuf
        let pb_namespace = pb::Namespace::from(domain_namespace.clone());
        assert_eq!(
            pb_namespace.name,
            "tenants/test-tenant/namespaces/test-namespace"
        );
        assert_eq!(pb_namespace.flush_size_bytes, ByteSize::mb(8).as_u64());
        assert_eq!(pb_namespace.flush_interval_millis, 250);
        assert_eq!(
            pb_namespace.default_object_store,
            "tenants/test-tenant/object-stores/test-secret"
        );
        assert!(pb_namespace.data_lake_config.is_some());

        // Protobuf to domain
        let converted_namespace = Namespace::try_from(pb_namespace).unwrap();
        assert_eq!(converted_namespace, domain_namespace);
    }

    #[test]
    fn test_topic_conversion() {
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        let topic_name = TopicName::new("test-topic", namespace_name).unwrap();
        let fields = vec![Field::new("test", DataType::Utf8, false)];
        let options = TopicOptions::new_with_partition_key(fields.clone(), Some(0));
        let domain_topic = Topic::new(topic_name.clone(), options);

        // Domain to protobuf
        let pb_topic = pb::Topic::from(domain_topic.clone());
        assert_eq!(
            pb_topic.name,
            "tenants/test-tenant/namespaces/test-namespace/topics/test-topic"
        );
        assert_eq!(pb_topic.partition_key, Some(0));
        assert!(!pb_topic.fields.is_empty());

        // Protobuf to domain
        let converted_topic = Topic::try_from(pb_topic).unwrap();
        assert_eq!(converted_topic.name, domain_topic.name);
        assert_eq!(converted_topic.partition_key, domain_topic.partition_key);
        assert_eq!(converted_topic.fields.len(), domain_topic.fields.len());
    }

    #[test]
    fn test_list_tenants_request_conversion() {
        let domain_request = ListTenantsRequest {
            page_size: Some(50),
            page_token: Some("token123".to_string()),
        };

        // Domain to protobuf
        let pb_request = pb::ListTenantsRequest::from(domain_request.clone());
        assert_eq!(pb_request.page_size.unwrap(), 50);
        assert_eq!(pb_request.page_token.clone().unwrap(), "token123");

        // Protobuf to domain
        let converted_request = ListTenantsRequest::from(pb_request);
        assert_eq!(converted_request, domain_request);
    }

    #[test]
    fn test_list_tenants_request_defaults() {
        let domain_request = ListTenantsRequest::default();

        // Domain to protobuf
        let pb_request = pb::ListTenantsRequest::from(domain_request.clone());
        assert_eq!(pb_request.page_size, Some(100));
        assert_eq!(pb_request.page_token, None);

        // Protobuf to domain
        let converted_request = ListTenantsRequest::from(pb_request);
        assert_eq!(converted_request.page_size, Some(100));
        assert_eq!(converted_request.page_token, None);
    }

    #[test]
    fn test_list_tenants_response_conversion() {
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let tenant = Tenant::new(tenant_name);
        let domain_response = ListTenantsResponse {
            tenants: vec![tenant],
            next_page_token: Some("next-token".to_string()),
        };

        // Domain to protobuf
        let pb_response = pb::ListTenantsResponse::from(domain_response.clone());
        assert_eq!(pb_response.tenants.len(), 1);
        assert_eq!(pb_response.next_page_token, "next-token");

        // Protobuf to domain
        let converted_response = ListTenantsResponse::try_from(pb_response).unwrap();
        assert_eq!(
            converted_response.tenants.len(),
            domain_response.tenants.len()
        );
        assert_eq!(
            converted_response.next_page_token,
            domain_response.next_page_token
        );
    }

    #[test]
    fn test_invalid_tenant_name_conversion() {
        let pb_tenant = pb::Tenant {
            name: "invalid-format".to_string(),
        };

        let result = Tenant::try_from(pb_tenant);
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_namespace_name_conversion() {
        let pb_namespace = pb::Namespace {
            name: "invalid-format".to_string(),
            flush_size_bytes: 1024,
            flush_interval_millis: 250,
            default_object_store: "tenants/test-tenant/object-stores/test".to_string(),
            data_lake_config: Some(DataLakeConfig::IcebergInMemoryCatalog.into()),
        };

        let result = Namespace::try_from(pb_namespace);
        assert!(result.is_err());
    }

    #[test]
    fn test_arrow_schema_serialization() {
        let fields = vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, true),
        ];

        let serialized = serialize_fields(&fields.into());
        assert!(!serialized.is_empty());

        let deserialized = deserialize_fields(&serialized).unwrap();
        assert_eq!(deserialized.len(), 3);
        assert_eq!(deserialized[0].name(), "id");
        assert_eq!(deserialized[1].name(), "name");
        assert_eq!(deserialized[2].name(), "value");
    }
}
