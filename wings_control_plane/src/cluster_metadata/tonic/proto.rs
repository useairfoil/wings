//! Conversions between admin domain types and protobuf types.

use std::time::Duration;

use bytesize::ByteSize;
use snafu::ResultExt;

use crate::{
    cluster_metadata::{
        ClusterMetadataError, ListDataLakesRequest, ListDataLakesResponse, ListNamespacesRequest,
        ListNamespacesResponse, ListObjectStoresRequest, ListObjectStoresResponse,
        ListTenantsRequest, ListTenantsResponse, ListTopicsRequest, ListTopicsResponse,
        Result as AdminResult,
        error::{InternalSnafu, InvalidResourceNameSnafu, SchemaSnafu},
    },
    resources::{
        AwsConfiguration, AzureConfiguration, CompactionConfiguration, DataLake,
        DataLakeConfiguration, DataLakeName, GoogleConfiguration, IcebergConfiguration, Namespace,
        NamespaceName, NamespaceOptions, ObjectStore, ObjectStoreConfiguration, ObjectStoreName,
        ParquetConfiguration, S3CompatibleConfiguration, Tenant, TenantName, Topic, TopicName,
        TopicOptions,
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

impl TryFrom<Topic> for pb::Topic {
    type Error = ClusterMetadataError;

    fn try_from(topic: Topic) -> AdminResult<Self> {
        let schema = topic.schema().try_into().context(SchemaSnafu {})?;
        let compaction = topic.compaction.into();

        Ok(Self {
            name: topic.name.name(),
            schema: Some(schema),
            description: topic.description,
            partition_key: topic.partition_key,
            compaction: Some(compaction),
        })
    }
}

impl TryFrom<pb::Topic> for Topic {
    type Error = ClusterMetadataError;

    fn try_from(topic: pb::Topic) -> AdminResult<Self> {
        let name = TopicName::parse(&topic.name)
            .context(InvalidResourceNameSnafu { resource: "topic" })?;
        let schema = topic
            .schema
            .as_ref()
            .ok_or_else(|| {
                InternalSnafu {
                    message: "missing schema field in Topic proto".to_string(),
                }
                .build()
            })?
            .try_into()
            .context(SchemaSnafu {})?;

        let compaction = topic
            .compaction
            .ok_or_else(|| {
                InternalSnafu {
                    message: "missing compaction field in Topic proto".to_string(),
                }
                .build()
            })?
            .into();

        Ok(Self {
            name,
            schema,
            description: topic.description,
            partition_key: topic.partition_key,
            compaction,
        })
    }
}

impl TryFrom<pb::Topic> for TopicOptions {
    type Error = ClusterMetadataError;

    fn try_from(topic: pb::Topic) -> AdminResult<Self> {
        let schema = topic
            .schema
            .as_ref()
            .ok_or_else(|| {
                InternalSnafu {
                    message: "missing schema field in Topic proto".to_string(),
                }
                .build()
            })?
            .try_into()
            .context(SchemaSnafu {})?;
        let compaction = topic
            .compaction
            .ok_or_else(|| {
                InternalSnafu {
                    message: "missing compaction field in Topic proto".to_string(),
                }
                .build()
            })?
            .into();

        Ok(Self {
            schema,
            partition_key: topic.partition_key,
            description: topic.description,
            compaction,
        })
    }
}

impl TryFrom<TopicOptions> for pb::Topic {
    type Error = ClusterMetadataError;

    fn try_from(options: TopicOptions) -> AdminResult<Self> {
        let compaction = options.compaction.into();
        let schema = (&options.schema).try_into().context(SchemaSnafu {})?;

        Ok(pb::Topic {
            name: String::new(),
            schema: Some(schema),
            partition_key: options.partition_key,
            description: options.description,
            compaction: Some(compaction),
        })
    }
}

impl From<CompactionConfiguration> for pb::CompactionConfiguration {
    fn from(config: CompactionConfiguration) -> Self {
        pb::CompactionConfiguration {
            freshness_seconds: config.freshness.as_secs(),
            ttl_seconds: config.ttl.as_ref().map(|ttl| ttl.as_secs()),
        }
    }
}

impl From<pb::CompactionConfiguration> for CompactionConfiguration {
    fn from(config: pb::CompactionConfiguration) -> Self {
        CompactionConfiguration {
            freshness: Duration::from_secs(config.freshness_seconds),
            ttl: config.ttl_seconds.map(Duration::from_secs),
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

impl TryFrom<ListTopicsResponse> for pb::ListTopicsResponse {
    type Error = ClusterMetadataError;

    fn try_from(response: ListTopicsResponse) -> AdminResult<Self> {
        let topics = response
            .topics
            .into_iter()
            .map(pb::Topic::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            topics,
            next_page_token: response.next_page_token.unwrap_or_default(),
        })
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
            ObjectStoreConfiguration::Aws(aws) => {
                pb::object_store::ObjectStoreConfig::Aws(aws.into())
            }
            ObjectStoreConfiguration::Azure(azure) => {
                pb::object_store::ObjectStoreConfig::Azure(azure.into())
            }
            ObjectStoreConfiguration::Google(google) => {
                pb::object_store::ObjectStoreConfig::Google(google.into())
            }
            ObjectStoreConfiguration::S3Compatible(s3) => {
                pb::object_store::ObjectStoreConfig::S3Compatible(s3.into())
            }
        }
    }
}

impl TryFrom<pb::object_store::ObjectStoreConfig> for ObjectStoreConfiguration {
    type Error = ClusterMetadataError;

    fn try_from(config: pb::object_store::ObjectStoreConfig) -> AdminResult<Self> {
        use pb::object_store::ObjectStoreConfig;
        match config {
            ObjectStoreConfig::Aws(aws) => Ok(ObjectStoreConfiguration::Aws(aws.into())),
            ObjectStoreConfig::Azure(azure) => Ok(ObjectStoreConfiguration::Azure(azure.into())),
            ObjectStoreConfig::Google(google) => {
                Ok(ObjectStoreConfiguration::Google(google.into()))
            }
            ObjectStoreConfig::S3Compatible(s3) => {
                Ok(ObjectStoreConfiguration::S3Compatible(s3.into()))
            }
        }
    }
}

impl From<AwsConfiguration> for pb::AwsConfiguration {
    fn from(c: AwsConfiguration) -> Self {
        pb::AwsConfiguration {
            bucket_name: c.bucket_name,
            prefix: c.prefix,
            access_key_id: c.access_key_id,
            secret_access_key: c.secret_access_key,
            region: c.region,
        }
    }
}

impl From<pb::AwsConfiguration> for AwsConfiguration {
    fn from(c: pb::AwsConfiguration) -> Self {
        AwsConfiguration {
            bucket_name: c.bucket_name,
            prefix: c.prefix,
            access_key_id: c.access_key_id,
            secret_access_key: c.secret_access_key,
            region: c.region,
        }
    }
}

impl From<AzureConfiguration> for pb::AzureConfiguration {
    fn from(c: AzureConfiguration) -> Self {
        pb::AzureConfiguration {
            container_name: c.container_name,
            prefix: c.prefix,
            storage_account_name: c.storage_account_name,
            storage_account_key: c.storage_account_key,
        }
    }
}

impl From<pb::AzureConfiguration> for AzureConfiguration {
    fn from(c: pb::AzureConfiguration) -> Self {
        AzureConfiguration {
            container_name: c.container_name,
            prefix: c.prefix,
            storage_account_name: c.storage_account_name,
            storage_account_key: c.storage_account_key,
        }
    }
}

impl From<GoogleConfiguration> for pb::GoogleConfiguration {
    fn from(c: GoogleConfiguration) -> Self {
        pb::GoogleConfiguration {
            bucket_name: c.bucket_name,
            prefix: c.prefix,
            service_account: c.service_account,
            service_account_key: c.service_account_key,
        }
    }
}

impl From<pb::GoogleConfiguration> for GoogleConfiguration {
    fn from(c: pb::GoogleConfiguration) -> Self {
        GoogleConfiguration {
            bucket_name: c.bucket_name,
            prefix: c.prefix,
            service_account: c.service_account,
            service_account_key: c.service_account_key,
        }
    }
}

impl From<S3CompatibleConfiguration> for pb::S3CompatibleConfiguration {
    fn from(c: S3CompatibleConfiguration) -> Self {
        pb::S3CompatibleConfiguration {
            bucket_name: c.bucket_name,
            prefix: c.prefix,
            access_key_id: c.access_key_id,
            secret_access_key: c.secret_access_key,
            endpoint: c.endpoint,
            region: c.region,
        }
    }
}

impl From<pb::S3CompatibleConfiguration> for S3CompatibleConfiguration {
    fn from(c: pb::S3CompatibleConfiguration) -> Self {
        S3CompatibleConfiguration {
            bucket_name: c.bucket_name,
            prefix: c.prefix,
            access_key_id: c.access_key_id,
            secret_access_key: c.secret_access_key,
            endpoint: c.endpoint,
            region: c.region,
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

    use crate::schema::{Field, Schema};
    use datafusion::common::arrow::datatypes::DataType;

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
        let tenant_name = TenantName::new_unchecked("test-tenant");
        let namespace_name = NamespaceName::new_unchecked("test-namespace", tenant_name.clone());
        let object_store_name = ObjectStoreName::new_unchecked("test-secret", tenant_name.clone());
        let data_lake_name = DataLakeName::new_unchecked("test-data-lake", tenant_name);
        let options = NamespaceOptions::new(object_store_name.clone(), data_lake_name.clone());
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
            pb_namespace.object_store,
            "tenants/test-tenant/object-stores/test-secret"
        );
        assert_eq!(
            pb_namespace.data_lake,
            "tenants/test-tenant/data-lakes/test-data-lake"
        );

        // Protobuf to domain
        let converted_namespace = Namespace::try_from(pb_namespace).unwrap();
        assert_eq!(converted_namespace, domain_namespace);
    }

    #[test]
    fn test_topic_conversion() {
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        let topic_name = TopicName::new("test-topic", namespace_name).unwrap();
        let schema = Schema::new(0, vec![Field::new("test", 0, DataType::Utf8, false)]);
        let options = TopicOptions::new_with_partition_key(schema, Some(0));
        let domain_topic = Topic::new(topic_name.clone(), options);

        // Domain to protobuf
        let pb_topic = pb::Topic::try_from(domain_topic.clone()).unwrap();

        // Protobuf to domain
        let converted_topic = Topic::try_from(pb_topic).unwrap();
        assert_eq!(converted_topic.name, domain_topic.name);
        assert_eq!(converted_topic.partition_key, domain_topic.partition_key);
        assert_eq!(
            converted_topic.schema().fields.len(),
            domain_topic.schema().fields.len()
        );
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
            object_store: "tenants/test-tenant/object-stores/test".to_string(),
            data_lake: "tenants/test-tenant/data-lakes/test".to_string(),
        };

        let result = Namespace::try_from(pb_namespace);
        assert!(result.is_err());
    }
}
