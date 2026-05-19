//! Conversions between admin domain types and protobuf types.

use std::time::{Duration, SystemTime};

use bytesize::ByteSize;
use prost_types::Timestamp;
use snafu::ResultExt;
use wings_resources::{
    AwsConfiguration, AzureConfiguration, CompactionConfiguration, DataLake, DataLakeConfiguration,
    DataLakeName, DeltaConfiguration, GoogleConfiguration, IcebergConfiguration, Namespace,
    NamespaceName, NamespaceOptions, ObjectStore, ObjectStoreConfiguration, ObjectStoreName,
    ParquetConfiguration, S3CompatibleConfiguration, Tenant, TenantName, Table, TableCondition,
    TableName, TableOptions, TableStatus,
};

use crate::{
    cluster_metadata::{
        ListDataLakesRequest, ListDataLakesResponse, ListNamespacesRequest, ListNamespacesResponse,
        ListObjectStoresRequest, ListObjectStoresResponse, ListTenantsRequest, ListTenantsResponse,
        ListTablesRequest, ListTablesResponse, TableView,
    },
    pb::{
        self,
        error::{ResourceSnafu, Result, WireError},
        schema::FromOptionalField,
    },
};

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
    type Error = WireError;

    fn try_from(tenant: pb::Tenant) -> Result<Self> {
        let name = TenantName::parse(&tenant.name).context(ResourceSnafu { resource: "tenant" })?;

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
    type Error = WireError;

    fn try_from(response: pb::ListTenantsResponse) -> Result<Self> {
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
    type Error = WireError;

    fn try_from(namespace: pb::Namespace) -> Result<Self> {
        let name = NamespaceName::parse(&namespace.name).context(ResourceSnafu {
            resource: "namespace",
        })?;
        let flush_size = ByteSize::b(namespace.flush_size_bytes);
        let flush_interval = Duration::from_millis(namespace.flush_interval_millis);
        let object_store =
            ObjectStoreName::parse(&namespace.object_store).context(ResourceSnafu {
                resource: "object store",
            })?;
        let data_lake = DataLakeName::parse(&namespace.data_lake).context(ResourceSnafu {
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
    type Error = WireError;

    fn try_from(namespace: pb::Namespace) -> Result<Self> {
        let flush_size = ByteSize::b(namespace.flush_size_bytes);
        let flush_interval = Duration::from_millis(namespace.flush_interval_millis);

        let object_store =
            ObjectStoreName::parse(&namespace.object_store).context(ResourceSnafu {
                resource: "object store",
            })?;

        let data_lake = DataLakeName::parse(&namespace.data_lake).context(ResourceSnafu {
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
    type Error = WireError;

    fn try_from(request: pb::ListNamespacesRequest) -> Result<Self> {
        let parent =
            TenantName::parse(&request.parent).context(ResourceSnafu { resource: "tenant" })?;

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
    type Error = WireError;

    fn try_from(response: pb::ListNamespacesResponse) -> Result<Self> {
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

impl TryFrom<Table> for pb::Table {
    type Error = WireError;

    fn try_from(table: Table) -> Result<Self> {
        let schema = table.schema().into();
        let compaction = table.compaction.into();
        let status = table.status.map(pb::TableStatus::from);

        Ok(Self {
            name: table.name.name(),
            schema: Some(schema),
            description: table.description,
            partition_key: table.partition_key,
            compaction: Some(compaction),
            status,
        })
    }
}

impl TryFrom<pb::Table> for Table {
    type Error = WireError;

    fn try_from(table: pb::Table) -> Result<Self> {
        let name = TableName::parse(&table.name).context(ResourceSnafu { resource: "table" })?;
        let schema = table.schema.as_ref().required("schema")?.try_into()?;

        let compaction = table.compaction.required("compaction")?.into();

        let status = table.status.map(Into::into);

        Ok(Self {
            name,
            schema,
            description: table.description,
            partition_key: table.partition_key,
            compaction,
            status,
        })
    }
}

impl TryFrom<pb::Table> for TableOptions {
    type Error = WireError;

    fn try_from(table: pb::Table) -> Result<Self> {
        let schema = table.schema.as_ref().required("schema")?.try_into()?;
        let compaction = table.compaction.required("compaction")?.into();

        Ok(Self {
            schema,
            partition_key: table.partition_key,
            description: table.description,
            compaction,
        })
    }
}

impl TryFrom<TableOptions> for pb::Table {
    type Error = WireError;

    fn try_from(options: TableOptions) -> Result<Self> {
        let compaction = options.compaction.into();
        let schema = (&options.schema).into();

        Ok(pb::Table {
            name: String::new(),
            schema: Some(schema),
            partition_key: options.partition_key,
            description: options.description,
            compaction: Some(compaction),
            status: None,
        })
    }
}

impl From<TableView> for pb::TableView {
    fn from(view: TableView) -> Self {
        match view {
            TableView::Basic => pb::TableView::Basic,
            TableView::Full => pb::TableView::Full,
        }
    }
}

impl From<pb::TableView> for TableView {
    fn from(view: pb::TableView) -> Self {
        match view {
            pb::TableView::Unspecified => Default::default(),
            pb::TableView::Basic => TableView::Basic,
            pb::TableView::Full => TableView::Full,
        }
    }
}

impl From<CompactionConfiguration> for pb::CompactionConfiguration {
    fn from(config: CompactionConfiguration) -> Self {
        pb::CompactionConfiguration {
            freshness_seconds: config.freshness.as_secs(),
            ttl_seconds: config.ttl.as_ref().map(|ttl| ttl.as_secs()),
            target_file_size_bytes: config.target_file_size.as_u64(),
        }
    }
}

impl From<pb::CompactionConfiguration> for CompactionConfiguration {
    fn from(config: pb::CompactionConfiguration) -> Self {
        CompactionConfiguration {
            freshness: Duration::from_secs(config.freshness_seconds),
            ttl: config.ttl_seconds.map(Duration::from_secs),
            target_file_size: ByteSize::b(config.target_file_size_bytes),
        }
    }
}

impl From<TableStatus> for pb::TableStatus {
    fn from(status: TableStatus) -> Self {
        let conditions = status.conditions.into_iter().map(Into::into).collect();

        Self {
            num_partitions: status.num_partitions,
            conditions,
        }
    }
}

impl From<pb::TableStatus> for TableStatus {
    fn from(status: pb::TableStatus) -> Self {
        let conditions = status.conditions.into_iter().map(Into::into).collect();

        Self {
            num_partitions: status.num_partitions,
            conditions,
        }
    }
}

impl From<TableCondition> for pb::TableCondition {
    fn from(condition: TableCondition) -> Self {
        let duration = condition
            .last_transition_time
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or(Duration::ZERO);
        let last_transition_time = Timestamp {
            seconds: duration.as_secs() as i64,
            nanos: duration.subsec_nanos() as i32,
        };

        Self {
            r#type: condition.condition_type,
            status: condition.status,
            reason: condition.reason,
            message: condition.message,
            last_transition_time: Some(last_transition_time),
        }
    }
}

impl From<pb::TableCondition> for TableCondition {
    fn from(condition: pb::TableCondition) -> Self {
        let last_transition_time = condition
            .last_transition_time
            .map(|ts| {
                SystemTime::UNIX_EPOCH
                    + Duration::from_secs(ts.seconds as u64)
                    + Duration::from_nanos(ts.nanos as u64)
            })
            .unwrap_or_else(SystemTime::now);

        Self {
            condition_type: condition.r#type,
            status: condition.status,
            reason: condition.reason,
            message: condition.message,
            last_transition_time,
        }
    }
}

impl From<ListTablesRequest> for pb::ListTablesRequest {
    fn from(request: ListTablesRequest) -> Self {
        Self {
            parent: request.parent.name(),
            page_size: request.page_size.map(|size| size as i32),
            page_token: request.page_token.clone(),
        }
    }
}

impl TryFrom<pb::ListTablesRequest> for ListTablesRequest {
    type Error = WireError;

    fn try_from(request: pb::ListTablesRequest) -> Result<Self> {
        let parent = NamespaceName::parse(&request.parent).context(ResourceSnafu {
            resource: "namespace",
        })?;

        Ok(Self {
            parent,
            page_size: request.page_size.map(|size| size as usize),
            page_token: request.page_token.clone(),
        })
    }
}

impl TryFrom<ListTablesResponse> for pb::ListTablesResponse {
    type Error = WireError;

    fn try_from(response: ListTablesResponse) -> Result<Self> {
        let tables = response
            .tables
            .into_iter()
            .map(pb::Table::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            tables,
            next_page_token: response.next_page_token.unwrap_or_default(),
        })
    }
}

impl TryFrom<pb::ListTablesResponse> for ListTablesResponse {
    type Error = WireError;

    fn try_from(response: pb::ListTablesResponse) -> Result<Self> {
        let tables = response
            .tables
            .into_iter()
            .map(Table::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            tables,
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
    type Error = WireError;

    fn try_from(object_store: pb::ObjectStore) -> Result<Self> {
        let name = ObjectStoreName::parse(&object_store.name).context(ResourceSnafu {
            resource: "object store",
        })?;
        let object_store_config = object_store
            .object_store_config
            .required("object_store_config")?
            .try_into()?;

        Ok(Self {
            name,
            object_store: object_store_config,
        })
    }
}

impl TryFrom<pb::ObjectStore> for ObjectStoreConfiguration {
    type Error = WireError;

    fn try_from(object_store: pb::ObjectStore) -> Result<Self> {
        object_store
            .object_store_config
            .required("object_store_config")?
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
    type Error = WireError;

    fn try_from(config: pb::object_store::ObjectStoreConfig) -> Result<Self> {
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
            allow_http: c.allow_http,
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
            allow_http: c.allow_http,
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
    type Error = WireError;

    fn try_from(request: pb::ListObjectStoresRequest) -> Result<Self> {
        let parent =
            TenantName::parse(&request.parent).context(ResourceSnafu { resource: "tenant" })?;

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
    type Error = WireError;

    fn try_from(response: pb::ListObjectStoresResponse) -> Result<Self> {
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
    type Error = WireError;

    fn try_from(data_lake: pb::DataLake) -> Result<Self> {
        let name = DataLakeName::parse(&data_lake.name).context(ResourceSnafu {
            resource: "data lake",
        })?;
        let data_lake_config = data_lake
            .data_lake_config
            .required("data_lake_config")?
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
            DataLakeConfiguration::Parquet(config) => Self::Parquet(config.into()),
            DataLakeConfiguration::Iceberg(config) => Self::Iceberg(config.into()),
            DataLakeConfiguration::Delta(config) => Self::Delta(config.into()),
        }
    }
}

impl TryFrom<pb::data_lake::DataLakeConfig> for DataLakeConfiguration {
    type Error = WireError;

    fn try_from(config: pb::data_lake::DataLakeConfig) -> Result<Self> {
        use pb::data_lake::DataLakeConfig;

        match config {
            DataLakeConfig::Parquet(config) => {
                Ok(DataLakeConfiguration::Parquet(config.try_into()?))
            }
            DataLakeConfig::Iceberg(config) => {
                Ok(DataLakeConfiguration::Iceberg(config.try_into()?))
            }
            DataLakeConfig::Delta(config) => Ok(DataLakeConfiguration::Delta(config.try_into()?)),
        }
    }
}

impl From<ParquetConfiguration> for pb::ParquetConfiguration {
    fn from(_config: ParquetConfiguration) -> Self {
        pb::ParquetConfiguration {}
    }
}

impl TryFrom<pb::ParquetConfiguration> for ParquetConfiguration {
    type Error = WireError;

    fn try_from(_config: pb::ParquetConfiguration) -> Result<Self> {
        Ok(ParquetConfiguration {})
    }
}

impl From<IcebergConfiguration> for pb::IcebergConfiguration {
    fn from(_config: IcebergConfiguration) -> Self {
        pb::IcebergConfiguration {}
    }
}

impl TryFrom<pb::IcebergConfiguration> for IcebergConfiguration {
    type Error = WireError;

    fn try_from(_config: pb::IcebergConfiguration) -> Result<Self> {
        Ok(IcebergConfiguration {})
    }
}

impl From<DeltaConfiguration> for pb::DeltaConfiguration {
    fn from(config: DeltaConfiguration) -> Self {
        pb::DeltaConfiguration {
            object_store: config.object_store.map(|s| s.to_string()),
        }
    }
}

impl TryFrom<pb::DeltaConfiguration> for DeltaConfiguration {
    type Error = WireError;

    fn try_from(config: pb::DeltaConfiguration) -> Result<Self> {
        let object_store = match &config.object_store {
            None => None,
            Some(name) => {
                let name = ObjectStoreName::parse(name).context(ResourceSnafu {
                    resource: "object store",
                })?;
                Some(name)
            }
        };
        Ok(DeltaConfiguration { object_store })
    }
}

impl TryFrom<pb::CreateDataLakeRequest> for (DataLakeName, DataLakeConfiguration) {
    type Error = WireError;

    fn try_from(request: pb::CreateDataLakeRequest) -> Result<Self> {
        let tenant_name =
            TenantName::parse(&request.parent).context(ResourceSnafu { resource: "tenant" })?;
        let data_lake_name =
            DataLakeName::new(request.data_lake_id, tenant_name).context(ResourceSnafu {
                resource: "data lake",
            })?;

        let data_lake_config = request
            .data_lake
            .required("data_lake")?
            .data_lake_config
            .required("data_lake_config")?
            .try_into()?;

        Ok((data_lake_name, data_lake_config))
    }
}

impl TryFrom<pb::GetDataLakeRequest> for DataLakeName {
    type Error = WireError;

    fn try_from(request: pb::GetDataLakeRequest) -> Result<Self> {
        DataLakeName::parse(&request.name).context(ResourceSnafu {
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
    type Error = WireError;

    fn try_from(request: pb::ListDataLakesRequest) -> Result<Self> {
        let parent =
            TenantName::parse(&request.parent).context(ResourceSnafu { resource: "tenant" })?;

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
    type Error = WireError;

    fn try_from(response: pb::ListDataLakesResponse) -> Result<Self> {
        let data_lakes = response
            .data_lakes
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>>>()?;

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
    type Error = WireError;

    fn try_from(request: pb::DeleteDataLakeRequest) -> Result<Self> {
        DataLakeName::parse(&request.name).context(ResourceSnafu {
            resource: "data lake",
        })
    }
}

#[cfg(test)]
mod tests {
    use wings_schema::{DataType, Field, SchemaBuilder};

    use super::*;

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
    fn test_table_conversion() {
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name).unwrap();
        let table_name = TableName::new("test-table", namespace_name).unwrap();
        let schema = SchemaBuilder::new(vec![Field::new("test", 0, DataType::Utf8, false)])
            .build()
            .unwrap();
        let options = TableOptions::new_with_partition_key(schema, Some(0));
        let domain_table = Table::new(table_name.clone(), options);

        // Domain to protobuf
        let pb_table = pb::Table::try_from(domain_table.clone()).unwrap();

        // Protobuf to domain
        let converted_table = Table::try_from(pb_table).unwrap();
        assert_eq!(converted_table.name, domain_table.name);
        assert_eq!(converted_table.partition_key, domain_table.partition_key);
        assert_eq!(
            converted_table.schema().fields.len(),
            domain_table.schema().fields.len()
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
