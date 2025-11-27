use std::time::Duration;

use datafusion::common::arrow::datatypes::{DataType, Field};
use snafu::ResultExt;
use tokio_util::sync::CancellationToken;
use wings_control_plane::{
    cluster_metadata::{
        ClusterMetadata, ListNamespacesRequest, ListTenantsRequest, ListTopicsRequest,
    },
    resources::{
        CredentialName, DataLakeConfig, Namespace, NamespaceName, NamespaceOptions, Tenant,
        TenantName, Topic, TopicName, TopicOptions,
    },
};

use crate::{
    error::{CliError, ClusterMetadataSnafu, InvalidResourceNameSnafu, Result},
    remote::RemoteArgs,
};

#[derive(clap::Subcommand)]
pub enum ClusterMetadataCommands {
    /// Create a new tenant
    CreateTenant {
        /// Tenant name
        name: String,
        #[clap(flatten)]
        remote: RemoteArgs,
    },
    /// List all tenants
    ListTenants {
        #[clap(flatten)]
        remote: RemoteArgs,
    },
    /// Create a new namespace
    CreateNamespace {
        /// Namespace name
        namespace: String,
        /// Object store credentials name
        #[arg(long)]
        credentials: String,
        /// Flush interval in milliseconds
        #[arg(long)]
        flush_millis: Option<u64>,
        /// Flush size in megabytes
        #[arg(long)]
        flush_mib: Option<u64>,
        #[clap(flatten)]
        remote: RemoteArgs,
    },
    /// List namespaces for a tenant
    ListNamespaces {
        /// Tenant name
        tenant: String,
        #[clap(flatten)]
        remote: RemoteArgs,
    },
    /// Create a new topic
    CreateTopic {
        /// Topic name in format 'tenant/namespace/topic'
        name: String,
        /// Comma-separated list of fields in format 'column_name:column_type'
        fields: Vec<String>,
        /// Partition key column name (must be one of the specified fields)
        #[clap(long)]
        partition: Option<String>,
        #[clap(flatten)]
        remote: RemoteArgs,
    },
    /// List topics for a namespace
    ListTopics {
        /// Namespace name in format 'tenant/namespace'
        namespace: String,
        #[clap(flatten)]
        remote: RemoteArgs,
    },
    /// Delete a topic
    DeleteTopic {
        /// Topic name in format 'tenant/namespace/topic'
        name: String,
        /// Force deletion even if topic has data
        #[clap(long)]
        force: bool,
        #[clap(flatten)]
        remote: RemoteArgs,
    },
}

impl ClusterMetadataCommands {
    pub async fn run(self, _ct: CancellationToken) -> Result<()> {
        match self {
            ClusterMetadataCommands::CreateTenant { name, remote } => {
                let client = remote.cluster_metadata_client().await?;
                let tenant_name = TenantName::new(name)
                    .context(InvalidResourceNameSnafu { resource: "tenant" })?;

                let tenant =
                    client
                        .create_tenant(tenant_name)
                        .await
                        .context(ClusterMetadataSnafu {
                            operation: "create_tenant",
                        })?;

                print_tenant(&tenant);

                Ok(())
            }
            ClusterMetadataCommands::ListTenants { remote } => {
                let client = remote.cluster_metadata_client().await?;

                let response = client
                    .list_tenants(ListTenantsRequest::default())
                    .await
                    .context(ClusterMetadataSnafu {
                        operation: "list_tenants",
                    })?;

                for tenant in response.tenants {
                    print_tenant(&tenant);
                }

                Ok(())
            }
            ClusterMetadataCommands::CreateNamespace {
                namespace,
                credentials,
                flush_millis,
                flush_mib,
                remote,
            } => {
                let client = remote.cluster_metadata_client().await?;

                let namespace_name =
                    NamespaceName::parse(&namespace).context(InvalidResourceNameSnafu {
                        resource: "namespace",
                    })?;

                let credential_name =
                    CredentialName::parse(&credentials).context(InvalidResourceNameSnafu {
                        resource: "credential",
                    })?;

                let mut options = NamespaceOptions::new(credential_name);

                if let Some(millis) = flush_millis {
                    options.flush_interval = Duration::from_millis(millis);
                }

                if let Some(mib) = flush_mib {
                    options.flush_size = bytesize::ByteSize::mib(mib);
                }

                let namespace = client
                    .create_namespace(namespace_name, options)
                    .await
                    .context(ClusterMetadataSnafu {
                        operation: "create_namespace",
                    })?;

                print_namespace(&namespace);

                Ok(())
            }
            ClusterMetadataCommands::ListNamespaces { tenant, remote } => {
                let client = remote.cluster_metadata_client().await?;

                let tenant_name = TenantName::parse(&tenant)
                    .context(InvalidResourceNameSnafu { resource: "tenant" })?;

                let response = client
                    .list_namespaces(ListNamespacesRequest {
                        parent: tenant_name,
                        page_size: None,
                        page_token: None,
                    })
                    .await
                    .context(ClusterMetadataSnafu {
                        operation: "list_namespaces",
                    })?;

                for namespace in response.namespaces {
                    print_namespace(&namespace);
                }

                Ok(())
            }
            ClusterMetadataCommands::CreateTopic {
                name,
                fields,
                partition,
                remote,
            } => {
                let client = remote.cluster_metadata_client().await?;

                // Parse topic name
                let topic_name = TopicName::parse(&name)
                    .context(InvalidResourceNameSnafu { resource: "topic" })?;

                // Parse fields
                let parsed_fields = parse_fields(&fields)?;

                // Validate partition key if provided
                let partition_key = if let Some(partition_column) = partition {
                    let index = parsed_fields
                        .iter()
                        .position(|f| f.name() == &partition_column)
                        .ok_or_else(|| CliError::InvalidArgument {
                            name: "partition",
                            message: format!(
                                "partition key column '{}' not found in fields",
                                partition_column
                            ),
                        })?;
                    Some(index)
                } else {
                    None
                };

                let topic_options =
                    TopicOptions::new_with_partition_key(parsed_fields, partition_key);

                let topic = client
                    .create_topic(topic_name, topic_options)
                    .await
                    .context(ClusterMetadataSnafu {
                        operation: "create_topic",
                    })?;

                print_topic(&topic);

                Ok(())
            }
            ClusterMetadataCommands::ListTopics { namespace, remote } => {
                let client = remote.cluster_metadata_client().await?;

                let namespace_name =
                    NamespaceName::parse(&namespace).context(InvalidResourceNameSnafu {
                        resource: "namespace",
                    })?;

                let response = client
                    .list_topics(ListTopicsRequest::new(namespace_name))
                    .await
                    .context(ClusterMetadataSnafu {
                        operation: "list_topics",
                    })?;

                for topic in response.topics {
                    print_topic(&topic);
                }

                Ok(())
            }
            ClusterMetadataCommands::DeleteTopic {
                name,
                force,
                remote,
            } => {
                let client = remote.cluster_metadata_client().await?;

                let topic_name = TopicName::parse(&name)
                    .context(InvalidResourceNameSnafu { resource: "topic" })?;

                client
                    .delete_topic(topic_name, force)
                    .await
                    .context(ClusterMetadataSnafu {
                        operation: "delete_topic",
                    })?;

                println!("Deleted topic '{}'", name);

                Ok(())
            }
        }
    }
}

/// Parse field specifications from strings like "column_name:column_type"
fn parse_fields(fields: &[String]) -> Result<Vec<Field>, CliError> {
    let mut parsed_fields = Vec::new();

    for field_str in fields {
        let parts: Vec<&str> = field_str.split(':').collect();
        if parts.len() != 2 {
            return Err(CliError::InvalidArgument {
                name: "field",
                message: format!(
                    "invalid field format '{}'. Expected 'column_name:column_type'",
                    field_str
                ),
            });
        }

        let column_name = parts[0].trim();
        let type_str = parts[1].trim();

        if column_name.is_empty() {
            return Err(CliError::InvalidArgument {
                name: "field",
                message: "column name cannot be empty".to_string(),
            });
        }

        let data_type = match type_str.to_lowercase().as_str() {
            "int8" | "i8" => DataType::Int8,
            "int16" | "i16" => DataType::Int16,
            "int32" | "i32" => DataType::Int32,
            "int64" | "i64" => DataType::Int64,
            "uint8" | "u8" => DataType::UInt8,
            "uint16" | "u16" => DataType::UInt16,
            "uint32" | "u32" => DataType::UInt32,
            "uint64" | "u64" => DataType::UInt64,
            "float32" | "f32" => DataType::Float32,
            "float64" | "f64" => DataType::Float64,
            "string" | "utf8" => DataType::Utf8,
            "bool" | "boolean" => DataType::Boolean,
            "binary" => DataType::Binary,
            _ => {
                return Err(CliError::InvalidArgument {
                    name: "field",
                    message: format!(
                        "unsupported type '{}'. Supported types: int8, int16, int32, int64, uint8, uint16, uint32, uint64, float32, float64, string, bool, binary",
                        type_str
                    ),
                });
            }
        };

        parsed_fields.push(Field::new(column_name, data_type, false));
    }

    Ok(parsed_fields)
}

fn print_tenant(tenant: &Tenant) {
    println!("{}", tenant.name);
}

fn print_namespace(namespace: &Namespace) {
    println!("{}", namespace.name);
    println!("  flush interval: {:?}", namespace.flush_interval);
    println!("  flush size: {}", namespace.flush_size);
    println!(
        "  object store credentials: {}",
        namespace.default_object_store_credentials
    );

    match namespace.data_lake_config {
        DataLakeConfig::IcebergInMemoryCatalog => {
            println!("  data lake: iceberg in-memory catalog");
        }
        DataLakeConfig::IcebergRestCatalog(ref _config) => {
            println!("  data lake: iceberg rest catalog");
        }
    }
}

fn print_topic(topic: &Topic) {
    println!("{}", topic.name);
    if let Some(partition_key) = topic.partition_key {
        println!("  partition key: {}", topic.fields[partition_key].name());
    }
    println!("  fields:");
    for field in topic.fields.iter() {
        println!("  - {}: {}", field.name(), field.data_type());
    }
}
