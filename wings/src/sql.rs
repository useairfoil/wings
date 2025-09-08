use std::sync::Arc;

use clap::Parser;
use snafu::ResultExt;
use tokio_util::sync::CancellationToken;
use wings_control_plane::admin::NamespaceName;
use wings_object_store::LocalFileSystemFactory;
use wings_server_core::query::NamespaceProviderFactory;

use crate::{
    error::{DataFusionSnafu, InvalidResourceNameSnafu, ObjectStoreSnafu, Result},
    remote::RemoteArgs,
};

/// Run SQL queries against a namespace
#[derive(Parser)]
pub struct SqlArgs {
    /// SQL query to run
    query: String,

    /// Namespace (format: tenants/<tenant>/namespaces/<namespace>)
    #[clap(default_value = "tenants/default/namespaces/default")]
    namespace: String,

    /// The base path where the data is stored
    #[arg(long)]
    base_path: String,

    #[clap(flatten)]
    remote: RemoteArgs,
}

impl SqlArgs {
    pub async fn run(self, _ct: CancellationToken) -> Result<()> {
        let admin = self.remote.admin_client().await?;
        let offset_registry = self.remote.offset_registry_client().await?;

        let namespace_name =
            NamespaceName::parse(&self.namespace).context(InvalidResourceNameSnafu {
                resource: "namespace",
            })?;

        let object_store_factory =
            LocalFileSystemFactory::new(self.base_path).context(ObjectStoreSnafu {})?;

        let factory = NamespaceProviderFactory::new(
            Arc::new(admin),
            Arc::new(offset_registry),
            Arc::new(object_store_factory),
        );

        let namespace = factory
            .create_provider(namespace_name)
            .await
            .context(DataFusionSnafu {})?;

        let ctx = namespace
            .new_session_context()
            .await
            .context(DataFusionSnafu {})?;

        let result = ctx.sql(&self.query).await.context(DataFusionSnafu {})?;

        result.show().await.context(DataFusionSnafu {})?;

        Ok(())
    }
}
