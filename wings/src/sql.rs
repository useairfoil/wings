use std::sync::Arc;

use clap::Parser;
use snafu::ResultExt;
use tokio_util::sync::CancellationToken;
use wings_metadata_core::admin::NamespaceName;
use wings_server_core::query::NamespaceProvider;

use crate::{
    error::{DataFusionSnafu, InvalidResourceNameSnafu, Result},
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

    #[clap(flatten)]
    remote: RemoteArgs,
}

impl SqlArgs {
    pub async fn run(self, _ct: CancellationToken) -> Result<()> {
        let admin = self.remote.admin_client().await?;

        let namespace_name =
            NamespaceName::parse(&self.namespace).context(InvalidResourceNameSnafu {
                resource: "namespace",
            })?;

        let namespace = NamespaceProvider::new(Arc::new(admin), namespace_name)
            .await
            .context(DataFusionSnafu {})?;

        let ctx = namespace.new_session_context();

        let result = ctx.sql(&self.query).await.context(DataFusionSnafu {})?;

        result.show().await.context(DataFusionSnafu {})?;

        Ok(())
    }
}
