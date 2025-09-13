use arrow::util::pretty::pretty_format_batches;
use clap::Parser;
use snafu::ResultExt;
use tokio_util::sync::CancellationToken;

use crate::{
    error::{ArrowSnafu, Result},
    flight::execute_flight_info,
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
        let mut client = self.remote.flight_sql_client(&self.namespace).await?;
        client.set_header("x-wings-namespace", self.namespace);

        let flight_info = client
            .execute(self.query, None)
            .await
            .context(ArrowSnafu {})?;

        let batches = execute_flight_info(&mut client, flight_info).await?;

        let out = pretty_format_batches(&batches).context(ArrowSnafu {})?;
        println!("{}", out);

        Ok(())
    }
}
