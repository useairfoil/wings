use std::sync::Arc;

use arrow::{array::RecordBatch, datatypes::Schema, util::pretty::pretty_format_batches};
use arrow_flight::{FlightInfo, sql::client::FlightSqlServiceClient};
use clap::Parser;
use futures::TryStreamExt;
use snafu::ResultExt;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;

use crate::{
    error::{ArrowSnafu, FlightSnafu, Result},
    remote::RemoteArgs,
};

/// Run SQL queries against a namespace
#[derive(Parser)]
pub struct SqlArgs {
    /// SQL query to run
    query: String,

    /// Namespace (format: namespaces/<namespace>)
    #[clap(default_value = "namespaces/default")]
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

async fn execute_flight_info(
    client: &mut FlightSqlServiceClient<Channel>,
    flight_info: FlightInfo,
) -> Result<Vec<RecordBatch>> {
    let schema: Arc<_> = Schema::try_from(flight_info.clone())
        .context(ArrowSnafu {})?
        .into();

    let mut batches = Vec::with_capacity(flight_info.endpoint.len() + 1);
    batches.push(RecordBatch::new_empty(schema));

    for endpoint in flight_info.endpoint {
        let Some(ticket) = endpoint.ticket else {
            continue;
        };

        let flight_data = client.do_get(ticket).await.context(ArrowSnafu {})?;
        let mut data: Vec<_> = flight_data.try_collect().await.context(FlightSnafu {})?;
        batches.append(&mut data);
    }

    Ok(batches)
}
