use std::sync::Arc;

use arrow::util::pretty::pretty_format_batches;
use arrow_flight::{
    FlightInfo,
    sql::{CommandGetDbSchemas, CommandGetTables, client::FlightSqlServiceClient},
};
use datafusion::common::arrow::{datatypes::Schema, record_batch::RecordBatch};
use futures::TryStreamExt;
use snafu::ResultExt;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;

use crate::{
    error::{ArrowSnafu, FlightSnafu, Result},
    remote::RemoteArgs,
};

type Client = FlightSqlServiceClient<Channel>;

#[derive(clap::Subcommand)]
pub enum FlightCommands {
    /// Get the available catalogs
    ///
    /// Notice that this command is pretty useless since there's only the `wings` catalog for now.
    Catalogs {
        /// Namespace (format: tenants/<tenant>/namespaces/<namespace>)
        #[clap(default_value = "tenants/default/namespaces/default")]
        namespace: String,
        #[clap(flatten)]
        remote: RemoteArgs,
    },
    /// Get the available schemas
    Schemas {
        /// Namespace (format: tenants/<tenant>/namespaces/<namespace>)
        #[clap(default_value = "tenants/default/namespaces/default")]
        namespace: String,
        /// The catalog
        #[clap(long, default_value = "wings")]
        catalog: String,
        #[clap(flatten)]
        remote: RemoteArgs,
    },
    /// Get the available tables
    Tables {
        /// Namespace (format: tenants/<tenant>/namespaces/<namespace>)
        #[clap(default_value = "tenants/default/namespaces/default")]
        namespace: String,
        /// The catalog
        #[clap(long, default_value = "wings")]
        catalog: String,
        /// Filter table types
        ///
        /// TABLE, VIEW, and SYSTEM TABLE are commonly supported.
        #[clap(long)]
        table_type: Vec<String>,
        #[clap(flatten)]
        remote: RemoteArgs,
    },
}

impl FlightCommands {
    pub async fn run(self, _ct: CancellationToken) -> Result<()> {
        let (mut client, flight_info) = match self {
            FlightCommands::Catalogs { namespace, remote } => {
                run_catalogs(namespace, remote).await?
            }
            FlightCommands::Schemas {
                namespace,
                catalog,
                remote,
            } => run_schemas(namespace, catalog, remote).await?,
            FlightCommands::Tables {
                namespace,
                catalog,
                table_type: table_types,
                remote,
            } => run_tables(namespace, catalog, table_types, remote).await?,
        };

        let batches = execute_flight_info(&mut client, flight_info).await?;

        let res = pretty_format_batches(&batches).context(ArrowSnafu {})?;
        println!("{res}");

        Ok(())
    }
}

async fn run_catalogs(namespace: String, remote: RemoteArgs) -> Result<(Client, FlightInfo)> {
    let mut client = remote.flight_sql_client(&namespace).await?;

    let flight_info = client.get_catalogs().await.context(ArrowSnafu {})?;

    Ok((client, flight_info))
}

async fn run_schemas(
    namespace: String,
    catalog: String,
    remote: RemoteArgs,
) -> Result<(Client, FlightInfo)> {
    let mut client = remote.flight_sql_client(&namespace).await?;

    let flight_info = client
        .get_db_schemas(CommandGetDbSchemas {
            catalog: Some(catalog),
            db_schema_filter_pattern: None,
        })
        .await
        .context(ArrowSnafu {})?;

    Ok((client, flight_info))
}

async fn run_tables(
    namespace: String,
    catalog: String,
    table_types: Vec<String>,
    remote: RemoteArgs,
) -> Result<(Client, FlightInfo)> {
    let mut client = remote.flight_sql_client(&namespace).await?;

    let flight_info = client
        .get_tables(CommandGetTables {
            catalog: Some(catalog),
            db_schema_filter_pattern: None,
            table_name_filter_pattern: None,
            table_types,
            include_schema: false,
        })
        .await
        .context(ArrowSnafu {})?;

    Ok((client, flight_info))
}

pub async fn execute_flight_info(
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
