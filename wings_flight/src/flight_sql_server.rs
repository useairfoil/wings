use std::{collections::HashSet, sync::Arc};

use arrow_flight::{
    FlightDescriptor, FlightEndpoint, FlightInfo, PutResult, Ticket,
    decode::{DecodedFlightData, DecodedPayload, FlightDataDecoder},
    encode::FlightDataEncoderBuilder,
    error::FlightError,
    flight_service_server::{FlightService, FlightServiceServer},
    sql::{
        Any, CommandGetCatalogs, CommandGetDbSchemas, CommandGetTables, CommandStatementIngest,
        CommandStatementQuery, ProstMessageExt, SqlInfo, TicketStatementQuery,
        server::{FlightSqlService, PeekableFlightDataStream},
    },
};
use datafusion::{
    error::DataFusionError,
    logical_expr::LogicalPlan,
    prelude::{SQLOptions, SessionContext},
};
use futures::{StreamExt, TryFutureExt, TryStreamExt};
use prost::Message;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, metadata::MetadataMap};
use tracing::{debug, instrument};
use wings_control_plane::{
    log_metadata::{CommittedBatch, RejectedBatchInfo},
    resources::{NamespaceName, TopicName},
};
use wings_server_core::query::NamespaceProviderFactory;

use crate::{
    IngestionRequestMetadata, IngestionResponseMetadata, error::FlightServerError,
    ticket::StatementQueryTicket,
};

pub const WINGS_FLIGHT_SQL_NAMESPACE_HEADER: &str = "x-wings-namespace";

pub struct WingsFlightSqlServer {
    provider_factory: NamespaceProviderFactory,
}

impl WingsFlightSqlServer {
    pub fn new(provider_factory: NamespaceProviderFactory) -> Self {
        Self { provider_factory }
    }

    pub fn into_tonic_server(self) -> FlightServiceServer<Self> {
        FlightServiceServer::new(self)
    }

    async fn new_session_context(
        &self,
        namespace_name: NamespaceName,
    ) -> Result<SessionContext, DataFusionError> {
        let provider = self
            .provider_factory
            .create_provider(namespace_name)
            .await?;
        let ctx = provider.new_session_context().await?;
        Ok(ctx)
    }
}

#[tonic::async_trait]
impl FlightSqlService for WingsFlightSqlServer {
    type FlightService = WingsFlightSqlServer;

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}

    #[instrument(level = "DEBUG", skip_all)]
    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let flight_descriptor = request.into_inner();
        let ticket = Ticket {
            ticket: query.as_any().encode_to_vec().into(),
        };
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let flight_info = FlightInfo::new()
            .try_with_schema(&query.into_builder().schema())
            .map_err(FlightServerError::from)?
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);

        Ok(Response::new(flight_info))
    }

    #[instrument(level = "DEBUG", skip_all)]
    async fn do_get_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let namespace_name = get_namespace_from_headers(request.metadata())?;
        let ctx = self
            .new_session_context(namespace_name)
            .await
            .map_err(FlightServerError::from)?;

        let mut builder = query.into_builder();
        for name in ctx.catalog_names() {
            builder.append(name);
        }

        let schema = builder.schema();
        let batch = builder.build();
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::once(async { batch }))
            .map_err(Status::from)
            .boxed();

        Ok(Response::new(stream))
    }

    #[instrument(level = "DEBUG", skip_all)]
    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let flight_descriptor = request.into_inner();
        let ticket = Ticket {
            ticket: query.as_any().encode_to_vec().into(),
        };
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let flight_info = FlightInfo::new()
            .try_with_schema(&query.into_builder().schema())
            .map_err(FlightServerError::from)?
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);

        Ok(Response::new(flight_info))
    }

    #[instrument(level = "DEBUG", skip_all)]
    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let namespace_name = get_namespace_from_headers(request.metadata())?;
        let ctx = self
            .new_session_context(namespace_name)
            .await
            .map_err(FlightServerError::from)?;

        let Some(catalog) = ctx.catalog(query.catalog()) else {
            return Err(Status::not_found(format!(
                "catalog {} not found",
                query.catalog()
            )));
        };

        let catalog_name = query.catalog().to_string();
        let mut builder = query.into_builder();

        for schema_name in catalog.schema_names() {
            builder.append(&catalog_name, schema_name);
        }

        let schema = builder.schema();
        let batch = builder.build();
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::once(async { batch }))
            .map_err(Status::from)
            .boxed();

        Ok(Response::new(stream))
    }

    #[instrument(level = "DEBUG", skip_all)]
    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let flight_descriptor = request.into_inner();
        let ticket = Ticket {
            ticket: query.as_any().encode_to_vec().into(),
        };
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let flight_info = FlightInfo::new()
            .try_with_schema(&query.into_builder().schema())
            .map_err(FlightServerError::from)?
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);

        Ok(Response::new(flight_info))
    }

    #[instrument(level = "DEBUG", skip_all)]
    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let namespace_name = get_namespace_from_headers(request.metadata())?;
        let ctx = self
            .new_session_context(namespace_name)
            .await
            .map_err(FlightServerError::from)?;

        let table_types: HashSet<String> =
            HashSet::from_iter(query.table_types.iter().map(|s| s.to_uppercase()));

        let Some(catalog) = ctx.catalog(query.catalog()) else {
            return Err(Status::not_found(format!(
                "catalog {} not found",
                query.catalog()
            )));
        };

        let catalog_name = query.catalog().to_string();
        let mut builder = query.into_builder();

        for schema_name in catalog.schema_names() {
            let Some(schema_provider) = catalog.schema(&schema_name) else {
                continue;
            };

            for table_name in schema_provider.table_names() {
                let Some(table) = schema_provider
                    .table(&table_name)
                    .await
                    .map_err(FlightServerError::from)?
                else {
                    continue;
                };

                let table_type = table.table_type().to_string().to_uppercase();
                if table_types.is_empty() || table_types.contains(&table_type) {
                    builder
                        .append(
                            &catalog_name,
                            &schema_name,
                            table_name,
                            &table_type,
                            &table.schema(),
                        )
                        .map_err(FlightServerError::from)?;
                }
            }
        }

        let schema = builder.schema();
        let batch = builder.build();
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::once(async { batch }))
            .map_err(Status::from)
            .boxed();

        Ok(Response::new(stream))
    }

    #[instrument(level = "DEBUG", skip_all)]
    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let namespace_name = get_namespace_from_headers(request.metadata())?;
        let ctx = self
            .new_session_context(namespace_name)
            .await
            .map_err(FlightServerError::from)?;

        let plan = ctx
            .state()
            .create_logical_plan(&query.query)
            .await
            .map_err(FlightServerError::from)?;

        validate_logical_plan(&plan)?;

        let ticket = StatementQueryTicket::new(query.query).into_ticket();

        let flight_descriptor = request.into_inner();
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let flight_info = FlightInfo::new()
            .try_with_schema(plan.schema().as_arrow())
            .map_err(FlightServerError::from)?
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);

        Ok(Response::new(flight_info))
    }

    #[instrument(level = "DEBUG", skip_all)]
    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        debug!("do_get_statement");
        let ticket = StatementQueryTicket::try_decode(ticket.statement_handle)?;

        let namespace_name = get_namespace_from_headers(request.metadata())?;
        let ctx = self
            .new_session_context(namespace_name)
            .await
            .map_err(FlightServerError::from)?;

        let out = ctx
            .sql(&ticket.query)
            .await
            .map_err(FlightServerError::from)?;

        let schema: Arc<_> = out.schema().as_arrow().clone().into();

        let stream = out
            .execute_stream()
            .await
            .map_err(FlightServerError::from)?;

        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(stream.map_err(|err| FlightError::from_external_error(Box::new(err))))
            .map_err(Status::from)
            .boxed();

        Ok(Response::new(stream))
    }

    #[instrument(level = "DEBUG", skip_all)]
    async fn do_put_fallback(
        &self,
        request: Request<PeekableFlightDataStream>,
        command: Any,
    ) -> Result<Response<<Self as FlightService>::DoPutStream>, Status> {
        debug!(command = ?command, "do_put_fallback");
        let mut request_stream = FlightDataDecoder::new(request.into_inner().map_err(From::from));
        let Some(first_message) = request_stream.try_next().await? else {
            return Err(Status::invalid_argument("missing first message"));
        };

        let Some(flight_descriptor) = first_message.inner.flight_descriptor else {
            return Err(Status::invalid_argument("missing flight descriptor"));
        };

        let topic_name: TopicName = flight_descriptor
            .path
            .first()
            .ok_or_else(|| Status::invalid_argument("missing path"))?
            .parse()
            .unwrap();

        let DecodedPayload::Schema(_schema) = first_message.payload else {
            return Err(Status::invalid_argument("expected schema"));
        };

        println!("topic_name: {}", topic_name);

        // TODO: delete all of this and implement
        let (tx, rx) = mpsc::channel(100);

        // TODO:
        //  - validate namespace
        //  - validate topic matches namespace
        //  - parse partition value, timestamp
        //  - parse record batches
        tokio::spawn(async move {
            let Ok(_) = tx
                .send(Ok(PutResult {
                    app_metadata: Default::default(),
                }))
                .await
            else {
                return;
            };

            while let Some(flight_data) = request_stream.try_next().await.unwrap() {
                let metadata =
                    IngestionRequestMetadata::try_decode(flight_data.app_metadata()).unwrap();

                let DecodedPayload::RecordBatch(batch) = flight_data.payload else {
                    println!("Received unexpected payload type");
                    continue;
                };

                println!(
                    "Received {} rows with metadata: {:?}",
                    batch.num_rows(),
                    metadata
                );

                let response = CommittedBatch::Rejected(RejectedBatchInfo {
                    num_messages: batch.num_rows() as _,
                });

                let Ok(_) = tx
                    .send(Ok(PutResult {
                        app_metadata: IngestionResponseMetadata::new(metadata.request_id, response)
                            .encode(),
                    }))
                    .await
                else {
                    break;
                };
            }
        });

        // let output = futures::stream::once(async { Err(Status::unimplemented("message")) });
        Ok(Response::new(ReceiverStream::new(rx).boxed()))
    }
}

fn get_namespace_from_headers(headers: &MetadataMap) -> Result<NamespaceName, Status> {
    let namespace = headers
        .get(WINGS_FLIGHT_SQL_NAMESPACE_HEADER)
        .ok_or_else(|| {
            Status::invalid_argument(format!(
                "missing namespace metadata {}",
                WINGS_FLIGHT_SQL_NAMESPACE_HEADER
            ))
        })?;

    let namespace = namespace
        .to_str()
        .map_err(|_| Status::invalid_argument("malformed namespace metadata: invalid UTF-8"))?;

    let namespace = NamespaceName::parse(namespace).map_err(|err| {
        Status::invalid_argument(format!("malformed namespace metadata: {}", err))
    })?;

    Ok(namespace)
}

fn validate_logical_plan(plan: &LogicalPlan) -> Result<(), FlightServerError> {
    let verifier = SQLOptions::default()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false);
    verifier.verify_plan(&plan).map_err(FlightServerError::from)
}
