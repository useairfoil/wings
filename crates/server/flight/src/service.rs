use std::{collections::HashSet, sync::Arc, time::Duration};

use arrow_flight::{
    FlightDescriptor, FlightEndpoint, FlightInfo, PutResult, Ticket,
    decode::{DecodedPayload, FlightDataDecoder},
    encode::FlightDataEncoderBuilder,
    error::FlightError,
    flight_service_server::{FlightService, FlightServiceServer},
    sql::{
        Any, CommandGetCatalogs, CommandGetDbSchemas, CommandGetTables, CommandStatementQuery,
        ProstMessageExt, SqlInfo, TicketStatementQuery,
        server::{FlightSqlService, PeekableFlightDataStream},
    },
};
use datafusion::{
    arrow::datatypes::Schema as ArrowSchema,
    error::DataFusionError,
    logical_expr::LogicalPlan,
    prelude::{SQLOptions, SessionContext},
};
use futures::{StreamExt, TryStreamExt};
use prost::Message;
use tokio::{sync::mpsc, task::JoinSet};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, metadata::MetadataMap};
use tracing::debug;
use wings_meta_db::ClusterStore;
use wings_resources::{NamespaceName, PartitionValue, Table, TableName};
use wings_schema::schema_without_partition_field;

use crate::{
    error::Error,
    query::NamespaceProvider,
    ticket::{
        IngestionOperation, IngestionRequestMetadata, IngestionResponseMetadata,
        StatementQueryTicket,
    },
};

pub const WINGS_FLIGHT_SQL_NAMESPACE_HEADER: &str = "x-wings-namespace";

pub struct ClusterFlightService {
    cluster_store: ClusterStore,
}

impl ClusterFlightService {
    pub fn new(cluster_store: ClusterStore) -> Self {
        Self { cluster_store }
    }

    pub fn into_tonic_server(self) -> FlightServiceServer<Self> {
        FlightServiceServer::new(self)
    }

    async fn new_session_context(
        &self,
        name: NamespaceName,
    ) -> Result<SessionContext, DataFusionError> {
        let provider = NamespaceProvider::init(self.cluster_store.clone(), name).await?;
        let ctx = provider.new_session_context().await?;
        Ok(ctx)
    }
}

#[tonic::async_trait]
impl FlightSqlService for ClusterFlightService {
    type FlightService = ClusterFlightService;

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}

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
            .map_err(Error::from)?
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);

        Ok(Response::new(flight_info))
    }

    async fn do_get_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let namespace_name = get_namespace_from_headers(request.metadata())?;
        let ctx = self
            .new_session_context(namespace_name)
            .await
            .map_err(Error::from)?;

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
            .map_err(Error::from)?
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);

        Ok(Response::new(flight_info))
    }

    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let namespace_name = get_namespace_from_headers(request.metadata())?;
        let ctx = self
            .new_session_context(namespace_name)
            .await
            .map_err(Error::from)?;

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
            .map_err(Error::from)?
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);

        Ok(Response::new(flight_info))
    }

    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let namespace_name = get_namespace_from_headers(request.metadata())?;
        let ctx = self
            .new_session_context(namespace_name)
            .await
            .map_err(Error::from)?;

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
                    .map_err(Error::from)?
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
                        .map_err(Error::from)?;
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

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let namespace_name = get_namespace_from_headers(request.metadata())?;
        let ctx = self
            .new_session_context(namespace_name)
            .await
            .map_err(Error::from)?;

        let plan = ctx
            .state()
            .create_logical_plan(&query.query)
            .await
            .map_err(Error::from)?;

        validate_logical_plan(&plan)?;

        let ticket = StatementQueryTicket::new(query.query).into_ticket();

        let flight_descriptor = request.into_inner();
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let flight_info = FlightInfo::new()
            .try_with_schema(plan.schema().as_arrow())
            .map_err(Error::from)?
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);

        Ok(Response::new(flight_info))
    }

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
            .map_err(Error::from)?;

        let out = ctx.sql(&ticket.query).await.map_err(Error::from)?;

        let schema: Arc<_> = out.schema().as_arrow().clone().into();

        let stream = out.execute_stream().await.map_err(Error::from)?;

        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(stream.map_err(|err| FlightError::from_external_error(Box::new(err))))
            .map_err(Status::from)
            .boxed();

        Ok(Response::new(stream))
    }

    async fn do_get_fallback(
        &self,
        _request: Request<Ticket>,
        _message: Any,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        todo!()
    }

    async fn do_put_fallback(
        &self,
        request: Request<PeekableFlightDataStream>,
        _command: Any,
    ) -> Result<Response<<Self as FlightService>::DoPutStream>, Status> {
        let namespace_name = get_namespace_from_headers(request.metadata())?;
        let cluster_store = self.cluster_store.clone();
        let stream = request
            .into_inner()
            .map_err(|status| FlightError::Tonic(Box::new(status)));
        let mut decoded = FlightDataDecoder::new(stream);
        let (tx, rx) = mpsc::channel(32);

        tokio::spawn(async move {
            let mut responses = JoinSet::new();
            let mut pending_response = None;
            let mut skip_record_batches = false;

            loop {
                tokio::select! {
                    joined = responses.join_next(), if !responses.is_empty() => {
                        let Some(joined) = joined else {
                            continue;
                        };

                        if !send_ingestion_response(joined, &tx).await {
                            return;
                        }
                    }
                    maybe_data = decoded.next() => {
                        let Some(data) = maybe_data else {
                            break;
                        };

                        let data = match data {
                            Ok(data) => data,
                            Err(err) => {
                                let _ = tx.send(Err(Status::from(err))).await;
                                return;
                            }
                        };

                        match data.payload {
                            DecodedPayload::Schema(schema) => {
                                if let Some(response) = pending_response.take() {
                                    responses.spawn(delayed_ingestion_response(response));
                                }

                                let response = match validate_ingestion_request(
                                    cluster_store.clone(),
                                    namespace_name.clone(),
                                    data.inner.flight_descriptor.as_ref(),
                                    data.inner.app_metadata.as_ref(),
                                    schema.as_ref(),
                                )
                                .await
                                {
                                    Ok(response) => response,
                                    Err(status) => {
                                        if !drain_ingestion_responses(&mut responses, &tx).await {
                                            return;
                                        }
                                        let _ = tx.send(Err(status)).await;
                                        return;
                                    }
                                };

                                skip_record_batches = !response.accepted;
                                pending_response = Some(response);
                            }
                            DecodedPayload::RecordBatch(_) => {
                                if skip_record_batches {
                                    debug!("skipping rejected ingestion record batch");
                                }
                            }
                            DecodedPayload::None => {}
                        }
                    }
                }
            }

            if let Some(response) = pending_response.take() {
                responses.spawn(delayed_ingestion_response(response));
            }

            drain_ingestion_responses(&mut responses, &tx).await;
        });

        Ok(Response::new(ReceiverStream::new(rx).boxed()))
    }
}

async fn drain_ingestion_responses(
    responses: &mut JoinSet<Result<PutResult, Status>>,
    tx: &mpsc::Sender<Result<PutResult, Status>>,
) -> bool {
    while let Some(joined) = responses.join_next().await {
        if !send_ingestion_response(joined, tx).await {
            return false;
        }
    }

    true
}

async fn send_ingestion_response(
    joined: Result<Result<PutResult, Status>, tokio::task::JoinError>,
    tx: &mpsc::Sender<Result<PutResult, Status>>,
) -> bool {
    let result = match joined {
        Ok(result) => result,
        Err(err) => Err(Status::internal(format!(
            "ingestion response task failed: {err}"
        ))),
    };
    let should_continue = result.is_ok();

    if tx.send(result).await.is_err() {
        return false;
    }

    should_continue
}

async fn delayed_ingestion_response(
    response: IngestionResponseMetadata,
) -> Result<PutResult, Status> {
    let delay_ms = rand::random_range(10..250);
    tokio::time::sleep(Duration::from_millis(delay_ms)).await;

    let app_metadata = response.encode_to_vec();

    Ok(PutResult {
        app_metadata: app_metadata.into(),
    })
}

async fn validate_ingestion_request(
    cluster_store: ClusterStore,
    namespace_name: NamespaceName,
    descriptor: Option<&FlightDescriptor>,
    app_metadata: &[u8],
    schema: &ArrowSchema,
) -> Result<IngestionResponseMetadata, Status> {
    let metadata = IngestionRequestMetadata::try_decode(app_metadata).map_err(|err| {
        Status::invalid_argument(format!("failed to decode ingestion metadata: {err}"))
    })?;

    let operation = metadata
        .checked_operation()
        .map_err(Status::invalid_argument)?;
    let partition_value = metadata.partition_value().map_err(|err| {
        Status::invalid_argument(format!("failed to decode ingestion partition value: {err}"))
    })?;

    let table_id = table_id_from_descriptor(descriptor)?;
    let table_name = match TableName::new(table_id, namespace_name.clone()) {
        Ok(table_name) => table_name,
        Err(err) => {
            return Ok(IngestionResponseMetadata::rejected(
                metadata.request_id,
                format!("invalid table name: {err}"),
            ));
        }
    };

    let table_store = match cluster_store.namespace(namespace_name).table(table_name) {
        Ok(table_store) => table_store,
        Err(err) => {
            return Ok(IngestionResponseMetadata::rejected(
                metadata.request_id,
                err.to_string(),
            ));
        }
    };

    let table = match table_store.load().await {
        Ok(table) => table.table(),
        Err(err) => {
            return Ok(IngestionResponseMetadata::rejected(
                metadata.request_id,
                format!("table not found: {err}"),
            ));
        }
    };

    if let Err(message) = validate_partition_value(&table, partition_value.as_ref()) {
        return Ok(IngestionResponseMetadata::rejected(
            metadata.request_id,
            message,
        ));
    }

    let expected_schema = expected_ingestion_schema(&table, operation);
    if schema != &expected_schema {
        return Ok(IngestionResponseMetadata::rejected(
            metadata.request_id,
            format!("schema mismatch: expected {expected_schema:?}, got {schema:?}"),
        ));
    }

    Ok(IngestionResponseMetadata::accepted(metadata.request_id))
}

fn table_id_from_descriptor(descriptor: Option<&FlightDescriptor>) -> Result<String, Status> {
    let descriptor = descriptor.ok_or_else(|| {
        Status::invalid_argument("missing flight descriptor on ingestion schema message")
    })?;

    let [table_id] = descriptor.path.as_slice() else {
        return Err(Status::invalid_argument(
            "ingestion flight descriptor path must contain exactly one table name",
        ));
    };

    Ok(table_id.clone())
}

fn validate_partition_value(
    table: &Table,
    partition_value: Option<&PartitionValue>,
) -> Result<(), String> {
    match (table.partition_field_data_type(), partition_value) {
        (None, None) => Ok(()),
        (None, Some(_)) => Err("table is not partitioned but partition value was provided".into()),
        (Some(_), None) => Err("table is partitioned but partition value is missing".into()),
        (Some(data_type), Some(partition_value)) => {
            let partition_value_data_type = partition_value.data_type();
            if data_type == &partition_value_data_type {
                Ok(())
            } else {
                Err(format!(
                    "partition value type mismatch: expected {data_type}, got {partition_value_data_type}"
                ))
            }
        }
    }
}

fn expected_ingestion_schema(table: &Table, operation: IngestionOperation) -> ArrowSchema {
    match operation {
        IngestionOperation::Unspecified => unreachable!("ingestion operation is validated"),
        IngestionOperation::Upsert => {
            schema_without_partition_field(table.schema(), table.partition_field_id).arrow_schema()
        }
        IngestionOperation::Delete => ArrowSchema::new(vec![
            table.key_field().to_arrow_field(),
            table.version_field().to_arrow_field(),
        ]),
    }
}

#[allow(clippy::result_large_err)]
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

fn validate_logical_plan(plan: &LogicalPlan) -> Result<(), Error> {
    let verifier = SQLOptions::default()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false);
    verifier.verify_plan(plan).map_err(Error::from)
}
