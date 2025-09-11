use std::collections::HashSet;

use arrow_flight::{
    FlightDescriptor, FlightEndpoint, FlightInfo, Ticket,
    encode::FlightDataEncoderBuilder,
    flight_service_server::{FlightService, FlightServiceServer},
    sql::{
        CommandGetCatalogs, CommandGetDbSchemas, CommandGetTables, ProstMessageExt, SqlInfo,
        server::FlightSqlService,
    },
};
use datafusion::{error::DataFusionError, prelude::SessionContext};
use futures::TryStreamExt;
use prost::Message;
use tonic::{Request, Response, Status, metadata::MetadataMap};
use wings_control_plane::resources::NamespaceName;
use wings_server_core::query::NamespaceProviderFactory;

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
            .map_err(|e| Status::internal(format!("Unable to encode schema {e}")))?
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);

        Ok(tonic::Response::new(flight_info))
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
            .map_err(|_| Status::internal("failed to create session context"))?;

        let mut builder = query.into_builder();
        for name in ctx.catalog_names() {
            builder.append(name);
        }

        let schema = builder.schema();
        let batch = builder.build();
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::once(async { batch }))
            .map_err(Status::from);

        Ok(Response::new(Box::pin(stream)))
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
            .map_err(|e| Status::internal(format!("Unable to encode schema {e}")))?
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);

        Ok(tonic::Response::new(flight_info))
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
            .map_err(|_| Status::internal("failed to create session context"))?;

        let catalog_name = query.catalog().to_string();
        let catalog = ctx.catalog(query.catalog()).unwrap();
        let mut builder = query.into_builder();

        for schema_name in catalog.schema_names() {
            builder.append(&catalog_name, schema_name);
        }

        let schema = builder.schema();
        let batch = builder.build();
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::once(async { batch }))
            .map_err(Status::from);

        Ok(Response::new(Box::pin(stream)))
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
            .map_err(|e| Status::internal(format!("Unable to encode schema {e}")))?
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);

        Ok(tonic::Response::new(flight_info))
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
            .map_err(|_| Status::internal("failed to create session context"))?;

        let table_types: HashSet<String> =
            HashSet::from_iter(query.table_types.iter().map(|s| s.to_uppercase()));

        let catalog_name = query.catalog().to_string();
        let catalog = ctx.catalog(query.catalog()).unwrap();
        let mut builder = query.into_builder();

        for schema_name in catalog.schema_names() {
            let schema_provider = catalog.schema(&schema_name).unwrap();
            for table_name in schema_provider.table_names() {
                let table = schema_provider.table(&table_name).await.unwrap().unwrap();
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
                        .unwrap();
                }
            }
        }

        let schema = builder.schema();
        let batch = builder.build();
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::once(async { batch }))
            .map_err(Status::from);

        Ok(Response::new(Box::pin(stream)))
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
