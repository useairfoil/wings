//! Arrow Flight SQL ingestion service.

use std::time::Duration;

use arrow_flight::{
    FlightData, PutResult,
    flight_service_server::{FlightService, FlightServiceServer},
    sql::{
        Any, SqlInfo,
        server::{FlightSqlService, PeekableFlightDataStream},
    },
};
use arrow_ipc::MessageHeader;
use serde::{Deserialize, Serialize};
use tokio::{sync::mpsc, task::JoinSet, time::sleep};
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};
use tracing::debug;
use wings_meta_store::catalog::{CatalogName, CatalogStore};

const MAX_ACK_DELAY: Duration = Duration::from_millis(100);

#[derive(Debug, Clone)]
pub struct IngestionService {
    catalog_store: CatalogStore,
    ct: CancellationToken,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SchemaMetadata {
    request_id: u64,
    catalog: String,
    namespace: Vec<String>,
    table_name: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RequestMetadata {
    request_id: u64,
}

#[derive(Debug, Serialize)]
struct AckMetadata {
    request_id: u64,
}

pub fn service(
    catalog_store: CatalogStore,
    ct: CancellationToken,
) -> FlightServiceServer<IngestionService> {
    FlightServiceServer::new(IngestionService { catalog_store, ct })
}

#[tonic::async_trait]
impl FlightSqlService for IngestionService {
    type FlightService = Self;

    async fn do_put_fallback(
        &self,
        request: Request<PeekableFlightDataStream>,
        _message: Any,
    ) -> Result<Response<<Self as FlightService>::DoPutStream>, Status> {
        let mut input = request.into_inner();

        let schema_message = tokio::select! {
            _ = self.ct.cancelled() => return Err(Status::cancelled("server shutting down")),
            message = input.next() => message
                .ok_or_else(|| Status::invalid_argument("schema message is required"))??,
        };
        let schema_metadata = parse_schema_metadata(&schema_message)?;

        debug!(
            request_id = schema_metadata.request_id,
            catalog = %schema_metadata.catalog,
            namespace = ?schema_metadata.namespace,
            table_name = %schema_metadata.table_name,
            "ingestion started"
        );

        tokio::select! {
            _ = self.ct.cancelled() => return Err(Status::cancelled("server shutting down")),
            result = load_catalog(&self.catalog_store, &schema_metadata.catalog) => result?,
        }

        let (sender, receiver) = mpsc::channel(32);

        tokio::select! {
            _ = self.ct.cancelled() => return Err(Status::cancelled("server shutting down")),
            _ = sleep(rand::random_range(Duration::ZERO..=MAX_ACK_DELAY)) => {},
        }

        let request_id = schema_metadata.request_id;
        sender
            .send(Ok(acknowledgement(request_id)?))
            .await
            .map_err(|_| Status::internal("acknowledgement stream closed"))?;

        debug!(
            response_type = "PutResult",
            request_id, "ingestion response sent"
        );

        tokio::spawn(process_messages(input, sender, self.ct.clone()));

        let output: <Self as FlightService>::DoPutStream = Box::pin(ReceiverStream::new(receiver));

        Ok(Response::new(output))
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

async fn load_catalog(catalog_store: &CatalogStore, catalog: &str) -> Result<(), Status> {
    let name =
        CatalogName::new(catalog).map_err(|_| Status::invalid_argument("invalid catalog id"))?;

    catalog_store
        .get(name)
        .await
        .map_err(|_| Status::internal("failed to load catalog"))?
        .ok_or_else(|| Status::not_found("catalog not found"))?;

    Ok(())
}

async fn process_messages(
    mut input: PeekableFlightDataStream,
    sender: mpsc::Sender<Result<PutResult, Status>>,
    ct: CancellationToken,
) {
    let mut tasks = JoinSet::new();
    let mut input_done = false;

    loop {
        if input_done && tasks.is_empty() {
            break;
        }

        tokio::select! {
            _ = ct.cancelled() => break,
            message = input.next(), if !input_done => match message {
                Some(Ok(message)) => {
                    let request_id = match parse_request_metadata(&message) {
                        Ok(request_id) => request_id,
                        Err(error) => {
                            let _ = sender.send(Err(error)).await;
                            return;
                        }
                    };

                    debug!(request_id, "ingestion message received");

                    tasks.spawn(async move {
                        sleep(rand::random_range(Duration::ZERO..=MAX_ACK_DELAY)).await;
                        request_id
                    });
                }

                Some(Err(error)) => {
                    let _ = sender.send(Err(error)).await;
                    return;
                }
                None => input_done = true,
            },
            result = tasks.join_next(), if !tasks.is_empty() => {
                let request_id = match result {
                    Some(Ok(request_id)) => request_id,
                    Some(Err(_)) => {
                        let _ = sender
                            .send(Err(Status::internal("acknowledgement task failed")))
                            .await;
                        return;
                    }
                    None => continue,
                };

                let response = match acknowledgement(request_id) {
                    Ok(response) => response,
                    Err(error) => {
                        let _ = sender.send(Err(error)).await;
                        return;
                    }
                };

                if sender.send(Ok(response)).await.is_err() {
                    return;
                }

                debug!(response_type = "PutResult", request_id, "ingestion response sent");
            },
        }
    }
}

fn parse_schema_metadata(message: &FlightData) -> Result<SchemaMetadata, Status> {
    let ipc_message = arrow_ipc::root_as_message(&message.data_header)
        .map_err(|_| Status::invalid_argument("first message must be a schema message"))?;
    if ipc_message.header_type() != MessageHeader::Schema {
        return Err(Status::invalid_argument(
            "first message must be a schema message",
        ));
    }

    let metadata: SchemaMetadata = serde_json::from_slice(&message.app_metadata)
        .map_err(|_| Status::invalid_argument("invalid schema app metadata"))?;
    if metadata.request_id != 0 {
        return Err(Status::invalid_argument("schema request_id must be zero"));
    }

    Ok(metadata)
}

fn parse_request_metadata(message: &FlightData) -> Result<u64, Status> {
    let metadata: RequestMetadata = serde_json::from_slice(&message.app_metadata)
        .map_err(|_| Status::invalid_argument("invalid app metadata"))?;
    if metadata.request_id == 0 {
        return Err(Status::invalid_argument(
            "request_id zero is reserved for the schema message",
        ));
    }
    Ok(metadata.request_id)
}

fn acknowledgement(request_id: u64) -> Result<PutResult, Status> {
    let app_metadata = serde_json::to_vec(&AckMetadata { request_id })
        .map_err(|_| Status::internal("failed to serialize acknowledgement"))?
        .into();

    Ok(PutResult { app_metadata })
}
