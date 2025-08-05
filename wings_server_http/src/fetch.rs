use std::time::{Duration, Instant};

use arrow::array::RecordBatch;
use axum::{
    Json,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use error_stack::{Report, ResultExt};
use futures::{StreamExt, stream::FuturesOrdered};
use serde_json::Value;
use tokio_util::sync::CancellationToken;
use wings_metadata_core::admin::{NamespaceName, TopicName};
use wings_server_core::{FetchState, Fetcher, ServerError, ServerResult};

use crate::{
    error::{HttpServerError, HttpServerResult},
    types::{ErrorResponse, FetchRequest, FetchResponse, TopicResponseError, TopicResponseSuccess},
};

pub async fn fetch_handler(
    State(fetcher): State<Fetcher>,
    Json(request): Json<FetchRequest>,
) -> impl IntoResponse {
    match process_fetch_request(fetcher, request).await {
        Ok(response) => Json(response).into_response(),
        Err(err) => map_error_to_response(err),
    }
}

async fn process_fetch_request(
    fetcher: Fetcher,
    request: FetchRequest,
) -> HttpServerResult<FetchResponse> {
    let timeout_ms = request.timeout_ms.unwrap_or(500);
    let min_messages = request.min_messages.unwrap_or(1);
    let max_messages = request.max_messages.unwrap_or(10_000);

    let deadline = Instant::now() + Duration::from_millis(timeout_ms);

    let state = FetchState::new(min_messages, max_messages, deadline)
        .map_err(|err| HttpServerError::BadRequest(format!("invalid parameter: {err}")))?;

    let namespace_name = NamespaceName::parse(&request.namespace).map_err(|err| {
        HttpServerError::BadRequest(format!(
            "invalid namespace format: {} {err}",
            request.namespace,
        ))
    })?;

    let ct = CancellationToken::new();
    let _drop_guard = ct.child_token().drop_guard();

    let mut fetches = FuturesOrdered::new();

    for topic_request in request.topics {
        let topic_name =
            TopicName::new(&topic_request.topic, namespace_name.clone()).map_err(|err| {
                HttpServerError::BadRequest(format!(
                    "invalid topic name: {} {err}",
                    topic_request.topic
                ))
            })?;

        let fetcher = fetcher.clone();
        let state = state.clone();
        let ct = ct.clone();

        fetches.push_back(async move {
            let result = fetcher
                .fetch(
                    topic_name.clone(),
                    topic_request.partition_value.clone(),
                    topic_request.offset,
                    state,
                    ct,
                )
                .await;

            (topic_name, topic_request.partition_value, result)
        });
    }

    let mut topics = Vec::with_capacity(fetches.len());

    while let Some((topic_name, partition_value, fetch_result)) = fetches.next().await {
        match fetch_result {
            Ok(topic) => {
                assert_eq!(topic_name, topic.topic);

                match serialize_arrow_to_json(topic.batch) {
                    Ok(messages) => {
                        topics.push(
                            TopicResponseSuccess {
                                topic: topic.topic.to_string(),
                                partition_value: topic.partition,
                                start_offset: topic.start_offset,
                                end_offset: topic.end_offset,
                                messages,
                            }
                            .into(),
                        );
                    }
                    Err(err) => {
                        topics.push(
                            TopicResponseError {
                                topic: topic_name.to_string(),
                                partition_value,
                                message: err.to_string(),
                            }
                            .into(),
                        );
                    }
                }
            }
            Err(err) => topics.push(
                TopicResponseError {
                    topic: topic_name.to_string(),
                    partition_value,
                    message: err.to_string(),
                }
                .into(),
            ),
        }
    }

    Ok(FetchResponse { topics })
}

fn map_error_to_response(error: Report<HttpServerError>) -> Response {
    let status_code = match error.current_context() {
        HttpServerError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        HttpServerError::BadRequest(_) => StatusCode::BAD_REQUEST,
        HttpServerError::NotFound(_) => StatusCode::NOT_FOUND,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };

    let response = Json(ErrorResponse {
        message: error.to_string(),
    });

    (status_code, response).into_response()
}

/// Serialize an Arrow RecordBatch to JSON.
///
/// This function writes the batch to a temporary buffer and then parses it back into a Vec<Value>.
/// This is not efficient at all, but for now it's good enough.
///
/// High-performance applications should use the Arrow Flight protocol.
fn serialize_arrow_to_json(batch: RecordBatch) -> ServerResult<Vec<Value>> {
    let buf = Vec::new();
    let mut writer = arrow_json::ArrayWriter::new(buf);
    writer
        .write_batches(&[&batch])
        .change_context(ServerError::DataFusionError)?;
    writer
        .finish()
        .change_context(ServerError::DataFusionError)?;

    let json_data = writer.into_inner();

    let json_rows: Vec<Value> = serde_json::from_reader(json_data.as_slice())
        .change_context(ServerError::DataFusionError)?;

    Ok(json_rows)
}
