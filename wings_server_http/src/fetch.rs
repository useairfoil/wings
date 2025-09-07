use axum::{Json, response::IntoResponse};

use crate::{
    error::Result,
    types::{FetchRequest, FetchResponse},
};

pub async fn fetch_handler(Json(request): Json<FetchRequest>) -> impl IntoResponse {
    match process_fetch_request(request).await {
        Ok(response) => Json(response).into_response(),
        Err(_err) => todo!(),
    }
}

async fn process_fetch_request(_request: FetchRequest) -> Result<FetchResponse> {
    todo!();
}
