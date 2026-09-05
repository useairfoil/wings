//! Iceberg REST catalog endpoints, relative to `/catalogs/{id}`.

mod config;

use axum::{
    Json, Router,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use iceberg_catalog_rest::ErrorModel;
use serde::Serialize;
use wings_meta_store::catalog::{CatalogStore, Error as StoreError};

#[derive(Debug, thiserror::Error)]
pub(super) enum Error {
    #[error("invalid catalog id")]
    InvalidId,
    #[error("catalog not found")]
    NotFound,
    #[error("{0}")]
    Store(#[from] StoreError),
}

#[derive(Serialize)]
struct ErrorResponse {
    error: ErrorModel,
}

pub(super) fn router() -> Router<CatalogStore> {
    Router::new().route("/v1/config", get(config::get))
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let (status, error_type, message) = match self {
            Self::InvalidId => (
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "invalid catalog id",
            ),
            Self::NotFound => (
                StatusCode::NOT_FOUND,
                "NoSuchCatalogException",
                "catalog not found",
            ),
            // Never expose upstream credentials or backend details to clients.
            Self::Store(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalServerError",
                "internal server error",
            ),
        };
        (
            status,
            Json(ErrorResponse {
                error: ErrorModel {
                    message: message.to_owned(),
                    r#type: error_type.to_owned(),
                    code: status.as_u16(),
                    stack: None,
                },
            }),
        )
            .into_response()
    }
}
