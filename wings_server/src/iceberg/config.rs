use std::collections::HashMap;

use axum::{
    Json,
    extract::{Path, State},
};
use serde::Serialize;
use wings_meta_store::catalog::{CatalogName, CatalogStore};

use super::Error;

#[derive(Default, Serialize)]
pub(super) struct ConfigResponse {
    defaults: HashMap<String, String>,
    overrides: HashMap<String, String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    endpoints: Vec<String>,
}

pub(super) async fn get(
    State(store): State<CatalogStore>,
    Path(id): Path<String>,
) -> Result<Json<ConfigResponse>, Error> {
    let name = CatalogName::new(id).map_err(|_| Error::InvalidId)?;
    store.get(name.clone()).await?.ok_or(Error::NotFound)?;

    let mut overrides = HashMap::new();
    overrides.insert("prefix".to_string(), name.id().to_string());

    Ok(Json(ConfigResponse {
        defaults: HashMap::new(),
        overrides,
        endpoints: Vec::new(),
    }))
}
