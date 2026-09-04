use tonic::{Request, Response, Status};
use wings_grpc_common::pb;
use wings_meta_store::catalog::{
    CatalogConfig, CatalogName, CatalogStore, Error as CatalogStoreError, RestCatalogConfig,
    StoredCatalog,
};

#[derive(Debug)]
pub struct CatalogService {
    catalog_store: CatalogStore,
}

impl CatalogService {
    pub fn new(catalog_store: CatalogStore) -> Self {
        Self { catalog_store }
    }

    pub fn into_service(self) -> pb::catalog_service_server::CatalogServiceServer<Self> {
        pb::catalog_service_server::CatalogServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl pb::catalog_service_server::CatalogService for CatalogService {
    async fn create_catalog(
        &self,
        request: Request<pb::CreateCatalogRequest>,
    ) -> Result<Response<pb::CreateCatalogResponse>, Status> {
        let request = request.into_inner();
        let name = parse_catalog_id(&request.catalog_id)?;
        let config = catalog_config_from_proto(request.config)?;

        self.catalog_store
            .create(name, config)
            .await
            .map_err(catalog_store_error_to_status)?;

        Ok(Response::new(pb::CreateCatalogResponse {}))
    }

    async fn get_catalog(
        &self,
        request: Request<pb::GetCatalogRequest>,
    ) -> Result<Response<pb::GetCatalogResponse>, Status> {
        let request = request.into_inner();
        let name = parse_catalog_name(&request.name)?;

        let catalog = self
            .catalog_store
            .get(name.clone())
            .await
            .map_err(catalog_store_error_to_status)?
            .ok_or_else(|| Status::not_found(format!("catalog not found: {name}")))?;

        let response = pb::GetCatalogResponse {
            catalog: Some(catalog_to_proto(&catalog)),
        };

        Ok(Response::new(response))
    }

    async fn delete_catalog(
        &self,
        request: Request<pb::DeleteCatalogRequest>,
    ) -> Result<Response<pb::DeleteCatalogResponse>, Status> {
        let request = request.into_inner();
        let name = parse_catalog_name(&request.name)?;

        self.catalog_store
            .delete(name)
            .await
            .map_err(catalog_store_error_to_status)?;

        Ok(Response::new(pb::DeleteCatalogResponse {}))
    }
}

/// Parses the catalog name of a request into a [`CatalogName`].
fn parse_catalog_name(name: &str) -> Result<CatalogName, Status> {
    CatalogName::parse(name)
        .map_err(|_| Status::invalid_argument(format!("invalid catalog name: {name}")))
}

/// Parses the catalog id of a create request into a [`CatalogName`].
fn parse_catalog_id(id: &str) -> Result<CatalogName, Status> {
    CatalogName::new(id).map_err(|_| Status::invalid_argument(format!("invalid catalog id: {id}")))
}

/// Converts a proto catalog config into a catalog config.
fn catalog_config_from_proto(config: Option<pb::CatalogConfig>) -> Result<CatalogConfig, Status> {
    match config.and_then(|config| config.config) {
        Some(pb::catalog_config::Config::Rest(rest)) => {
            Ok(CatalogConfig::Rest(RestCatalogConfig {
                uri: rest.uri,
                warehouse: rest.warehouse,
                properties: rest.properties,
            }))
        }
        _ => Err(Status::invalid_argument("missing catalog config")),
    }
}

/// Converts a stored catalog into a proto catalog.
fn catalog_to_proto(catalog: &StoredCatalog) -> pb::Catalog {
    let config = match catalog.config() {
        CatalogConfig::Rest(rest) => pb::catalog_config::Config::Rest(pb::RestCatalogConfig {
            uri: rest.uri.clone(),
            warehouse: rest.warehouse.clone(),
            properties: rest.properties.clone(),
        }),
    };

    pb::Catalog {
        name: catalog.name().to_string(),
        config: Some(pb::CatalogConfig {
            config: Some(config),
        }),
    }
}

/// Maps a catalog store error to a gRPC status.
fn catalog_store_error_to_status(error: CatalogStoreError) -> Status {
    match &error {
        CatalogStoreError::AlreadyExists(name) => {
            Status::already_exists(format!("catalog already exists: {name}"))
        }
        _ => Status::internal(error.to_string()),
    }
}
