use tonic::{Request, Response, Status};
use wings_grpc_common::pb;
use wings_meta_store::catalog::{
    CatalogConfig, CatalogName, CatalogStore, Error as CatalogStoreError, NamespaceIdent,
    RestCatalogConfig, StoredCatalog, TableIdent,
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
        let catalog = self.catalog(name).await?;

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

    async fn link_table(
        &self,
        request: Request<pb::LinkTableRequest>,
    ) -> Result<Response<pb::LinkTableResponse>, Status> {
        let request = request.into_inner();
        let name = parse_catalog_name(&request.catalog)?;
        let table_ident = parse_table_identifier(request.table)?;
        let catalog = self.catalog(name).await?;

        let table = catalog
            .link_table(table_ident)
            .await
            .map_err(catalog_store_error_to_status)?;

        Ok(Response::new(pb::LinkTableResponse {
            table_uuid: table.metadata().uuid.to_string(),
        }))
    }

    async fn unlink_table(
        &self,
        request: Request<pb::UnlinkTableRequest>,
    ) -> Result<Response<pb::UnlinkTableResponse>, Status> {
        let request = request.into_inner();
        let name = parse_catalog_name(&request.catalog)?;
        let table_ident = parse_table_identifier(request.table)?;
        let catalog = self.catalog(name).await?;

        catalog
            .unlink_table(table_ident)
            .await
            .map_err(catalog_store_error_to_status)?;

        Ok(Response::new(pb::UnlinkTableResponse {}))
    }

    async fn load_table(
        &self,
        request: Request<pb::LoadTableRequest>,
    ) -> Result<Response<pb::LoadTableResponse>, Status> {
        let request = request.into_inner();
        let name = parse_catalog_name(&request.catalog)?;
        let table_ident = parse_table_identifier(request.table)?;
        let catalog = self.catalog(name).await?;

        let table = catalog
            .try_load_table(table_ident.clone())
            .await
            .map_err(catalog_store_error_to_status)?
            .ok_or_else(|| Status::not_found(format!("table not found: {table_ident}")))?;

        Ok(Response::new(pb::LoadTableResponse {
            table_uuid: table.metadata().uuid.to_string(),
        }))
    }
}

impl CatalogService {
    /// Returns the catalog with the given name.
    ///
    /// Returns a `not_found` status if the catalog does not exist.
    async fn catalog(&self, name: CatalogName) -> Result<StoredCatalog, Status> {
        self.catalog_store
            .get(name.clone())
            .await
            .map_err(catalog_store_error_to_status)?
            .ok_or_else(|| Status::not_found(format!("catalog not found: {name}")))
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

/// Parses the table identifier of a link or unlink request into a [`TableIdent`].
fn parse_table_identifier(table: Option<pb::TableIdentifier>) -> Result<TableIdent, Status> {
    let table = table.ok_or_else(|| Status::invalid_argument("missing table identifier"))?;
    if table.name.is_empty() {
        return Err(Status::invalid_argument(
            "invalid table name: name is empty",
        ));
    }
    let namespace = NamespaceIdent::from_vec(table.namespace)
        .map_err(|_| Status::invalid_argument("invalid table namespace: namespace is empty"))?;
    Ok(TableIdent::new(namespace, table.name))
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
pub(crate) fn catalog_store_error_to_status(error: CatalogStoreError) -> Status {
    match &error {
        CatalogStoreError::AlreadyExists(name) => {
            Status::already_exists(format!("catalog already exists: {name}"))
        }
        CatalogStoreError::TableLinkAlreadyExists(table_ident) => {
            Status::already_exists(format!("table link already exists: {table_ident}"))
        }
        CatalogStoreError::TableMetadataMissing(uuid) => {
            Status::not_found(format!("table metadata is missing: {uuid}"))
        }
        _ => Status::internal(error.to_string()),
    }
}
