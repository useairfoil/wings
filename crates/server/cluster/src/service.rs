use std::sync::Arc;

use tonic::{Request, Response, Status};
use wings_meta_db::NamespaceStore;
use wings_resources::{Namespace, NamespaceName, NamespaceOptions};
use wings_secret_manager::SecretManager;

use crate::pb;

pub struct ClusterService {
    namespace_store: NamespaceStore,
    secret_manager: Arc<dyn SecretManager>,
}

impl ClusterService {
    pub fn new(namespace_store: NamespaceStore, secret_manager: Arc<dyn SecretManager>) -> Self {
        Self {
            namespace_store,
            secret_manager,
        }
    }

    pub fn into_tonic_server(self) -> pb::cluster_service_server::ClusterServiceServer<Self> {
        pb::cluster_service_server::ClusterServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl pb::cluster_service_server::ClusterService for ClusterService {
    async fn create_namespace(
        &self,
        request: Request<pb::CreateNamespaceRequest>,
    ) -> Result<Response<pb::Namespace>, Status> {
        let request = request.into_inner();
        let name = NamespaceName::new(request.namespace_id).map_err(invalid_request)?;
        let options: NamespaceOptions = request
            .namespace
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing field: namespace"))?
            .try_into()
            .map_err(invalid_request)?;
        let namespace = Namespace {
            name: name.clone(),
            object_store: options.object_store.clone(),
            lake: options.lake.clone(),
        };

        self.namespace_store
            .create_namespace(self.secret_manager.clone(), name, options)
            .await
            .map_err(meta_db_status)?;

        Ok(Response::new((&namespace.into_redacted()).into()))
    }

    async fn update_namespace(
        &self,
        _request: Request<pb::UpdateNamespaceRequest>,
    ) -> Result<Response<pb::Namespace>, Status> {
        todo!()
    }

    async fn get_namespace(
        &self,
        request: Request<pb::GetNamespaceRequest>,
    ) -> Result<Response<pb::Namespace>, Status> {
        let name = NamespaceName::parse(&request.into_inner().name).map_err(invalid_request)?;
        let namespace = self
            .namespace_store
            .get_namespace(self.secret_manager.clone(), &name)
            .await
            .map_err(meta_db_status)?;

        Ok(Response::new((&namespace.into_redacted()).into()))
    }

    async fn list_namespaces(
        &self,
        request: Request<pb::ListNamespacesRequest>,
    ) -> Result<Response<pb::ListNamespacesResponse>, Status> {
        let request = request.into_inner();
        let page_size = request
            .page_size
            .map(usize::try_from)
            .transpose()
            .map_err(invalid_request)?;
        let result = self
            .namespace_store
            .list_namespace_names(page_size, request.page_token)
            .await
            .map_err(meta_db_status)?;
        let namespaces = result
            .names
            .into_iter()
            .map(|name| pb::Namespace {
                name: name.to_string(),
                object_store: None,
                lake: None,
            })
            .collect();

        Ok(Response::new(pb::ListNamespacesResponse {
            namespaces,
            next_page_token: result.next_page_token,
        }))
    }

    async fn delete_namespace(
        &self,
        request: Request<pb::DeleteNamespaceRequest>,
    ) -> Result<Response<()>, Status> {
        let name = NamespaceName::parse(&request.into_inner().name).map_err(invalid_request)?;
        self.namespace_store
            .delete_namespace(&name)
            .await
            .map_err(meta_db_status)?;

        Ok(Response::new(()))
    }

    async fn create_table(
        &self,
        _request: Request<pb::CreateTableRequest>,
    ) -> Result<Response<pb::Table>, Status> {
        todo!()
    }

    async fn get_table(
        &self,
        _request: Request<pb::GetTableRequest>,
    ) -> Result<Response<pb::Table>, Status> {
        todo!()
    }

    async fn list_tables(
        &self,
        _request: Request<pb::ListTablesRequest>,
    ) -> Result<Response<pb::ListTablesResponse>, Status> {
        todo!()
    }

    async fn delete_table(
        &self,
        _request: Request<pb::DeleteTableRequest>,
    ) -> Result<Response<()>, Status> {
        todo!()
    }
}

fn invalid_request(err: impl std::fmt::Display) -> Status {
    Status::invalid_argument(err.to_string())
}

fn meta_db_status(err: wings_meta_db::Error) -> Status {
    let message = err.to_string();
    match &err {
        wings_meta_db::Error::AlreadyExists { .. } => Status::already_exists(message),
        wings_meta_db::Error::NotFound { .. } => Status::not_found(message),
        wings_meta_db::Error::Decode { .. } => Status::internal(message),
        wings_meta_db::Error::SecretManager { .. } | wings_meta_db::Error::ObjectStore { .. } => {
            Status::unknown(message)
        }
    }
}
