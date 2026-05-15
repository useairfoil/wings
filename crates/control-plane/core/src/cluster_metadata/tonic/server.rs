use std::sync::Arc;

use tonic::{Request, Response, Status, async_trait};
use wings_resources::{
    DataLakeName, NamespaceName, NamespaceOptions, ObjectStoreConfiguration, ObjectStoreName,
    TenantName, TableName, TableOptions,
};

use crate::{
    ClusterMetadataError,
    cluster_metadata::{
        ClusterMetadata, ListDataLakesRequest, ListNamespacesRequest, ListObjectStoresRequest,
        ListTenantsRequest, ListTablesRequest, Result,
    },
    error::ResourceErrorExt,
    pb::{
        self,
        cluster_metadata_service_server::{
            ClusterMetadataService as TonicService, ClusterMetadataServiceServer as TonicServer,
        },
    },
};

/// A tonic service for managing cluster metadata.
pub struct ClusterMetadataServer {
    inner: Arc<dyn ClusterMetadata>,
}

impl ClusterMetadataServer {
    /// Create a new tonic cluster metadata server.
    pub fn new(inner: Arc<dyn ClusterMetadata>) -> Self {
        Self { inner }
    }

    pub fn into_tonic_server(self) -> TonicServer<Self> {
        TonicServer::new(self)
    }
}

#[async_trait]
impl TonicService for ClusterMetadataServer {
    async fn create_tenant(
        &self,
        request: Request<pb::CreateTenantRequest>,
    ) -> Result<Response<pb::Tenant>, Status> {
        let request = request.into_inner();

        let tenant_name = TenantName::new(request.tenant_id)
            .map_err(|err| err.to_cluster_metadata_error("tenant"))?;

        let tenant = self.inner.create_tenant(tenant_name).await?;

        Ok(Response::new(tenant.into()))
    }

    async fn get_tenant(
        &self,
        request: Request<pb::GetTenantRequest>,
    ) -> Result<Response<pb::Tenant>, Status> {
        let request = request.into_inner();

        let tenant_name = TenantName::parse(&request.name)
            .map_err(|err| err.to_cluster_metadata_error("tenant"))?;

        let tenant = self.inner.get_tenant(tenant_name).await?;

        Ok(Response::new(tenant.into()))
    }

    async fn list_tenants(
        &self,
        request: Request<pb::ListTenantsRequest>,
    ) -> Result<Response<pb::ListTenantsResponse>, Status> {
        let request = ListTenantsRequest::from(request.into_inner());

        let response = self.inner.list_tenants(request).await?;

        Ok(Response::new(response.into()))
    }

    async fn delete_tenant(
        &self,
        request: Request<pb::DeleteTenantRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();

        let tenant_name = TenantName::parse(&request.name)
            .map_err(|err| err.to_cluster_metadata_error("tenant"))?;

        self.inner.delete_tenant(tenant_name).await?;

        Ok(Response::new(()))
    }

    async fn create_namespace(
        &self,
        request: Request<pb::CreateNamespaceRequest>,
    ) -> Result<Response<pb::Namespace>, Status> {
        let request = request.into_inner();

        let tenant_name = TenantName::parse(&request.parent)
            .map_err(|err| err.to_cluster_metadata_error("tenant"))?;

        let namespace_name = NamespaceName::new(request.namespace_id, tenant_name)
            .map_err(|err| err.to_cluster_metadata_error("namespace"))?;

        let options = NamespaceOptions::try_from(request.namespace.unwrap_or_default())
            .map_err(ClusterMetadataError::from)?;

        let namespace = self.inner.create_namespace(namespace_name, options).await?;

        Ok(Response::new(namespace.into()))
    }

    async fn get_namespace(
        &self,
        request: Request<pb::GetNamespaceRequest>,
    ) -> Result<Response<pb::Namespace>, Status> {
        let request = request.into_inner();

        let namespace_name = NamespaceName::parse(&request.name)
            .map_err(|err| err.to_cluster_metadata_error("namespace"))?;

        let namespace = self.inner.get_namespace(namespace_name).await?;

        Ok(Response::new(namespace.into()))
    }

    async fn list_namespaces(
        &self,
        request: Request<pb::ListNamespacesRequest>,
    ) -> Result<Response<pb::ListNamespacesResponse>, Status> {
        let request = request.into_inner();
        let request =
            ListNamespacesRequest::try_from(request).map_err(ClusterMetadataError::from)?;

        let response = self.inner.list_namespaces(request).await?;

        Ok(Response::new(response.into()))
    }

    async fn delete_namespace(
        &self,
        request: Request<pb::DeleteNamespaceRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();

        let namespace_name = NamespaceName::parse(&request.name)
            .map_err(|err| err.to_cluster_metadata_error("namespace"))?;

        self.inner.delete_namespace(namespace_name).await?;

        Ok(Response::new(()))
    }

    async fn create_table(
        &self,
        request: Request<pb::CreateTableRequest>,
    ) -> Result<Response<pb::Table>, Status> {
        let request = request.into_inner();

        let namespace_name = NamespaceName::parse(&request.parent)
            .map_err(|err| err.to_cluster_metadata_error("namespace"))?;

        let table_name = TableName::new(request.table_id, namespace_name)
            .map_err(|err| err.to_cluster_metadata_error("table"))?;

        let options = TableOptions::try_from(request.table.unwrap_or_default())
            .map_err(ClusterMetadataError::from)?;

        let table = self
            .inner
            .create_table(table_name, options)
            .await?
            .try_into()
            .map_err(ClusterMetadataError::from)?;

        Ok(Response::new(table))
    }

    async fn get_table(
        &self,
        request: Request<pb::GetTableRequest>,
    ) -> Result<Response<pb::Table>, Status> {
        let request = request.into_inner();

        let table_name = TableName::parse(&request.name)
            .map_err(|err| err.to_cluster_metadata_error("table"))?;

        let view = request.view().into();

        let table = self
            .inner
            .get_table(table_name, view)
            .await?
            .try_into()
            .map_err(ClusterMetadataError::from)?;

        Ok(Response::new(table))
    }

    async fn list_tables(
        &self,
        request: Request<pb::ListTablesRequest>,
    ) -> Result<Response<pb::ListTablesResponse>, Status> {
        let request = request.into_inner();

        let request = ListTablesRequest::try_from(request).map_err(ClusterMetadataError::from)?;

        let response = self
            .inner
            .list_tables(request)
            .await?
            .try_into()
            .map_err(ClusterMetadataError::from)?;

        Ok(Response::new(response))
    }

    async fn delete_table(
        &self,
        request: Request<pb::DeleteTableRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();

        let table_name = TableName::parse(&request.name)
            .map_err(|err| err.to_cluster_metadata_error("table"))?;

        self.inner.delete_table(table_name, request.force).await?;

        Ok(Response::new(()))
    }

    async fn create_object_store(
        &self,
        request: Request<pb::CreateObjectStoreRequest>,
    ) -> Result<Response<pb::ObjectStore>, Status> {
        let request = request.into_inner();

        let tenant_name = TenantName::parse(&request.parent)
            .map_err(|err| err.to_cluster_metadata_error("tenant"))?;

        let object_store_name = ObjectStoreName::new(request.object_store_id, tenant_name)
            .map_err(|err| err.to_cluster_metadata_error("object store"))?;

        let object_store_config =
            ObjectStoreConfiguration::try_from(request.object_store.unwrap_or_default())
                .map_err(ClusterMetadataError::from)?;

        let object_store = self
            .inner
            .create_object_store(object_store_name, object_store_config)
            .await?;

        Ok(Response::new(object_store.into()))
    }

    async fn get_object_store(
        &self,
        request: Request<pb::GetObjectStoreRequest>,
    ) -> Result<Response<pb::ObjectStore>, Status> {
        let request = request.into_inner();

        let object_store_name = ObjectStoreName::parse(&request.name)
            .map_err(|err| err.to_cluster_metadata_error("object store"))?;

        let object_store = self.inner.get_object_store(object_store_name).await?;

        Ok(Response::new(object_store.into()))
    }

    async fn list_object_stores(
        &self,
        request: Request<pb::ListObjectStoresRequest>,
    ) -> Result<Response<pb::ListObjectStoresResponse>, Status> {
        let request = request.into_inner();
        let request =
            ListObjectStoresRequest::try_from(request).map_err(ClusterMetadataError::from)?;

        let response = self.inner.list_object_stores(request).await?;

        Ok(Response::new(response.into()))
    }

    async fn delete_object_store(
        &self,
        request: Request<pb::DeleteObjectStoreRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();

        let object_store_name = ObjectStoreName::parse(&request.name)
            .map_err(|err| err.to_cluster_metadata_error("object store"))?;

        self.inner.delete_object_store(object_store_name).await?;

        Ok(Response::new(()))
    }

    async fn create_data_lake(
        &self,
        request: Request<pb::CreateDataLakeRequest>,
    ) -> Result<Response<pb::DataLake>, Status> {
        let request = request.into_inner();

        let (data_lake_name, data_lake_config) =
            request.try_into().map_err(ClusterMetadataError::from)?;

        let data_lake = self
            .inner
            .create_data_lake(data_lake_name, data_lake_config)
            .await?;

        Ok(Response::new(data_lake.into()))
    }

    async fn get_data_lake(
        &self,
        request: Request<pb::GetDataLakeRequest>,
    ) -> Result<Response<pb::DataLake>, Status> {
        let request = request.into_inner();

        let data_lake_name = DataLakeName::try_from(request).map_err(ClusterMetadataError::from)?;

        let data_lake = self.inner.get_data_lake(data_lake_name).await?;

        Ok(Response::new(data_lake.into()))
    }

    async fn list_data_lakes(
        &self,
        request: Request<pb::ListDataLakesRequest>,
    ) -> Result<Response<pb::ListDataLakesResponse>, Status> {
        let request = request.into_inner();
        let request =
            ListDataLakesRequest::try_from(request).map_err(ClusterMetadataError::from)?;

        let response = self.inner.list_data_lakes(request).await?;

        Ok(Response::new(response.into()))
    }

    async fn delete_data_lake(
        &self,
        request: Request<pb::DeleteDataLakeRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();

        let data_lake_name = DataLakeName::try_from(request).map_err(ClusterMetadataError::from)?;

        self.inner.delete_data_lake(data_lake_name).await?;

        Ok(Response::new(()))
    }
}
