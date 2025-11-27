use std::marker::Send;

use async_trait::async_trait;
use bytes::Bytes;
use http_body::Body;

use crate::{
    cluster_metadata::{
        ClusterMetadata, ClusterMetadataError, ListCredentialsRequest, ListCredentialsResponse,
        ListNamespacesRequest, ListNamespacesResponse, ListTenantsRequest, ListTenantsResponse,
        ListTopicsRequest, ListTopicsResponse, Result,
    },
    resources::{
        Credential, CredentialName, Namespace, NamespaceName, NamespaceOptions, Tenant, TenantName,
        Topic, TopicName, TopicOptions,
    },
};

use super::pb::{
    self, cluster_metadata_service_client::ClusterMetadataServiceClient as TonicClient,
};

type StdError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug, Clone)]
pub struct ClusterMetadataClient<T> {
    client: TonicClient<T>,
}

impl<T> ClusterMetadataClient<T>
where
    T: tonic::client::GrpcService<tonic::body::Body> + Clone,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
{
    /// Create a new remote cluster metadata service with the given transport.
    pub fn new(inner: T) -> Self {
        Self::new_with_client(TonicClient::new(inner))
    }

    /// Create a new remote cluster metadata service with the given client.
    pub fn new_with_client(client: TonicClient<T>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl<T> ClusterMetadata for ClusterMetadataClient<T>
where
    T: tonic::client::GrpcService<tonic::body::Body> + Send + Sync + Clone,
    <T as tonic::client::GrpcService<tonic::body::Body>>::Future: Send,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
{
    async fn create_tenant(&self, name: TenantName) -> Result<Tenant> {
        let request = pb::CreateTenantRequest {
            tenant_id: name.id().to_string(),
            tenant: None,
        };

        self.client
            .clone()
            .create_tenant(request)
            .await
            .map_err(|status| status_to_cluster_metadata_error("tenant", status))?
            .into_inner()
            .try_into()
    }

    async fn get_tenant(&self, name: TenantName) -> Result<Tenant> {
        let request = pb::GetTenantRequest {
            name: name.to_string(),
        };

        self.client
            .clone()
            .get_tenant(request)
            .await
            .map_err(|status| status_to_cluster_metadata_error("tenant", status))?
            .into_inner()
            .try_into()
    }

    async fn list_tenants(&self, request: ListTenantsRequest) -> Result<ListTenantsResponse> {
        let request = pb::ListTenantsRequest::from(request);

        self.client
            .clone()
            .list_tenants(request)
            .await
            .map_err(|status| status_to_cluster_metadata_error("tenant", status))?
            .into_inner()
            .try_into()
    }

    async fn delete_tenant(&self, name: TenantName) -> Result<()> {
        let request = pb::DeleteTenantRequest {
            name: name.to_string(),
        };

        self.client
            .clone()
            .delete_tenant(request)
            .await
            .map_err(|status| status_to_cluster_metadata_error("tenant", status))?;

        Ok(())
    }

    async fn create_namespace(
        &self,
        name: NamespaceName,
        options: NamespaceOptions,
    ) -> Result<Namespace> {
        let request = pb::CreateNamespaceRequest {
            parent: name.parent().to_string(),
            namespace_id: name.id().to_string(),
            namespace: pb::Namespace::from(options).into(),
        };

        self.client
            .clone()
            .create_namespace(request)
            .await
            .map_err(|status| status_to_cluster_metadata_error("namespace", status))?
            .into_inner()
            .try_into()
    }

    async fn get_namespace(&self, name: NamespaceName) -> Result<Namespace> {
        let request = pb::GetNamespaceRequest {
            name: name.to_string(),
        };

        self.client
            .clone()
            .get_namespace(request)
            .await
            .map_err(|status| status_to_cluster_metadata_error("namespace", status))?
            .into_inner()
            .try_into()
    }

    async fn list_namespaces(
        &self,
        request: ListNamespacesRequest,
    ) -> Result<ListNamespacesResponse> {
        let request = pb::ListNamespacesRequest::from(request);

        self.client
            .clone()
            .list_namespaces(request)
            .await
            .map_err(|status| status_to_cluster_metadata_error("namespace", status))?
            .into_inner()
            .try_into()
    }

    async fn delete_namespace(&self, name: NamespaceName) -> Result<()> {
        let request = pb::DeleteNamespaceRequest {
            name: name.to_string(),
        };

        self.client
            .clone()
            .delete_namespace(request)
            .await
            .map_err(|status| status_to_cluster_metadata_error("tenant", status))?;

        Ok(())
    }

    async fn create_topic(&self, name: TopicName, options: TopicOptions) -> Result<Topic> {
        let request = pb::CreateTopicRequest {
            parent: name.parent().to_string(),
            topic_id: name.id().to_string(),
            topic: pb::Topic::from(options).into(),
        };

        self.client
            .clone()
            .create_topic(request)
            .await
            .map_err(|status| status_to_cluster_metadata_error("topic", status))?
            .into_inner()
            .try_into()
    }

    async fn get_topic(&self, name: TopicName) -> Result<Topic> {
        let request = pb::GetTopicRequest {
            name: name.to_string(),
        };

        self.client
            .clone()
            .get_topic(request)
            .await
            .map_err(|status| status_to_cluster_metadata_error("topic", status))?
            .into_inner()
            .try_into()
    }

    async fn list_topics(&self, request: ListTopicsRequest) -> Result<ListTopicsResponse> {
        let request = pb::ListTopicsRequest::from(request);

        self.client
            .clone()
            .list_topics(request)
            .await
            .map_err(|status| status_to_cluster_metadata_error("topic", status))?
            .into_inner()
            .try_into()
    }

    async fn delete_topic(&self, name: TopicName, force: bool) -> Result<()> {
        let request = pb::DeleteTopicRequest {
            name: name.to_string(),
            force,
        };

        self.client
            .clone()
            .delete_topic(request)
            .await
            .map_err(|status| status_to_cluster_metadata_error("topic", status))?;

        Ok(())
    }

    async fn create_credential(
        &self,
        name: CredentialName,
        credential: Credential,
    ) -> Result<Credential> {
        let request = pb::CreateCredentialRequest {
            parent: name.parent().to_string(),
            credential_id: name.id().to_string(),
            credential: Some(credential.into()),
        };

        self.client
            .clone()
            .create_credential(request)
            .await
            .map_err(|status| status_to_cluster_metadata_error("credential", status))?
            .into_inner()
            .try_into()
    }

    async fn get_credential(&self, name: CredentialName) -> Result<Credential> {
        let request = pb::GetCredentialRequest {
            name: name.to_string(),
        };

        self.client
            .clone()
            .get_credential(request)
            .await
            .map_err(|status| status_to_cluster_metadata_error("credential", status))?
            .into_inner()
            .try_into()
    }

    async fn list_credentials(
        &self,
        request: ListCredentialsRequest,
    ) -> Result<ListCredentialsResponse> {
        let request = pb::ListCredentialsRequest::from(request);

        self.client
            .clone()
            .list_credentials(request)
            .await
            .map_err(|status| status_to_cluster_metadata_error("credential", status))?
            .into_inner()
            .try_into()
    }

    async fn delete_credential(&self, name: CredentialName) -> Result<()> {
        let request = pb::DeleteCredentialRequest {
            name: name.to_string(),
        };

        self.client
            .clone()
            .delete_credential(request)
            .await
            .map_err(|status| status_to_cluster_metadata_error("credential", status))?;

        Ok(())
    }
}

fn status_to_cluster_metadata_error(
    resource: &'static str,
    status: tonic::Status,
) -> ClusterMetadataError {
    use tonic::Code;

    match status.code() {
        Code::NotFound => ClusterMetadataError::NotFound {
            resource,
            message: status.message().to_string(),
        },
        Code::AlreadyExists => ClusterMetadataError::AlreadyExists {
            resource,
            message: status.message().to_string(),
        },
        Code::InvalidArgument => ClusterMetadataError::InvalidArgument {
            resource,
            message: status.message().to_string(),
        },
        _ => ClusterMetadataError::Internal {
            message: format!("unknown error from remote service: {}", status.message()),
        },
    }
}
