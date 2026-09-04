use clap::Args;
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use wings_grpc_server::run_grpc_server;
use wings_meta_store::catalog::CatalogStore;

use crate::{object_store::ObjectStoreArgs, secret_store::SecretStoreArgs, server::ServerArgs};

#[derive(Debug, Args)]
pub struct DevArgs {
    #[command(flatten)]
    pub server: ServerArgs,
    #[command(flatten)]
    pub object_store: ObjectStoreArgs,
    #[command(flatten)]
    pub secret_store: SecretStoreArgs,
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("object store error: {0}")]
    ObjectStore(#[from] object_store::Error),
    #[error("secret store error: {0}")]
    SecretStore(#[from] wings_secret_store::Error),
    #[error("grpc server error: {0}")]
    GrpcServer(#[from] wings_grpc_server::Error),
}

impl DevArgs {
    pub async fn run(self, ct: CancellationToken) -> Result<(), Error> {
        let object_store = self.object_store.create_object_store()?;
        let secret_store = self
            .secret_store
            .create_secret_store(object_store.clone())
            .await?;
        let catalog_store = CatalogStore::new(secret_store, object_store);

        let listener = self.server.bind_listener().await?;

        run_grpc_server(listener, catalog_store, ct).await?;

        Ok(())
    }
}
