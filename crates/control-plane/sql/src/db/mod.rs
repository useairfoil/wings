pub mod entities;
pub mod migrations;
mod partition_key;
mod queries;

use std::{
    fmt::{Debug, Display},
    pin::Pin,
};

use sea_orm::{
    ConnectOptions, DatabaseConnection, DatabaseTransaction, DbErr, TransactionError,
    TransactionTrait,
};

pub use self::partition_key::PartitionKey;

#[derive(Clone)]
pub struct Database {
    pub options: ConnectOptions,
    pub pool: DatabaseConnection,
}

impl Database {
    pub async fn new(options: ConnectOptions) -> Result<Self, DbErr> {
        let pool = sea_orm::Database::connect(options.clone()).await?;
        Ok(Self { options, pool })
    }

    pub async fn with_transaction<F, T, E>(&self, f: F) -> Result<T, E>
    where
        F: for<'c> FnOnce(
                &'c DatabaseTransaction,
            ) -> Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'c>>
            + Send,
        T: Send,
        E: From<DbErr> + Send + Debug + Display,
    {
        match self.pool.transaction(f).await {
            Ok(result) => Ok(result),
            Err(TransactionError::Connection(err)) => Err(err.into()),
            Err(TransactionError::Transaction(err)) => Err(err),
        }
    }
}
