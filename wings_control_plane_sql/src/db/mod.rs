pub mod entities;
pub mod error;
pub mod migrations;
mod queries;

use std::pin::Pin;

use sea_orm::{
    ConnectOptions, DatabaseConnection, DatabaseTransaction, TransactionError, TransactionTrait,
};

pub use self::error::{Error, Result};

pub struct Database {
    pub options: ConnectOptions,
    pub pool: DatabaseConnection,
}

impl Database {
    pub async fn new(options: ConnectOptions) -> Result<Self> {
        let pool = sea_orm::Database::connect(options.clone()).await?;
        Ok(Self { options, pool })
    }

    pub async fn with_transaction<F, T>(&self, f: F) -> Result<T>
    where
        F: for<'c> FnOnce(
                &'c DatabaseTransaction,
            ) -> Pin<Box<dyn Future<Output = Result<T>> + Send + 'c>>
            + Send,
        T: Send,
    {
        match self.pool.transaction(f).await {
            Ok(result) => Ok(result),
            Err(TransactionError::Connection(err)) => Err(err.into()),
            Err(TransactionError::Transaction(err)) => Err(err),
        }
    }
}
