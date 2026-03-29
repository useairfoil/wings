//! SQL-based implementation of the control plane.
//!
//! This crate provides an SQL-backed implementation of the Wings control plane,
//! storing metadata in a relational database.

mod cluster_metadata;
pub mod db;
mod log_metadata;

pub use sea_orm::ConnectOptions;
use sea_orm_migration::MigratorTrait;
use wings_control_plane_core::ClusterMetadataError;

pub use self::db::Database;

/// SQL-based control plane implementation.
///
/// This struct provides an SQL-backed implementation of the control plane
/// metadata store, using a connection pool for all database operations.
#[derive(Clone)]
pub struct SqlControlPlane {
    db: Database,
}

impl SqlControlPlane {
    /// Creates a new SQL control plane with the given connection pool.
    pub fn new(db: Database) -> Self {
        Self { db }
    }

    /// Creates a new SQL control plane with an in-memory database.
    pub async fn new_in_memory() -> Self {
        let options = ConnectOptions::new("sqlite::memory:");
        let pool = Database::new(options)
            .await
            .expect("failed to create in-memory database");

        migrate(&pool)
            .await
            .expect("failed to migrate in-memory database");

        Self { db: pool }
    }
}

pub async fn migrate(db: &Database) -> Result<(), ClusterMetadataError> {
    use self::db::migrations::Migrator;

    Migrator::up(&db.pool, None).await.unwrap();

    Ok(())
}
