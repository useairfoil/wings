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
pub struct SqlControlPlane {
    db: Database,
}

impl SqlControlPlane {
    /// Creates a new SQL control plane with the given connection pool.
    pub fn new(db: Database) -> Self {
        Self { db }
    }
}

pub async fn migrate(db: &Database) -> Result<(), ClusterMetadataError> {
    use self::db::migrations::Migrator;

    Migrator::up(&db.pool, None).await.unwrap();

    Ok(())
}
