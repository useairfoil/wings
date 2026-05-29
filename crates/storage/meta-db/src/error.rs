use object_store::path::Path;
use snafu::Snafu;
use wings_txn_obj::TransactionalObjectError;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Object at {path} already exists"))]
    AlreadyExists { path: String },
    #[snafu(display("Object at {path} not found"))]
    NotFound { path: String },
    #[snafu(display("Failed to decode object at {path}"))]
    Decode { path: String },
    #[snafu(display(
        "Invalid resource parent for {name}: expected {expected_parent}, got {actual_parent}"
    ))]
    InvalidResourceParent {
        name: String,
        expected_parent: String,
        actual_parent: String,
    },
    #[snafu(display("Invalid table {name}: {message}"))]
    InvalidTable { name: String, message: String },
    #[snafu(transparent)]
    SecretManager { source: wings_secret_manager::Error },
    #[snafu(transparent)]
    ObjectStore { source: object_store::Error },
}

pub type Result<T, E = Error> = ::std::result::Result<T, E>;

pub(crate) fn txn_error(path: Path, err: TransactionalObjectError) -> Error {
    match err {
        TransactionalObjectError::ObjectVersionExists => Error::AlreadyExists {
            path: path.to_string(),
        },
        TransactionalObjectError::LatestRecordMissing => Error::NotFound {
            path: path.to_string(),
        },
        TransactionalObjectError::ObjectStoreError(source) => Error::ObjectStore { source },
        TransactionalObjectError::CallbackError(_)
        | TransactionalObjectError::InvalidObjectState => DecodeSnafu {
            path: path.to_string(),
        }
        .build(),
        TransactionalObjectError::IoError(_)
        | TransactionalObjectError::ObjectUpdateTimeout { .. }
        | TransactionalObjectError::Fenced => DecodeSnafu {
            path: path.to_string(),
        }
        .build(),
        _ => DecodeSnafu {
            path: path.to_string(),
        }
        .build(),
    }
}
