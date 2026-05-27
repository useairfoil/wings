use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Object at {path} already exists"))]
    AlreadyExists { path: String },
    #[snafu(display("Object at {path} not found"))]
    NotFound { path: String },
    #[snafu(display("Failed to decode object at {path}"))]
    Decode { path: String },
    #[snafu(transparent)]
    SecretManager { source: wings_secret_manager::Error },
    #[snafu(transparent)]
    ObjectStore { source: object_store::Error },
}

pub type Result<T, E = Error> = ::std::result::Result<T, E>;
