#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("server transport error: {0}")]
    ServerTransport(#[from] tonic::transport::Error),
    #[error("reflection server error: {0}")]
    ReflectionServer(#[from] tonic_reflection::server::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
