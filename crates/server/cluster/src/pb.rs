use snafu::Snafu;

mod inner {
    tonic::include_proto!("wings.cluster");
}

#[derive(Debug, Snafu)]
pub enum WireError {}

pub use self::inner::*;

type Result<T, E = WireError> = ::std::result::Result<T, E>;

mod convert {}
