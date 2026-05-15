// The code in this module is inspired by GreptimeDB's error module.

use std::error::Error;

use strum::FromRepr;

/// Standard error types used in Wings.
/// All status codes (except success) have a 4 digits code.
/// The first digit is the error category:
///
///  - 1xxx: common errors
///  - 2xxx: schema errors
#[derive(Debug, Clone, Copy, PartialEq, Eq, FromRepr)]
#[repr(u32)]
pub enum StatusCode {
    /// Success, no error occurred.
    Success = 0,

    /// Unknown error.
    Unknown = 1000,
    /// Unsupported operation.
    Unsupported = 1001,
    /// Unexpected error, maybe there is a bug.
    Unexpected = 1002,
    /// Internal error.
    Internal = 1003,
    /// Invalid argument.
    InvalidArgument = 1004,
    /// Illegal state.
    IllegalState = 1005,
    /// Caused by some error originated from an external system.
    External = 1006,
    /// The request deadline was exceeded.
    DeadlineExceeded = 1007,
    /// Invalid resource name.
    ResourceName = 1008,
    /// Not found.
    NotFound = 1009,
    /// Already exists.
    AlreadyExists = 1010,
    /// Operation not permitted.
    FailedPrecondition = 1011,

    /// Generic schema error.
    Schema = 2000,
    /// Duplicate field.
    DuplicateField = 2001,
    /// Invalid or mismatched data type.
    DataType = 2002,
}

impl StatusCode {
    pub fn to_tonic_code(&self) -> tonic::Code {
        // TODO: decide the mapping.
        match self {
            StatusCode::Success => tonic::Code::Ok,
            StatusCode::Unknown => tonic::Code::Unknown,
            StatusCode::Unsupported => tonic::Code::Unimplemented,
            StatusCode::Unexpected => tonic::Code::Internal,
            StatusCode::Internal => tonic::Code::Internal,
            StatusCode::InvalidArgument => tonic::Code::InvalidArgument,
            StatusCode::IllegalState => tonic::Code::FailedPrecondition,
            StatusCode::External => tonic::Code::Unknown,
            StatusCode::DeadlineExceeded => tonic::Code::DeadlineExceeded,
            StatusCode::ResourceName => tonic::Code::Unknown,
            StatusCode::NotFound => tonic::Code::NotFound,
            StatusCode::AlreadyExists => tonic::Code::AlreadyExists,
            StatusCode::FailedPrecondition => tonic::Code::FailedPrecondition,
            StatusCode::Schema => tonic::Code::Unknown,
            StatusCode::DuplicateField => tonic::Code::Unknown,
            StatusCode::DataType => tonic::Code::Unknown,
        }
    }
}

pub trait ErrorExt: Error {
    fn status_code(&self) -> StatusCode {
        StatusCode::Unknown
    }
}
