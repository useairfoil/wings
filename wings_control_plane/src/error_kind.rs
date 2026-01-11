/// Categories of errors for classification and handling.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ErrorKind {
    /// Bad configuration, needs user fix
    Configuration,

    /// Invalid input, user error
    Validation,

    /// Resource missing
    NotFound,

    /// Resource exists or is locked
    Conflict,

    /// Network/IO errors, retry possible
    Temporary,

    /// Bugs, system errors
    Internal,
}

impl ErrorKind {
    /// Whether this error is retryable.
    pub fn is_retryable(self) -> bool {
        matches!(self, Self::Temporary)
    }

    /// Standard exit code for this error category.
    pub fn exit_code(self) -> i32 {
        match self {
            Self::Configuration => 78, // EX_CONFIG
            Self::Validation => 64,    // EX_USAGE
            Self::Temporary => 75,     // EX_TEMPFAIL
            Self::NotFound => 66,      // EX_NOINPUT
            _ => 70,                   // EX_SOFTWARE
        }
    }
}
