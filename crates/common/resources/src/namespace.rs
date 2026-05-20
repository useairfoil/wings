use std::{sync::Arc, time::Duration};

use bytesize::ByteSize;

use crate::resource_type;

resource_type!(Namespace, "namespaces");

/// A root namespace.
///
/// A namespace is used to group related tables together.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Namespace {
    /// The namespace name.
    pub name: NamespaceName,
    /// The size at which the current segment is flushed to object storage.
    pub flush_size: ByteSize,
    /// The maximum interval at which the current segment is flushed to object storage.
    pub flush_interval: Duration,
}

pub type NamespaceRef = Arc<Namespace>;

impl Namespace {
    /// Create a new namespace with the given name and options.
    pub fn new(name: NamespaceName, options: NamespaceOptions) -> Self {
        Self {
            name,
            flush_size: options.flush_size,
            flush_interval: options.flush_interval,
        }
    }
}

/// Options for creating a namespace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceOptions {
    /// The size at which the current delta log is flushed to object storage.
    pub flush_size: ByteSize,
    /// The maximum interval at which the current delta log is flushed to object storage.
    pub flush_interval: Duration,
}

impl NamespaceOptions {
    /// Create new namespace options with the given configurations.
    pub fn new() -> Self {
        Self {
            flush_size: ByteSize::mb(8),
            flush_interval: Duration::from_millis(250),
        }
    }

    /// Change the flush size for the namespace.
    pub fn with_flush_size(mut self, flush_size: ByteSize) -> Self {
        self.flush_size = flush_size;
        self
    }

    /// Change the flush interval for the namespace.
    pub fn with_flush_interval(mut self, flush_interval: Duration) -> Self {
        self.flush_interval = flush_interval;
        self
    }
}
