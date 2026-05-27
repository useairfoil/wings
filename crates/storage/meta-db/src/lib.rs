mod error;
mod namespace;

pub use self::{
    error::Error,
    namespace::{ListNamespaceNamesResult, NamespaceManifest, NamespaceStore},
};
