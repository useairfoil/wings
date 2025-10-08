use tonic::{
    Request,
    metadata::{Ascii, MetadataValue},
};
use wings_control_plane::resources::NamespaceName;

pub fn new_request_for_namespace<T>(namespace: &NamespaceName, message: T) -> Request<T> {
    let mut request = Request::new(message);

    // PANIC: we know that the namespace name must be a valid ASCII string
    let namespace_ascii: MetadataValue<Ascii> = namespace
        .to_string()
        .parse()
        .expect("non-ascii namespace name");

    request
        .metadata_mut()
        .insert("x-wings-namespace", namespace_ascii);

    request
}
