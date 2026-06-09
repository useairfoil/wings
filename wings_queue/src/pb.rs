mod inner {
    tonic::include_proto!("wings.queue");
}

const QUEUE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("wings_queue");

pub fn queue_file_descriptor_set() -> &'static [u8] {
    QUEUE_DESCRIPTOR_SET
}

pub use self::inner::*;
