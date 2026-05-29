mod inner {
    tonic::include_proto!("arrow.flight.protocol");
}

const FLIGHT_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("wings_flight");

pub fn flight_file_descriptor_set() -> &'static [u8] {
    FLIGHT_DESCRIPTOR_SET
}

pub use self::inner::*;
