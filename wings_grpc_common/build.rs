use std::{env, path::PathBuf, println};

const WINGS_DESCRIPTOR_FILE: &str = "wings_grpc_common.bin";
const FLIGHT_DESCRIPTOR_FILE: &str = "arrow_flight.bin";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR")?);

    println!("cargo:rerun-if-changed=proto");

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .file_descriptor_set_path(out_dir.join(WINGS_DESCRIPTOR_FILE))
        .compile_protos(&["proto/wings/Catalog.proto"], &["proto/wings/"])?;

    // We only need the file descriptor set for reflection.
    tonic_prost_build::configure()
        .build_server(false)
        .build_client(false)
        .file_descriptor_set_path(out_dir.join(FLIGHT_DESCRIPTOR_FILE))
        .compile_protos(&["proto/arrow/Flight.proto"], &["proto/arrow/"])?;

    Ok(())
}
