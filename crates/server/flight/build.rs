use std::{env, io::Result, path::PathBuf, println};

const FLIGHT_DESCRIPTOR_FILE: &str = "wings_flight.bin";

fn main() -> Result<()> {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    println!("cargo:rerun-if-changed=proto");

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .file_descriptor_set_path(out_dir.join(FLIGHT_DESCRIPTOR_FILE))
        .compile_protos(&["proto/Flight.proto"], &["proto/"])?;

    Ok(())
}
