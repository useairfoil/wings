use std::{env, io::Result, path::PathBuf, println};

static FLIGHT_DESCRIPTOR_FILE: &str = "arrow_flight_protocol.bin";

fn main() -> Result<()> {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    println!("cargo:rerun-if-changed=proto");

    tonic_build::configure()
        .build_client(false)
        .build_server(false)
        .file_descriptor_set_path(out_dir.join(FLIGHT_DESCRIPTOR_FILE))
        .compile_protos(&["proto/Flight.proto"], &["proto/"])?;

    Ok(())
}
