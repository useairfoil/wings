use std::{env, path::PathBuf, println};

const QUEUE_DESCRIPTOR_FILE: &str = "wings_queue.bin";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    println!("cargo:rerun-if-changed=proto");

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .file_descriptor_set_path(out_dir.join(QUEUE_DESCRIPTOR_FILE))
        .compile_protos(&["proto/queue.proto"], &["proto/"])?;

    Ok(())
}
