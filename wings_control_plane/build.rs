use std::{env, io::Result, path::PathBuf, println};

static CLUSTER_METADATA_DESCRIPTOR_FILE: &str = "wings_v1_cluster_metadata.bin";
static LOG_METADATA_DESCRIPTOR_FILE: &str = "wings_v1_log_metadata.bin";

fn main() -> Result<()> {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    println!("cargo:rerun-if-changed=proto");

    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .file_descriptor_set_path(out_dir.join(CLUSTER_METADATA_DESCRIPTOR_FILE))
        .compile_protos(&["proto/wings/cluster_metadata.proto"], &["proto/"])?;

    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .file_descriptor_set_path(out_dir.join(LOG_METADATA_DESCRIPTOR_FILE))
        .compile_protos(&["proto/wings/log_metadata.proto"], &["proto/"])?;

    Ok(())
}
