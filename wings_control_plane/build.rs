use std::{env, io::Result, path::PathBuf, println};

static CLUSTER_METADATA_DESCRIPTOR_FILE: &str = "wings_v1_cluster_metadata.bin";

fn main() -> Result<()> {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    println!("cargo:rerun-if-changed=proto");

    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        // .include_file("mod.rs")
        .file_descriptor_set_path(out_dir.join(CLUSTER_METADATA_DESCRIPTOR_FILE))
        .compile_protos(
            &[
                "proto/wings/cluster_metadata.proto",
                // "proto/wings/log_metadata.proto",
            ],
            &["proto/"],
        )?;

    Ok(())
}
