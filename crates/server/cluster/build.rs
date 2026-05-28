use std::{env, io::Result, path::PathBuf, println};

const CLUSTER_DESCRIPTOR_FILE: &str = "wings_cluster.bin";

fn main() -> Result<()> {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    println!("cargo:rerun-if-changed=proto");

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .file_descriptor_set_path(out_dir.join(CLUSTER_DESCRIPTOR_FILE))
        .extern_path(".wings.schema", "wings_schema::pb")
        .compile_protos(&["proto/wings/cluster.proto"], &["proto/"])?;

    Ok(())
}
