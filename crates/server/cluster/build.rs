use std::{io::Result, println};

fn main() -> Result<()> {
    println!("cargo:rerun-if-changed=proto");

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .extern_path(".wings.schema", "wings_schema::pb")
        .compile_protos(&["proto/wings/cluster.proto"], &["proto/"])?;

    Ok(())
}
