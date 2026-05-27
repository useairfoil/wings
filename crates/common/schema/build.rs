use std::{io::Result, println};

fn main() -> Result<()> {
    println!("cargo:rerun-if-changed=proto");

    tonic_prost_build::configure()
        .build_server(false)
        .build_client(false)
        .compile_protos(&["proto/schema/arrow_type.proto"], &["proto/"])?;

    Ok(())
}
