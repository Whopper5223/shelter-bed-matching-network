fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = "../../proto";
    println!("cargo:rerun-if-changed={proto_dir}/shelterbed.proto");
    tonic_prost_build::configure().compile_protos(
        &[format!("{proto_dir}/shelterbed.proto")],
        &[proto_dir.to_string()],
    )?;
    Ok(())
}
