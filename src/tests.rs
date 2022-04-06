use crate::ImageArchive;
use crate::Result;
use std::fs::OpenOptions;

#[test]
fn test_local_tar() -> Result<()> {
    let img = ImageArchive::new_from_url("postgres:13")?;
    println!("{:?}", img);
    assert_eq!(img.manifest.layer_tar_paths.len(), 13);
    let mut out = OpenOptions::new()
        .write(true)
        .truncate(true)
        .create(true)
        .open("/tmp/postgres.json")?;
    serde_json::to_writer_pretty(&mut out, &img)?;
    Ok(())
}
