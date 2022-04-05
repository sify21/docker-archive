use docker_archive::{ImageArchive, Result};
use std::fs::OpenOptions;

fn main() -> Result<()> {
    let img = ImageArchive::new_from_file("/tmp/postgres.tar")?;
    let mut out = OpenOptions::new()
        .write(true)
        .truncate(true)
        .create(true)
        .open("/tmp/postgres.json")?;
    serde_json::to_writer_pretty(&mut out, &img)?;
    Ok(())
}
