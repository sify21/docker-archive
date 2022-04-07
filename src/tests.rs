use itertools::Itertools;

use crate::FileTree;
use crate::ImageArchive;
use crate::Result;
use std::fs::OpenOptions;

#[test]
fn test_url() -> Result<()> {
    let img = ImageArchive::new_from_url("postgres:13")?;
    assert_eq!(img.manifest.layer_tar_paths.len(), 13);
    let mut out = OpenOptions::new()
        .write(true)
        .truncate(true)
        .create(true)
        .open("/tmp/postgres.json")?;
    serde_json::to_writer_pretty(&mut out, &img)?;
    Ok(())
}

#[test]
fn test_tar() -> Result<()> {
    let img = ImageArchive::new_from_file("/tmp/postgres.tar")?;
    assert_eq!(img.manifest.layer_tar_paths.len(), 13);
    let tree = img.combined_tree();
    let mut out = OpenOptions::new()
        .write(true)
        .truncate(true)
        .create(true)
        .open("/tmp/postgres.json")?;
    serde_json::to_writer_pretty(&mut out, &*(tree.borrow()))?;
    Ok(())
}

#[test]
fn test_search() -> Result<()> {
    let img = ImageArchive::new_from_file("/tmp/postgres.tar")?;
    let tree = img.combined_tree();
    for path in FileTree::search(tree, &|node| {
        !node.data.file_info.is_dir && node.name.starts_with("psql")
    })
    .iter()
    .map(|node| node.borrow().path.clone())
    .sorted()
    {
        println!("{}", path);
    }
    Ok(())
}
