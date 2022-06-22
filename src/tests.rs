use itertools::Itertools;

use crate::FileTree;
use crate::ImageArchive;
use crate::Result;
use std::fs::OpenOptions;
use std::rc::Rc;

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
fn test_remove() -> Result<()> {
    let img = ImageArchive::new_from_url("postgres:13")?;
    let tree = img.merged_tree();
    assert!(FileTree::get_node(Rc::clone(&tree), "/usr/local/share").is_some());
    FileTree::remove_path(Rc::clone(&tree), "/usr/local/share");
    assert!(FileTree::get_node(Rc::clone(&tree), "/usr/local/share").is_none());
    Ok(())
}

#[test]
fn test_search() -> Result<()> {
    let img = ImageArchive::new_from_url("postgres:13")?;
    let tree = img.merged_tree();
    let ret: Vec<String> = FileTree::search(tree, &|node| {
        !node.data.file_info.is_dir && node.name.eq("psql")
    })
    .iter()
    .map(|node| node.borrow().path.clone())
    .sorted()
    .collect();
    assert_eq!(2, ret.len());
    println!("{:?}", ret);
    Ok(())
}
