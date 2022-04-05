use crate::ImageArchive;
use crate::Result;

#[test]
fn test_local_tar() -> Result<()> {
    let img = ImageArchive::new_from_file("/tmp/postgres.tar")?;
    println!("{:?}", img);
    assert_eq!(img.layer_map.len(), 13);
    Ok(())
}
