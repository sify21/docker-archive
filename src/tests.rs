use crate::ImageArchive;
use crate::Result;

#[test]
fn test_local_tar() -> Result<()> {
    let img = ImageArchive::new_from_file("/home/sify/Downloads/postgres.tar")?;
    assert_eq!(img.layer_map.len(), 13);
    Ok(())
}
