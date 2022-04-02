pub mod error;
mod file_tree;

use crate::error::DockerArchiveError;
use file_tree::FileTree;
use flate2::read::GzDecoder;
use serde::Deserialize;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Read;
use std::path::Path;
use tar::{Archive, EntryType};

type Result<T> = std::result::Result<T, DockerArchiveError>;

#[derive(Default, Debug)]
pub struct ImageArchive {
    pub manifest: Manifest,
    pub config: Config,
    pub layer_map: HashMap<String, FileTree>,
}

#[derive(Default, Debug, Deserialize)]
pub struct Manifest {
    #[serde(rename = "Config")]
    pub config_path: String,
    #[serde(rename = "RepoTags")]
    pub repo_tags: Vec<String>,
    #[serde(rename = "Layers")]
    pub layer_tar_paths: Vec<String>,
}

#[derive(Default, Debug, Deserialize)]
pub struct Config {
    pub history: Vec<HistoryEntry>,
    pub rootfs: RootFS,
}

#[derive(Default, Debug, Deserialize)]
pub struct RootFS {
    #[serde(rename = "type")]
    pub typ: String,
    pub diff_ids: Vec<String>,
}

#[derive(Default, Debug, Deserialize)]
pub struct HistoryEntry {
    #[serde(skip)]
    pub id: String,
    #[serde(skip)]
    pub size: u64,
    pub created: String,
    #[serde(default)]
    pub author: String,
    pub created_by: String,
    #[serde(default)]
    pub empty_layer: bool,
}

impl ImageArchive {
    pub fn new_from_file<P: AsRef<Path>>(tar_file: P) -> Result<Self> {
        let file = OpenOptions::new().read(true).open(tar_file)?;
        Self::new_from_reader(file)
    }

    pub fn new_from_reader<R: Read>(obj: R) -> Result<Self> {
        let mut img = Self::default();
        let mut ar = Archive::new(obj);
        // store discovered json files in a map so we can read the image in one pass
        let mut json_files: HashMap<String, Vec<u8>> = HashMap::new();
        for entry in ar.entries()? {
            let mut e = entry?;
            // some layer tars can be relative layer symlinks to other layer tars
            match e.header().entry_type() {
                EntryType::Symlink | EntryType::Regular => {
                    if let Some(name) = e.path()?.to_str().map(|s| s.to_string()) {
                        if name.ends_with(".tar") {
                            let mut layer_tree: FileTree = Archive::new(e).try_into()?;
                            layer_tree.name = name.clone();
                            img.layer_map.insert(name, layer_tree);
                        } else if name.ends_with(".tar.gz") || name.ends_with("tgz") {
                            let mut layer_tree: FileTree =
                                Archive::new(GzDecoder::new(e)).try_into()?;
                            layer_tree.name = name.clone();
                            img.layer_map.insert(name, layer_tree);
                        } else if name.ends_with(".json") || name.starts_with("sha256:") {
                            let mut content = vec![];
                            e.read_to_end(&mut content)?;
                            json_files.insert(name, content);
                        }
                    }
                }
                _ => (),
            }
        }
        if let Some(data) = json_files.get("manifest.json") {
            let manifests: Vec<Manifest> = serde_json::from_slice(data)?;
            img.manifest =
                manifests
                    .into_iter()
                    .nth(0)
                    .ok_or(DockerArchiveError::InvalidArchive(
                        "image manifest is empty",
                    ))?;
        } else {
            return Err(DockerArchiveError::InvalidArchive(
                "could not find image manifest",
            ));
        }
        if let Some(data) = json_files.get(&img.manifest.config_path) {
            let mut config: Config = serde_json::from_slice(data)?;
            let mut layer_idx = 0;
            for h in config.history.iter_mut() {
                if h.empty_layer {
                    h.id = "<missing>".to_string();
                } else if let Some(s) = config.rootfs.diff_ids.get(layer_idx) {
                    h.id = s.clone();
                    layer_idx += 1;
                }
            }
            img.config = config;
        } else {
            return Err(DockerArchiveError::InvalidArchive(
                "could not find image config",
            ));
        }
        Ok(img)
    }
}
