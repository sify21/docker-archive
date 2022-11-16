mod error;
mod file_tree;
#[cfg(test)]
mod tests;

pub use error::DockerArchiveError;
pub use file_tree::FileTree;
use flate2::read::GzDecoder;
use path_clean::clean;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::rc::Rc;
use tar::{Archive, EntryType};
use which::which;

pub type Result<T> = std::result::Result<T, DockerArchiveError>;

#[derive(Default, Debug, Serialize)]
pub struct ImageArchive {
    pub manifest: Manifest,
    pub config: Config,
    // layer_tar_path -> FileTree
    pub layer_map: HashMap<String, Rc<RefCell<FileTree>>>,
}

#[derive(Default, Debug, Deserialize, Serialize)]
pub struct Manifest {
    #[serde(rename = "Config")]
    pub config_path: String,
    #[serde(rename = "RepoTags")]
    pub repo_tags: Option<Vec<String>>,
    // layers of this image in order (oldest -> newest)
    #[serde(rename = "Layers")]
    pub layer_tar_paths: Vec<String>,
}

#[derive(Default, Debug, Deserialize, Serialize)]
pub struct Config {
    pub history: Vec<HistoryEntry>,
    pub rootfs: RootFS,
}

#[derive(Default, Debug, Deserialize, Serialize)]
pub struct RootFS {
    #[serde(rename = "type")]
    pub typ: String,
    pub diff_ids: Vec<String>,
}

#[derive(Default, Debug, Deserialize, Serialize)]
pub struct HistoryEntry {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub size: u64,
    // maybe missing, see https://www.jfrog.com/jira/browse/RTFACT-26005
    #[serde(default)]
    pub created: String,
    #[serde(default)]
    pub author: String,
    #[serde(default)]
    pub created_by: String,
    #[serde(default)]
    pub empty_layer: bool,
}

impl ImageArchive {
    pub fn new_from_url(img_url: &str) -> Result<Self> {
        let executable = which("docker").map_or_else(|_| which("podman"), |i| Ok(i))?;
        let mut child = Command::new(executable)
            .arg("image")
            .arg("save")
            .arg(img_url)
            .stdout(Stdio::piped())
            .spawn()?;
        let stdout = child
            .stdout
            .as_mut()
            .ok_or(DockerArchiveError::StdoutError)?;
        Self::new_from_reader(BufReader::new(stdout))
    }

    pub fn new_from_file<P: AsRef<Path>>(tar_file: P) -> Result<Self> {
        let file = OpenOptions::new().read(true).open(tar_file)?;
        Self::new_from_reader(BufReader::new(file))
    }

    pub fn new_from_reader<R: Read>(obj: R) -> Result<Self> {
        let mut img = Self::default();
        let mut ar = Archive::new(obj);
        // store discovered json files in a map so we can read the image in one pass
        let mut json_files: HashMap<String, Vec<u8>> = HashMap::new();
        // store layer tar symlink mapping to avoid construct the same FileTree twice
        let mut layer_tar_links: HashMap<String, String> = HashMap::new();
        for entry in ar.entries()? {
            let mut e = entry?;
            // some layer tars can be relative layer symlinks to other layer tars
            match e.header().entry_type() {
                EntryType::Symlink | EntryType::Regular => {
                    if let Some(name) = e.path()?.to_str().map(|s| s.to_string()) {
                        if name.ends_with(".tar") {
                            if e.header().entry_type().eq(&EntryType::Symlink) {
                                if let Ok(Some(p)) = e.header().link_name() {
                                    let tmp = PathBuf::from(&name);
                                    if let Some(tmp_parent) = tmp.parent() {
                                        layer_tar_links.insert(
                                            name,
                                            clean(&tmp_parent.join(p).to_string_lossy()),
                                        );
                                    } else {
                                        layer_tar_links.insert(name, clean(&p.to_string_lossy()));
                                    }
                                }
                            } else {
                                let layer_tree = FileTree::build_from_layer_tar(Archive::new(e))?;
                                layer_tree.borrow_mut().name = name.clone();
                                img.layer_map.insert(name, layer_tree);
                            }
                        } else if name.ends_with(".tar.gz") || name.ends_with("tgz") {
                            if e.header().entry_type().eq(&EntryType::Symlink) {
                                if let Ok(Some(p)) = e.header().link_name() {
                                    let tmp = PathBuf::from(&name);
                                    if let Some(tmp_parent) = tmp.parent() {
                                        layer_tar_links.insert(
                                            name,
                                            clean(&tmp_parent.join(p).to_string_lossy()),
                                        );
                                    } else {
                                        layer_tar_links.insert(name, clean(&p.to_string_lossy()));
                                    }
                                }
                            } else {
                                let layer_tree = FileTree::build_from_layer_tar(Archive::new(
                                    GzDecoder::new(e),
                                ))?;
                                layer_tree.borrow_mut().name = name.clone();
                                img.layer_map.insert(name, layer_tree);
                            }
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
        for (k, v) in layer_tar_links.iter() {
            // 2PB problem: https://github.com/rust-lang/rust/issues/59159
            if let Some(tree) = img.layer_map.get(v).map(Rc::clone) {
                img.layer_map.insert(k.clone(), tree);
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

    pub fn merged_tree(&self) -> Rc<RefCell<FileTree>> {
        FileTree::stack_trees(
            self.manifest
                .layer_tar_paths
                .iter()
                .map(|path| self.layer_map.get(path))
                .filter(|i| i.is_some())
                .map(|i| Rc::clone(i.unwrap()))
                .collect(),
        )
    }
}
