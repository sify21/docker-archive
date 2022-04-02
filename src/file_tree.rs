use crate::error::DockerArchiveError;
use crate::Result;
use path_clean::clean;
use std::cell::RefCell;
use std::rc::{Rc, Weak};
use std::{collections::HashMap, io::Read};
use tar::{Archive, EntryType};
use uuid::Uuid;
use xxhash_rust::xxh3::xxh3_64;

const WHITEOUT_PREFIX: &'static str = ".wh.";
const DOUBLE_WHITEOUT_PREFIX: &'static str = ".wh..wh..";

#[derive(Debug, Default)]
pub struct FileTree {
    pub root: Rc<RefCell<FileNode>>,
    pub size: u64,
    pub file_size: u64,
    pub name: String,
    pub id: Uuid,
}

#[derive(Debug, Default)]
pub struct FileNode {
    pub tree: Weak<RefCell<FileTree>>,
    pub parent: Weak<RefCell<FileNode>>,
    pub name: String,
    pub data: NodeData,
    pub children: HashMap<String, Rc<RefCell<FileNode>>>,
    pub path: String,
}

impl FileNode {
    pub fn add_child(parent: Rc<RefCell<Self>>, name: &str, data: FileInfo) -> Rc<RefCell<Self>> {
        if let Some(node) = parent.borrow().children.get(name) {
            // tree node already exists, replace the payload, keep the children
            node.borrow_mut().data.file_info = data;
            Rc::clone(node)
        } else {
            let mut path = parent.borrow().path.clone();
            path.push('/');
            // white out prefixes are fictitious on leaf nodes
            if let Some(s) = name.strip_prefix(WHITEOUT_PREFIX) {
                path.push_str(s);
            } else {
                path.push_str(name);
            }
            let child = Rc::new(RefCell::new(FileNode {
                tree: Weak::clone(&parent.borrow().tree),
                parent: Rc::downgrade(&parent),
                name: name.to_string(),
                data: NodeData {
                    file_info: data,
                    ..Default::default()
                },
                path,
                ..Default::default()
            }));
            parent
                .borrow_mut()
                .children
                .insert(name.to_string(), Rc::clone(&child));
            if let Some(t) = parent.borrow().tree.upgrade() {
                t.borrow_mut().size += 1;
            }
            child
        }
    }
}

impl<R: Read> TryFrom<Archive<R>> for FileTree {
    type Error = crate::error::DockerArchiveError;

    fn try_from(mut ar: Archive<R>) -> Result<Self> {
        let tree = Rc::new(RefCell::new(FileTree::default()));
        {
            let a = tree.borrow();
            let mut root = a.root.borrow_mut();
            root.tree = Rc::downgrade(&tree);
            root.path = "/".to_string();
        }
        'outer: for entry in ar.entries()? {
            let mut e = entry?;
            if let Some(name) = e.path()?.to_str().map(clean) {
                // always ensure relative path notations are not parsed as part of the filename
                if name.eq(".") {
                    continue;
                }
                match e.header().entry_type() {
                    EntryType::XGlobalHeader | EntryType::XHeader => {
                        return Err(DockerArchiveError::InvalidArchiveX(format!(
                            "unexptected tar file: type={:?} name={}",
                            e.header().entry_type(),
                            &name,
                        )));
                    }
                    _ => {
                        tree.borrow_mut().file_size += e.size();
                        let mut node = Rc::clone(&tree.borrow().root);
                        for node_name in name.trim_matches('/').split('/') {
                            if node_name.is_empty() {
                                continue;
                            }
                            // find or create node
                            let node_cloned = Rc::clone(&node);
                            if let Some(tmp) = node_cloned.borrow().children.get(node_name) {
                                node = Rc::clone(tmp);
                            } else {
                                // don't add paths that should be deleted
                                if node_name.starts_with(DOUBLE_WHITEOUT_PREFIX) {
                                    continue 'outer;
                                }
                                // don't attach the payload. The payload is destined for the Path's end node, not any intermediary node.
                                node = FileNode::add_child(
                                    Rc::clone(&node),
                                    node_name,
                                    Default::default(),
                                );
                            };
                        }
                        // attach payload to the last specified node
                        let mut hash = 0;
                        if !e.header().entry_type().is_dir() {
                            let mut data = vec![];
                            e.read_to_end(&mut data)?;
                            hash = xxh3_64(data.as_slice());
                        }
                        node.borrow_mut().data.file_info = FileInfo {
                            path: name,
                            hash,
                            ..e.header().into()
                        };
                    }
                }
                tree.borrow_mut().file_size += e.size();
            }
        }
        match Rc::try_unwrap(tree) {
            Ok(ret) => Ok(ret.take()),
            Err(origial) => Err(DockerArchiveError::InternalLogicError(format!(
                "Rc<FileTree> has more than one strong reference: {}",
                Rc::strong_count(&origial)
            ))),
        }
    }
}

// NodeData is the payload for a FileNode
#[derive(Debug, Default)]
pub struct NodeData {
    pub view_info: ViewInfo,
    pub file_info: FileInfo,
    pub diff_type: DiffType,
}

// ViewInfo contains UI specific detail for a specific FileNode
#[derive(Debug, Default)]
pub struct ViewInfo {
    pub collapsed: bool,
    pub hidden: bool,
}

// FileInfo contains tar metadata for a specific FileNode
#[derive(Debug, Default)]
pub struct FileInfo {
    pub path: String,
    pub type_flag: u8,
    pub linkname: String,
    pub hash: u64,
    pub size: u64,
    pub mode: u32,
    pub uid: u64,
    pub gid: u64,
    pub is_dir: bool,
}

impl From<&tar::Header> for FileInfo {
    fn from(header: &tar::Header) -> Self {
        Self {
            path: header
                .path()
                .map(|p| p.to_str().unwrap_or("").to_string())
                .unwrap_or(String::new()),
            type_flag: header.entry_type().as_byte(),
            linkname: header
                .link_name()
                .map(|p| {
                    p.map(|s| s.to_str().unwrap_or("").to_string())
                        .unwrap_or(String::new())
                })
                .unwrap_or(String::new()),
            size: header.size().unwrap_or(0),
            mode: header.mode().unwrap_or(0),
            uid: header.uid().unwrap_or(0),
            gid: header.gid().unwrap_or(0),
            is_dir: header.entry_type().is_dir(),
            ..Default::default()
        }
    }
}

// DiffType defines the comparison result between two FileNodes
#[derive(Debug)]
pub enum DiffType {
    Unmodified,
    Modified,
    Added,
    Removed,
}

impl Default for DiffType {
    fn default() -> Self {
        Self::Unmodified
    }
}