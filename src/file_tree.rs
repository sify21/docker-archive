use crate::error::DockerArchiveError;
use crate::Result;
use itertools::Itertools;
use path_clean::clean;
use serde::Serialize;
use std::cell::RefCell;
use std::rc::{Rc, Weak};
use std::{collections::HashMap, io::Read};
use tar::{Archive, EntryType};
use uuid::Uuid;
use xxhash_rust::xxh3::xxh3_64;

const WHITEOUT_PREFIX: &'static str = ".wh.";
const DOUBLE_WHITEOUT_PREFIX: &'static str = ".wh..wh..";

mod uuid_serde {
    use std::str::FromStr;

    use serde::{de::Error, Deserialize, Deserializer, Serializer};
    use uuid::Uuid;
    pub fn serialize<S>(uuid: &Uuid, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&uuid.to_string())
    }

    #[allow(dead_code)]
    pub fn deserialize<'de, D>(deserializer: D) -> Result<Uuid, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = <&str>::deserialize(deserializer)?;
        Uuid::from_str(s).map_err(Error::custom)
    }
}

#[derive(Debug, Default, Serialize)]
pub struct FileTree {
    pub root: Rc<RefCell<FileNode>>,
    pub size: u64,
    pub file_size: u64,
    pub name: String,
    #[serde(with = "uuid_serde")]
    pub id: Uuid,
}

impl FileTree {
    // marks the FileNodes in the owning (lower) tree with DiffType annotations when compared to the given (upper) tree
    pub fn compare_mark(&self, upper: &Self) {}

    // fetches a single node when given a slash-delimited string from root('/') to the desired node (e.g. '/a/node/path')
    pub fn get_node(tree: Rc<RefCell<Self>>, path: &str) -> Option<Rc<RefCell<FileNode>>> {
        let mut node = Rc::clone(&tree.borrow().root);
        for name in path.trim_matches('/').split('/') {
            if name.is_empty() {
                continue;
            }
            let child = node.borrow().children.get(name).map(Rc::clone);
            if child.is_none() {
                return None;
            } else {
                node = child.unwrap();
            }
        }
        Some(node)
    }

    // adds a new node to the tree with the given payload
    pub fn add_path(tree: Rc<RefCell<Self>>, path: &str, data: FileInfo) {
        let path = clean(path);
        // cannot add relative path
        if path.eq(".") {
            return;
        }
        let mut node = Rc::clone(&tree.borrow().root);
        for node_name in path.trim_matches('/').split('/') {
            if node_name.is_empty() {
                continue;
            }
            // find or create node
            let child = node.borrow().children.get(node_name).map(Rc::clone);
            if child.is_some() {
                node = child.unwrap();
                continue;
            }
            // don't add paths that should be deleted
            if node_name.starts_with(DOUBLE_WHITEOUT_PREFIX) {
                return;
            }
            // don't attach the payload. The payload is destined for the Path's end node, not any intermediary node.
            node = FileNode::add_child(Rc::clone(&node), node_name, Default::default());
        }
        // attach payload to the last specified node
        node.borrow_mut().data.file_info = data;
    }

    // removes a node from the tree given its path
    pub fn remove_path(tree: Rc<RefCell<Self>>, path: &str) {
        if let Some(node) = Self::get_node(tree, path) {
            FileNode::remove(node);
        }
    }
}

#[derive(Debug, Default, Serialize)]
pub struct FileNode {
    // serde don't support cyclic data structures(https://github.com/serde-rs/serde/issues/1361)
    #[serde(skip)]
    pub tree: Weak<RefCell<FileTree>>,
    #[serde(skip)]
    pub parent: Weak<RefCell<FileNode>>,
    pub name: String,
    pub data: NodeData,
    pub children: HashMap<String, Rc<RefCell<FileNode>>>,
    pub path: String,
}

// Visitor is a function that processes, observes, or otherwise transforms the given node
pub type FileNodeVistor = fn(Rc<RefCell<FileNode>>) -> ();
// VisitEvaluator is a function that indicates whether the given node should be visited by a Visitor
pub type FileNodeVisitEvaluator = fn(&FileNode) -> bool;

impl FileNode {
    pub fn add_child(parent: Rc<RefCell<Self>>, name: &str, data: FileInfo) -> Rc<RefCell<Self>> {
        // never allow processing of purely whiteout flag files (for now)
        // doublewhiteout 作用？
        if let Some(node) = parent.borrow().children.get(name) {
            // tree node already exists, replace the payload, keep the children
            node.borrow_mut().data.file_info = data;
            return Rc::clone(node);
        }
        // set node path
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

    // deletes the current FileNode from it's parent FileNode's relations
    pub fn remove(node: Rc<RefCell<Self>>) {
        if let Some(tree) = node.borrow().tree.upgrade() {
            // cannot remove the tree root
            if Rc::ptr_eq(&node, &tree.borrow().root) {
                return;
            }
            tree.borrow_mut().size -= 1;
        }
        if let Some(parent) = node.borrow().parent.upgrade() {
            parent.borrow_mut().children.remove(&node.borrow().name);
        }
        for child in node.borrow().children.values() {
            FileNode::remove(Rc::clone(child));
        }
    }

    pub fn is_whiteout(&self) -> bool {
        self.name.starts_with(WHITEOUT_PREFIX)
    }

    pub fn is_leaf(&self) -> bool {
        self.children.is_empty()
    }

    // compare self(lower node in the tree stack) and other(upper node in tree stack), return DiffType for upper node
    pub fn compare(lower: Option<Rc<RefCell<Self>>>, upper: Option<Rc<RefCell<Self>>>) -> DiffType {
        if lower.is_none() && upper.is_none() {
            return DiffType::Unmodified;
        }
        if lower.is_none() && upper.is_some() {
            return DiffType::Added;
        }
        if lower.is_some() && upper.is_none() {
            return DiffType::Removed;
        }
        // lower and upper both not none
        let (lower, upper) = (lower.unwrap(), upper.unwrap());
        if upper.borrow().is_whiteout() {
            return DiffType::Removed;
        }
        if lower.borrow().name.ne(&upper.borrow().name) {
            panic!("comparing mismatched nodes");
        }
        let file_info_diff = lower
            .borrow()
            .data
            .file_info
            .compare(&upper.borrow().data.file_info);
        file_info_diff
    }

    // iterates a tree depth-first (starting at this FileNode), evaluating the deepest depths first (visit on bubble up)
    pub fn visit_depth_child_first(
        node: Rc<RefCell<Self>>,
        visitor: FileNodeVistor,
        evaluator: FileNodeVisitEvaluator,
    ) {
        for (_, v) in node.borrow().children.iter().sorted_by_key(|x| x.0) {
            let child = Rc::clone(v);
            FileNode::visit_depth_child_first(child, visitor, evaluator);
        }
        // never visit the root node
        if let Some(tree) = node.borrow().tree.upgrade() {
            if Rc::ptr_eq(&node, &tree.borrow().root) {
                return;
            }
        }
        // put the borrow on its own line to make the borrow dropped before a call to visitor
        let visit = evaluator(&node.borrow());
        if visit {
            visitor(Rc::clone(&node))
        }
    }

    // iterates a tree depth-first (starting at this FileNode), evaluating the shallowest depths first (visit while sinking down)
    pub fn visit_depth_parent_first(
        node: Rc<RefCell<Self>>,
        visitor: FileNodeVistor,
        evaluator: FileNodeVisitEvaluator,
    ) {
        if !evaluator(&node.borrow()) {
            return;
        }
        // never visit the root node
        let mut is_root = false;
        if let Some(tree) = node.borrow().tree.upgrade() {
            if Rc::ptr_eq(&node, &tree.borrow().root) {
                is_root = true;
            }
        }
        if !is_root {
            visitor(Rc::clone(&node));
        }
        for (_, v) in node.borrow().children.iter().sorted_by_key(|x| x.0) {
            let child = Rc::clone(v);
            FileNode::visit_depth_parent_first(child, visitor, evaluator);
        }
    }
}

impl<R: Read> TryFrom<Archive<R>> for FileTree {
    type Error = crate::error::DockerArchiveError;

    fn try_from(ar: Archive<R>) -> Result<Self> {
        let tree = Rc::new(RefCell::new(FileTree::default()));
        {
            let a = tree.borrow();
            let mut root = a.root.borrow_mut();
            root.tree = Rc::downgrade(&tree);
            //root.path = "/".to_string(); // root路径初始值设为空，因为添加子结点都会加/前缀； 可以在最后再设为/
        }
        let file_infos = FileInfo::get_file_infos(ar)?;
        for file_info in file_infos.into_iter() {
            let path = file_info.path.clone();
            tree.borrow_mut().file_size += file_info.size;
            FileTree::add_path(Rc::clone(&tree), &path, file_info);
        }
        // root的path设为/
        tree.borrow().root.borrow_mut().path = "/".to_string();
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
#[derive(Debug, Default, Serialize)]
pub struct NodeData {
    pub view_info: ViewInfo,
    pub file_info: FileInfo,
    pub diff_type: DiffType,
}

// ViewInfo contains UI specific detail for a specific FileNode
#[derive(Debug, Default, Serialize)]
pub struct ViewInfo {
    pub collapsed: bool,
    pub hidden: bool,
}

// FileInfo contains tar metadata for a specific FileNode
#[derive(Debug, Default, Serialize)]
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

impl FileInfo {
    pub fn get_file_infos<R: Read>(mut ar: Archive<R>) -> Result<Vec<Self>> {
        let mut ret = vec![];
        for entry in ar.entries()? {
            let mut e = entry?;
            if let Some(path) = e.path()?.to_str().map(clean) {
                // always ensure relative path notations are not parsed as part of the filename
                if path.eq(".") {
                    continue;
                }
                match e.header().entry_type() {
                    EntryType::XGlobalHeader | EntryType::XHeader => {
                        return Err(DockerArchiveError::InvalidArchiveX(format!(
                            "unexptected tar file: type={:?} name={}",
                            e.header().entry_type(),
                            &path,
                        )));
                    }
                    _ => {
                        let mut hash = 0;
                        if !e.header().entry_type().is_dir() {
                            let mut data = vec![];
                            e.read_to_end(&mut data)?;
                            hash = xxh3_64(data.as_slice());
                        }
                        ret.push(FileInfo {
                            path,
                            hash,
                            ..e.header().into()
                        });
                    }
                }
            }
        }
        Ok(ret)
    }

    pub fn compare(&self, other: &Self) -> DiffType {
        if self.type_flag == other.type_flag {
            if self.hash == other.hash
                && self.mode == other.mode
                && self.uid == other.uid
                && self.gid == other.gid
            {
                return DiffType::Unmodified;
            }
        }
        DiffType::Modified
    }
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
#[derive(Debug, Serialize)]
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
