use fuser::{FileAttr, FileType, INodeNo};
#[cfg(unix)]
use nix::unistd::{Gid, Group, Uid, User};
use pna::Permission;
use std::collections::{BTreeMap, HashMap};
use std::ffi::{OsStr, OsString};
use std::io;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

pub(crate) type Inode = u64;
pub(crate) const ROOT_INODE: Inode = 1;

/// Cipher configuration used when re-encrypting file data on save.
///
/// `hash_algorithm` is intentionally absent: it is not accessible from
/// `NormalEntry`'s public API, so all re-encryption uses `argon2id()`
/// unconditionally.
#[derive(Copy, Clone, Debug)]
pub(crate) struct CipherConfig {
    pub encryption: pna::Encryption,
    pub cipher_mode: pna::CipherMode,
}

impl CipherConfig {
    pub(crate) fn from_entry_header(header: &pna::EntryHeader) -> Option<Self> {
        if header.encryption() != pna::Encryption::No {
            Some(Self {
                encryption: header.encryption(),
                cipher_mode: header.cipher_mode(),
            })
        } else {
            None
        }
    }
}

pub(crate) enum FileData {
    /// Data decoded and in memory; matches the on-disk state.
    Clean {
        data: Vec<u8>,
        cipher: Option<CipherConfig>,
    },
    /// Data decoded and modified; differs from on-disk state.
    Dirty {
        data: Vec<u8>,
        cipher: Option<CipherConfig>,
    },
    /// Newly created file; has never been written to the archive.
    New(Vec<u8>),
}

pub(crate) struct DirContent {
    children: BTreeMap<OsString, Inode>,
}

impl DirContent {
    pub(crate) fn new() -> Self {
        Self {
            children: BTreeMap::new(),
        }
    }

    pub(crate) fn get(&self, name: &OsStr) -> Option<Inode> {
        self.children.get(name).copied()
    }

    pub(crate) fn insert(&mut self, name: OsString, ino: Inode) {
        self.children.insert(name, ino);
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&OsString, &Inode)> {
        self.children.iter()
    }
}

pub(crate) enum FsContent {
    Directory(DirContent),
    File(FileData),
    Symlink(OsString),
}

pub(crate) struct FsNode {
    pub name: OsString,
    pub parent: Option<Inode>,
    pub attr: FileAttr,
    pub content: FsContent,
    pub xattrs: HashMap<OsString, Vec<u8>>,
}

impl std::fmt::Debug for FsNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FsNode")
            .field("name", &self.name)
            .field("ino", &self.attr.ino)
            .finish_non_exhaustive()
    }
}

pub(crate) struct FileTree {
    inodes: HashMap<Inode, FsNode>,
    next_inode: Inode,
    password: Option<String>,
    archive_path: PathBuf,
    dirty: bool,
}

// Static assertion: FileTree must be Send so it can live in Mutex<FileTree>.
const _: fn() = || {
    fn assert_send<T: Send>() {}
    assert_send::<FileTree>();
};

impl FileTree {
    /// Constructs a bare empty tree (no root node). Used by `archive_io::load`
    /// which inserts the root node itself after calling this.
    pub(crate) fn new(archive_path: PathBuf, password: Option<String>) -> Self {
        Self {
            inodes: HashMap::new(),
            next_inode: ROOT_INODE,
            password,
            archive_path,
            dirty: false,
        }
    }

    pub(crate) fn archive_path(&self) -> &Path {
        &self.archive_path
    }

    pub(crate) fn password(&self) -> Option<&str> {
        self.password.as_deref()
    }

    pub(crate) fn is_dirty(&self) -> bool {
        self.dirty
    }

    pub(crate) fn get(&self, ino: Inode) -> Option<&FsNode> {
        self.inodes.get(&ino)
    }

    pub(crate) fn get_mut(&mut self, ino: Inode) -> Option<&mut FsNode> {
        self.inodes.get_mut(&ino)
    }

    pub(crate) fn lookup_child(&self, parent: Inode, name: &OsStr) -> Option<&FsNode> {
        let parent_node = self.inodes.get(&parent)?;
        match &parent_node.content {
            FsContent::Directory(dir) => {
                let child_ino = dir.get(name)?;
                self.inodes.get(&child_ino)
            }
            _ => None,
        }
    }

    pub(crate) fn children(&self, parent: Inode) -> Option<impl Iterator<Item = &FsNode>> {
        let parent_node = self.inodes.get(&parent)?;
        match &parent_node.content {
            FsContent::Directory(dir) => {
                let inodes = &self.inodes;
                Some(dir.iter().filter_map(move |(_, &ino)| inodes.get(&ino)))
            }
            _ => None,
        }
    }

    pub(crate) fn next_inode(&mut self) -> Inode {
        self.next_inode += 1;
        self.next_inode
    }

    /// Insert a node into the tree under `parent` (or as root when `parent` is `None`).
    ///
    /// When `parent` is `Some(ino)`, the node is added to the parent's
    /// `DirContent.children` and `node.parent` is set accordingly.
    /// When `parent` is `None`, the node is inserted as-is (root node).
    ///
    /// Does NOT set `self.dirty` — callers that perform mutations (create_file,
    /// make_dir, etc.) set dirty themselves after calling this.
    pub(crate) fn insert_node(
        &mut self,
        mut node: FsNode,
        parent: Option<Inode>,
    ) -> io::Result<Inode> {
        let ino = node.attr.ino.0;
        node.parent = parent;

        if let Some(p) = parent {
            let parent_node = self
                .inodes
                .get_mut(&p)
                .ok_or_else(|| io::Error::other(format!("parent inode {p} not found")))?;
            match &mut parent_node.content {
                FsContent::Directory(dir) => {
                    dir.insert(node.name.clone(), ino);
                }
                _ => {
                    return Err(io::Error::other(format!(
                        "parent inode {p} is not a directory"
                    )));
                }
            }
        }

        self.inodes.insert(ino, node);
        Ok(ino)
    }

    /// Constructs an empty tree with just the root directory node, intended
    /// for use in unit tests.
    #[cfg(test)]
    pub(crate) fn new_for_test(archive_path: PathBuf, password: Option<String>) -> Self {
        let mut tree = Self::new(archive_path, password);
        let root = make_dir_node(ROOT_INODE, ".".into());
        tree.insert_node(root, None).unwrap();
        tree
    }
}

/// Build an [`FsNode`] representing an empty directory.
pub(crate) fn make_dir_node(ino: Inode, name: OsString) -> FsNode {
    let now = SystemTime::now();
    FsNode {
        name,
        parent: None,
        attr: FileAttr {
            ino: INodeNo(ino),
            size: 512,
            blocks: 1,
            atime: now,
            mtime: now,
            ctime: now,
            crtime: now,
            kind: FileType::Directory,
            perm: 0o775,
            nlink: 2,
            uid: current_uid(),
            gid: current_gid(),
            rdev: 0,
            blksize: 512,
            flags: 0,
        },
        content: FsContent::Directory(DirContent::new()),
        xattrs: HashMap::new(),
    }
}

#[cfg(unix)]
fn current_uid() -> u32 {
    Uid::current().as_raw()
}

#[cfg(unix)]
fn current_gid() -> u32 {
    Gid::current().as_raw()
}

#[cfg(not(unix))]
fn current_uid() -> u32 {
    0
}

#[cfg(not(unix))]
fn current_gid() -> u32 {
    0
}

#[cfg(unix)]
fn search_owner(name: &str, id: u64) -> Option<User> {
    let user = User::from_name(name).ok().flatten();
    if user.is_some() {
        return user;
    }
    User::from_uid((id as u32).into()).ok().flatten()
}

#[cfg(unix)]
fn search_group(name: &str, id: u64) -> Option<Group> {
    let group = Group::from_name(name).ok().flatten();
    if group.is_some() {
        return group;
    }
    Group::from_gid((id as u32).into()).ok().flatten()
}

/// Resolve the UID for a PNA permission entry, falling back to the current
/// process UID when the username / numeric id cannot be found.
pub(crate) fn get_uid(permission: Option<&Permission>) -> u32 {
    #[cfg(unix)]
    {
        permission
            .and_then(|it| search_owner(it.uname(), it.uid()))
            .map_or_else(Uid::current, |it| it.uid)
            .as_raw()
    }
    #[cfg(not(unix))]
    {
        let _ = permission;
        0
    }
}

/// Resolve the GID for a PNA permission entry, falling back to the current
/// process GID when the group name / numeric id cannot be found.
pub(crate) fn get_gid(permission: Option<&Permission>) -> u32 {
    #[cfg(unix)]
    {
        permission
            .and_then(|it| search_group(it.gname(), it.gid()))
            .map_or_else(Gid::current, |it| it.gid)
            .as_raw()
    }
    #[cfg(not(unix))]
    {
        let _ = permission;
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn make_tree() -> FileTree {
        FileTree::new_for_test(PathBuf::from("/tmp/test.pna"), None)
    }

    #[test]
    fn get_root_exists() {
        let tree = make_tree();
        assert!(tree.get(ROOT_INODE).is_some());
    }

    #[test]
    fn get_unknown_returns_none() {
        let tree = make_tree();
        assert!(tree.get(9999).is_none());
    }

    #[test]
    fn children_root_empty() {
        let tree = make_tree();
        let children: Vec<_> = tree.children(ROOT_INODE).unwrap().collect();
        assert!(children.is_empty());
    }

    #[test]
    fn is_dirty_initially_false() {
        let tree = make_tree();
        assert!(!tree.is_dirty());
    }

    #[test]
    fn lookup_child_not_found() {
        let tree = make_tree();
        assert!(tree.lookup_child(ROOT_INODE, OsStr::new("nope")).is_none());
    }
}
