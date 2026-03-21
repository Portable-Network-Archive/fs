use fuser::{Errno, FileAttr, FileType, INodeNo, TimeOrNow};
use id_tree::{InsertBehavior, Node as TreeNode, NodeId, RemoveBehavior, Tree, TreeBuilder};
#[cfg(unix)]
use nix::unistd::{Gid, Group, Uid, User};
use pna::Permission;
use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use std::{io, mem};

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

pub(crate) enum FileContent {
    /// Raw entry preserved for verbatim re-serialization via save().
    /// Note: load() always fully decodes entries; this variant is not
    /// produced by the current loading code.
    Unloaded(pna::NormalEntry<Vec<u8>>, pna::ReadOptions),
    /// Data decoded and in memory; matches the on-disk state.
    Loaded {
        data: Vec<u8>,
        cipher: Option<CipherConfig>,
    },
    /// Data decoded and modified; differs from on-disk state.
    Modified {
        data: Vec<u8>,
        cipher: Option<CipherConfig>,
    },
    /// Newly created file; has never been written to the archive.
    Created(Vec<u8>),
}

pub(crate) enum NodeContent {
    Directory,
    File(FileContent),
    Symlink(OsString),
}

pub(crate) struct Node {
    pub name: OsString,
    pub attr: FileAttr,
    pub content: NodeContent,
    pub xattrs: HashMap<OsString, Vec<u8>>,
}

impl std::fmt::Debug for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Node")
            .field("name", &self.name)
            .field("ino", &self.attr.ino)
            .finish_non_exhaustive()
    }
}

pub(crate) struct ArchiveStore {
    pub(crate) tree: Tree<Inode>,
    pub(crate) node_ids: HashMap<Inode, NodeId>,
    pub(crate) nodes: HashMap<Inode, Node>,
    pub(crate) last_inode: Inode,
    pub(crate) password: Option<String>,
    pub(crate) archive_path: PathBuf,
    pub(crate) dirty: bool,
}

// Static assertion: ArchiveStore must be Send so it can live in Mutex<ArchiveStore>.
const _: fn() = || {
    fn assert_send<T: Send>() {}
    assert_send::<ArchiveStore>();
};

/// Ensure `node` contains a writable file. Transitions Loaded->Modified if needed.
/// Returns Err(EISDIR) for directories, Err(EIO) for symlinks.
fn ensure_file_writable(node: &mut Node) -> Result<(), Errno> {
    match &node.content {
        NodeContent::Directory => return Err(Errno::EISDIR),
        NodeContent::Symlink(_) => return Err(Errno::EIO),
        _ => {}
    }
    force_load_file(node)?;
    if matches!(node.content, NodeContent::File(FileContent::Loaded { .. })) {
        let old = std::mem::replace(
            &mut node.content,
            NodeContent::File(FileContent::Created(Vec::new())),
        );
        match old {
            NodeContent::File(FileContent::Loaded { data, cipher }) => {
                node.content = NodeContent::File(FileContent::Modified { data, cipher });
            }
            _ => unreachable!(),
        }
    }
    Ok(())
}

fn force_load_file(node: &mut Node) -> Result<(), Errno> {
    if !matches!(node.content, NodeContent::File(FileContent::Unloaded(..))) {
        return Ok(());
    }
    // Take ownership with mem::replace to avoid borrow-checker issues
    let old = std::mem::replace(
        &mut node.content,
        NodeContent::File(FileContent::Created(Vec::new())),
    );
    match old {
        NodeContent::File(FileContent::Unloaded(entry, opts)) => {
            let cipher = CipherConfig::from_entry_header(entry.header());
            let mut data = Vec::new();
            match entry
                .reader(&opts)
                .and_then(|mut r| r.read_to_end(&mut data).map(|_| ()))
            {
                Ok(()) => {
                    node.content = NodeContent::File(FileContent::Loaded { data, cipher });
                }
                Err(e) => {
                    log::error!(
                        "force_load_file: failed to decode entry {:?}: {e}",
                        node.name
                    );
                    // Restore: put the original entry back so the node isn't corrupted
                    node.content = NodeContent::File(FileContent::Unloaded(entry, opts));
                    return Err(Errno::EIO);
                }
            }
        }
        _ => unreachable!(),
    }
    Ok(())
}

impl ArchiveStore {
    pub(crate) fn archive_path(&self) -> &Path {
        &self.archive_path
    }

    pub(crate) fn password(&self) -> Option<&str> {
        self.password.as_deref()
    }

    pub(crate) fn is_dirty(&self) -> bool {
        self.dirty
    }

    pub(crate) fn get_node(&self, ino: Inode) -> Option<&Node> {
        self.nodes.get(&ino)
    }

    pub(crate) fn get_node_mut(&mut self, ino: Inode) -> Option<&mut Node> {
        self.nodes.get_mut(&ino)
    }

    pub(crate) fn get_children(&self, parent: Inode) -> Option<Vec<&Node>> {
        let node_id = self.node_ids.get(&parent)?;
        let children = self.tree.children(node_id).ok()?;
        children
            .map(|tree_node| self.nodes.get(tree_node.data()))
            .collect::<Option<Vec<_>>>()
    }

    /// Transitions all in-memory dirty file states back to clean equivalents
    /// and clears the `dirty` flag.
    ///
    /// - `Modified { data, cipher }` → `Loaded { data, cipher }`
    /// - `Created(data)` + password present → `Loaded { data, cipher: Some(Aes/CTR) }`
    /// - `Created(data)` + no password → `Loaded { data, cipher: None }`
    /// - `Unloaded` → unchanged
    pub(crate) fn mark_clean(&mut self) {
        let has_password = self.password.is_some();
        for node in self.nodes.values_mut() {
            if let NodeContent::File(ref mut content) = node.content {
                let new_content = match content {
                    FileContent::Modified { .. } => {
                        // Take ownership by replacing with a temporary value.
                        let old = mem::replace(content, FileContent::Created(Vec::new()));
                        match old {
                            FileContent::Modified { data, cipher } => {
                                Some(FileContent::Loaded { data, cipher })
                            }
                            _ => unreachable!(),
                        }
                    }
                    FileContent::Created(_) => {
                        let old = mem::replace(content, FileContent::Created(Vec::new()));
                        match old {
                            FileContent::Created(data) => {
                                let cipher = if has_password {
                                    Some(CipherConfig {
                                        encryption: pna::Encryption::Aes,
                                        cipher_mode: pna::CipherMode::CTR,
                                    })
                                } else {
                                    None
                                };
                                Some(FileContent::Loaded { data, cipher })
                            }
                            _ => unreachable!(),
                        }
                    }
                    FileContent::Unloaded(..) | FileContent::Loaded { .. } => None,
                };
                if let Some(c) = new_content {
                    *content = c;
                }
            }
        }
        self.dirty = false;
    }

    pub(crate) fn next_inode(&mut self) -> Inode {
        self.last_inode += 1;
        self.last_inode
    }

    /// Insert a node into the store under `parent` (or as root when `parent` is `None`).
    pub(crate) fn insert_node(&mut self, node: Node, parent: Option<Inode>) -> io::Result<Inode> {
        let ino = node.attr.ino.0;
        let inserted_id = match parent {
            None => self
                .tree
                .insert(TreeNode::new(ino), InsertBehavior::AsRoot)
                .map_err(io::Error::other)?,
            Some(p) => {
                // Clone the NodeId so we do not hold a borrow on self.node_ids
                // while also mutating self.tree.
                let parent_tree_id = self
                    .node_ids
                    .get(&p)
                    .ok_or_else(|| io::Error::other(format!("parent inode {p} not found")))?
                    .clone();
                self.tree
                    .insert(
                        TreeNode::new(ino),
                        InsertBehavior::UnderNode(&parent_tree_id),
                    )
                    .map_err(io::Error::other)?
            }
        };
        self.node_ids.insert(ino, inserted_id);
        self.nodes.insert(ino, node);
        Ok(ino)
    }

    /// Walk `path` components under `parent`, creating intermediate directories
    /// as needed.  Returns the inode of the deepest component.
    pub(crate) fn make_dir_all(&mut self, path: &Path, mut parent: Inode) -> io::Result<Inode> {
        for component in path.components() {
            let name = component.as_os_str();
            let children = self
                .get_children(parent)
                .ok_or_else(|| io::Error::other("parent inode not found"))?;
            let existing = children
                .iter()
                .find(|n| n.name == name)
                .map(|n| n.attr.ino.0);
            if let Some(ino) = existing {
                parent = ino;
            } else {
                let ino = self.next_inode();
                let dir_node = make_dir_node(ino, name.into());
                self.insert_node(dir_node, Some(parent))?;
                parent = ino;
            }
        }
        Ok(parent)
    }

    pub(crate) fn create_file(
        &mut self,
        parent: Inode,
        name: &OsStr,
        mode: u32,
    ) -> Result<&Node, Errno> {
        let parent_node = self.nodes.get(&parent).ok_or(Errno::ENOENT)?;
        if !matches!(parent_node.content, NodeContent::Directory) {
            return Err(Errno::ENOTDIR);
        }
        let children = self.get_children(parent).unwrap_or_default();
        if children.iter().any(|n| n.name == name) {
            return Err(Errno::EEXIST);
        }
        let ino = self.next_inode();
        let now = SystemTime::now();
        let node = Node {
            name: name.to_owned(),
            xattrs: HashMap::new(),
            content: NodeContent::File(FileContent::Created(Vec::new())),
            attr: FileAttr {
                ino: INodeNo(ino),
                size: 0,
                blocks: 1,
                atime: now,
                mtime: now,
                ctime: now,
                crtime: now,
                kind: FileType::RegularFile,
                perm: mode as u16,
                nlink: 1,
                uid: current_uid(),
                gid: current_gid(),
                rdev: 0,
                blksize: 512,
                flags: 0,
            },
        };
        self.insert_node(node, Some(parent))
            .map_err(|_| Errno::EIO)?;
        self.dirty = true;
        Ok(self.nodes.get(&ino).unwrap())
    }

    pub(crate) fn write_file(
        &mut self,
        ino: Inode,
        offset: u64,
        data: &[u8],
    ) -> Result<usize, Errno> {
        // Early-exit guards — before any mutation
        if data.is_empty() {
            return Ok(0);
        }
        let offset = usize::try_from(offset).map_err(|_| Errno::EFBIG)?;

        let node = self.nodes.get_mut(&ino).ok_or(Errno::ENOENT)?;
        ensure_file_writable(node)?;
        let buf = match &mut node.content {
            NodeContent::File(FileContent::Modified { data: d, .. }) => d,
            NodeContent::File(FileContent::Created(d)) => d,
            _ => return Err(Errno::EIO),
        };
        if offset > buf.len() {
            buf.resize(offset, 0);
        }
        let end = offset + data.len();
        if end > buf.len() {
            buf.resize(end, 0);
        }
        buf[offset..end].copy_from_slice(data);
        node.attr.size = buf.len() as u64;
        self.dirty = true;
        Ok(data.len())
    }

    pub(crate) fn set_size(&mut self, ino: Inode, size: u64) -> Result<(), Errno> {
        let node = self.nodes.get_mut(&ino).ok_or(Errno::ENOENT)?;
        ensure_file_writable(node)?;
        let size_usize = usize::try_from(size).map_err(|_| Errno::EFBIG)?;
        let buf = match &mut node.content {
            NodeContent::File(FileContent::Modified { data: d, .. }) => d,
            NodeContent::File(FileContent::Created(d)) => d,
            _ => return Err(Errno::EIO),
        };
        buf.resize(size_usize, 0);
        node.attr.size = size;
        self.dirty = true;
        Ok(())
    }

    pub(crate) fn set_times(
        &mut self,
        ino: Inode,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
    ) -> Result<(), Errno> {
        let node = self.nodes.get_mut(&ino).ok_or(Errno::ENOENT)?;
        let mut changed = false;
        match atime {
            Some(TimeOrNow::SpecificTime(t)) => {
                node.attr.atime = t;
                changed = true;
            }
            Some(TimeOrNow::Now) => {
                node.attr.atime = SystemTime::now();
                changed = true;
            }
            None => {}
        }
        match mtime {
            Some(TimeOrNow::SpecificTime(t)) => {
                node.attr.mtime = t;
                changed = true;
            }
            Some(TimeOrNow::Now) => {
                node.attr.mtime = SystemTime::now();
                changed = true;
            }
            None => {}
        }
        if changed {
            self.dirty = true;
        }
        Ok(())
    }

    pub(crate) fn make_dir(
        &mut self,
        parent: Inode,
        name: &OsStr,
        mode: u32,
        umask: u32,
    ) -> Result<&Node, Errno> {
        let parent_node = self.nodes.get(&parent).ok_or(Errno::ENOENT)?;
        if !matches!(parent_node.content, NodeContent::Directory) {
            return Err(Errno::ENOTDIR);
        }
        let children = self.get_children(parent).unwrap_or_default();
        if children.iter().any(|n| n.name == name) {
            return Err(Errno::EEXIST);
        }
        let ino = self.next_inode();
        let effective_mode = (mode & !umask) as u16;
        let mut node = make_dir_node(ino, name.to_owned());
        node.attr.perm = effective_mode;
        self.insert_node(node, Some(parent))
            .map_err(|_| Errno::EIO)?;
        self.nodes.get_mut(&parent).unwrap().attr.nlink += 1;
        self.dirty = true;
        Ok(self.nodes.get(&ino).unwrap())
    }

    pub(crate) fn unlink(&mut self, parent: Inode, name: &OsStr) -> Result<(), Errno> {
        let children = self.get_children(parent).ok_or(Errno::ENOENT)?;
        let target_ino = children
            .iter()
            .find(|n| n.name == name)
            .ok_or(Errno::ENOENT)?
            .attr
            .ino
            .0;
        let target = self.nodes.get(&target_ino).unwrap();
        if matches!(target.content, NodeContent::Directory) {
            #[cfg(target_os = "macos")]
            return Err(Errno::EPERM);
            #[cfg(not(target_os = "macos"))]
            return Err(Errno::EISDIR);
        }
        let node_id = self
            .node_ids
            .get(&target_ino)
            .cloned()
            .ok_or(Errno::ENOENT)?;
        self.tree
            .remove_node(node_id, RemoveBehavior::OrphanChildren)
            .map_err(|_| Errno::EIO)?;
        self.node_ids.remove(&target_ino);
        self.nodes.remove(&target_ino);
        self.dirty = true;
        Ok(())
    }

    pub(crate) fn rmdir(&mut self, _parent: Inode, _name: &OsStr) -> Result<(), Errno> {
        Err(Errno::ENOSYS)
    }

    pub(crate) fn rename(
        &mut self,
        _old_parent: Inode,
        _old_name: &OsStr,
        _new_parent: Inode,
        _new_name: &OsStr,
        _flags: fuser::RenameFlags,
    ) -> Result<(), Errno> {
        Err(Errno::ENOSYS)
    }

    pub(crate) fn set_attr_full(
        &mut self,
        _ino: Inode,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
    ) -> Result<(), Errno> {
        Err(Errno::ENOSYS)
    }

    pub(crate) fn create_symlink(
        &mut self,
        _parent: Inode,
        _name: &OsStr,
        _target: &std::path::Path,
    ) -> Result<&Node, Errno> {
        Err(Errno::ENOSYS)
    }

    pub(crate) fn create_hardlink(
        &mut self,
        _parent: Inode,
        _name: &OsStr,
        _target: Inode,
    ) -> Result<(), Errno> {
        Err(Errno::ENOSYS)
    }

    pub(crate) fn set_xattr(
        &mut self,
        _ino: Inode,
        _name: &OsStr,
        _value: &[u8],
    ) -> Result<(), Errno> {
        Err(Errno::ENOSYS)
    }

    pub(crate) fn remove_xattr(&mut self, _ino: Inode, _name: &OsStr) -> Result<(), Errno> {
        Err(Errno::ENOSYS)
    }

    /// Constructs a bare empty store (no root node).  Used by `archive_io::load`
    /// which inserts the root node itself after calling this.
    pub(crate) fn new(archive_path: PathBuf, password: Option<String>) -> Self {
        Self {
            tree: TreeBuilder::new().build(),
            node_ids: HashMap::new(),
            nodes: HashMap::new(),
            last_inode: ROOT_INODE,
            password,
            archive_path,
            dirty: false,
        }
    }

    /// Constructs an empty store with just the root directory node, intended
    /// for use in unit tests.
    #[cfg(test)]
    pub(crate) fn new_for_test(archive_path: PathBuf, password: Option<String>) -> Self {
        let mut store = Self::new(archive_path, password);
        let root = make_dir_node(ROOT_INODE, ".".into());
        store.insert_node(root, None).unwrap();
        store
    }
}

/// Build a [`Node`] representing an empty directory.
pub(crate) fn make_dir_node(ino: Inode, name: OsString) -> Node {
    let now = SystemTime::now();
    Node {
        name,
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
        content: NodeContent::Directory,
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
    use fuser::TimeOrNow;
    use std::ffi::OsStr;
    use std::path::PathBuf;
    use tempfile::TempDir;

    /// Compare two `fuser::Errno` values by their raw code, since `Errno` does
    /// not implement `PartialEq`.
    fn errno_eq(a: fuser::Errno, b: fuser::Errno) -> bool {
        a.code() == b.code()
    }

    /// Panic unless `a` and `b` refer to the same errno code.
    #[track_caller]
    fn assert_errno(a: fuser::Errno, b: fuser::Errno) {
        assert_eq!(
            a.code(),
            b.code(),
            "expected errno {:?} ({}), got {:?} ({})",
            b,
            b.code(),
            a,
            a.code(),
        );
    }

    fn make_store() -> ArchiveStore {
        ArchiveStore::new_for_test(PathBuf::from("/tmp/test.pna"), None)
    }

    fn make_store_with_file(content: &[u8]) -> (ArchiveStore, Inode) {
        let mut store = make_store();
        let node = store
            .create_file(ROOT_INODE, OsStr::new("test.txt"), 0o644)
            .unwrap();
        let ino = node.attr.ino.0;
        if !content.is_empty() {
            store.write_file(ino, 0, content).unwrap();
        }
        (store, ino)
    }

    #[test]
    fn get_node_root_exists() {
        let store = make_store();
        assert!(store.get_node(ROOT_INODE).is_some());
    }

    #[test]
    fn get_node_unknown_returns_none() {
        let store = make_store();
        assert!(store.get_node(9999).is_none());
    }

    #[test]
    fn get_children_root_empty() {
        let store = make_store();
        let children = store.get_children(ROOT_INODE).unwrap();
        assert!(children.is_empty());
    }

    #[test]
    fn is_dirty_initially_false() {
        let store = make_store();
        assert!(!store.is_dirty());
    }

    // --- create_file ---
    #[test]
    fn create_file_happy_path() {
        let mut store = make_store();
        let node = store
            .create_file(ROOT_INODE, OsStr::new("a.txt"), 0o644)
            .unwrap();
        assert_eq!(node.name, "a.txt");
        assert_eq!(node.attr.kind, FileType::RegularFile);
        assert_eq!(node.attr.perm, 0o644);
        assert!(matches!(
            node.content,
            NodeContent::File(FileContent::Created(ref d)) if d.is_empty()
        ));
        assert!(store.is_dirty());
    }

    #[test]
    fn create_file_existing_name_returns_eexist() {
        let mut store = make_store();
        store
            .create_file(ROOT_INODE, OsStr::new("a.txt"), 0o644)
            .unwrap();
        let err = store
            .create_file(ROOT_INODE, OsStr::new("a.txt"), 0o644)
            .unwrap_err();
        assert_errno(err, fuser::Errno::EEXIST);
    }

    #[test]
    fn create_file_bad_parent_returns_enoent() {
        let mut store = make_store();
        let err = store
            .create_file(9999, OsStr::new("x.txt"), 0o644)
            .unwrap_err();
        assert_errno(err, fuser::Errno::ENOENT);
    }

    #[test]
    fn create_file_parent_is_file_returns_enotdir() {
        let (mut store, file_ino) = make_store_with_file(b"");
        let err = store
            .create_file(file_ino, OsStr::new("x.txt"), 0o644)
            .unwrap_err();
        assert_errno(err, fuser::Errno::ENOTDIR);
    }

    // --- write_file ---
    #[test]
    fn write_file_at_offset_zero() {
        let (mut store, ino) = make_store_with_file(b"");
        let written = store.write_file(ino, 0, b"hello").unwrap();
        assert_eq!(written, 5);
        let node = store.get_node(ino).unwrap();
        assert_eq!(node.attr.size, 5);
    }

    #[test]
    fn write_file_sparse_zero_fills() {
        let (mut store, ino) = make_store_with_file(b"hello");
        store.write_file(ino, 10, b"!").unwrap();
        let node = store.get_node(ino).unwrap();
        assert_eq!(node.attr.size, 11);
        if let NodeContent::File(FileContent::Created(data)) = &node.content {
            assert_eq!(data[5..10], [0u8; 5]);
            assert_eq!(data[10], b'!');
        } else {
            panic!("expected Created content");
        }
    }

    #[test]
    fn write_file_empty_data_is_noop() {
        let (mut store, ino) = make_store_with_file(b"hello");
        let written = store.write_file(ino, 0, b"").unwrap();
        assert_eq!(written, 0);
        assert_eq!(store.get_node(ino).unwrap().attr.size, 5);
    }

    #[test]
    fn write_file_bad_ino_returns_enoent() {
        let mut store = make_store();
        assert_errno(
            store.write_file(9999, 0, b"x").unwrap_err(),
            fuser::Errno::ENOENT,
        );
    }

    #[test]
    fn write_file_on_dir_returns_eisdir() {
        let mut store = make_store();
        assert_errno(
            store.write_file(ROOT_INODE, 0, b"x").unwrap_err(),
            fuser::Errno::EISDIR,
        );
    }

    // --- set_size ---
    #[test]
    fn set_size_truncate() {
        let (mut store, ino) = make_store_with_file(b"hello");
        store.set_size(ino, 3).unwrap();
        let node = store.get_node(ino).unwrap();
        assert_eq!(node.attr.size, 3);
        if let NodeContent::File(FileContent::Created(data)) = &node.content {
            assert_eq!(data.as_slice(), b"hel");
        } else {
            panic!("expected Created");
        }
    }

    #[test]
    fn set_size_truncate_to_zero() {
        let (mut store, ino) = make_store_with_file(b"hello");
        store.set_size(ino, 0).unwrap();
        assert_eq!(store.get_node(ino).unwrap().attr.size, 0);
        if let NodeContent::File(FileContent::Created(data)) = &store.get_node(ino).unwrap().content
        {
            assert!(data.is_empty());
        } else {
            panic!("expected Created");
        }
    }

    #[test]
    fn set_size_extend_zero_pads() {
        let (mut store, ino) = make_store_with_file(b"hi");
        store.set_size(ino, 5).unwrap();
        let node = store.get_node(ino).unwrap();
        assert_eq!(node.attr.size, 5);
        if let NodeContent::File(FileContent::Created(data)) = &store.get_node(ino).unwrap().content
        {
            assert_eq!(&data[..2], b"hi");
            assert_eq!(&data[2..5], &[0, 0, 0]);
        } else {
            panic!("expected Created");
        }
    }

    #[test]
    fn set_size_on_dir_returns_eisdir() {
        let mut store = make_store();
        assert_errno(
            store.set_size(ROOT_INODE, 0).unwrap_err(),
            fuser::Errno::EISDIR,
        );
    }

    // --- set_times ---
    #[test]
    fn set_times_specific() {
        let (mut store, ino) = make_store_with_file(b"");
        let t1 = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1000);
        let t2 = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(2000);
        store
            .set_times(
                ino,
                Some(TimeOrNow::SpecificTime(t1)),
                Some(TimeOrNow::SpecificTime(t2)),
            )
            .unwrap();
        let node = store.get_node(ino).unwrap();
        assert_eq!(node.attr.atime, t1);
        assert_eq!(node.attr.mtime, t2);
        assert!(store.is_dirty());
    }

    #[test]
    fn set_times_bad_ino_returns_enoent() {
        let mut store = make_store();
        assert_errno(
            store.set_times(9999, None, None).unwrap_err(),
            fuser::Errno::ENOENT,
        );
    }

    // --- make_dir ---
    #[test]
    fn make_dir_happy_path() {
        let mut store = make_store();
        let node = store
            .make_dir(ROOT_INODE, OsStr::new("mydir"), 0o755, 0)
            .unwrap();
        assert_eq!(node.attr.nlink, 2);
        assert_eq!(node.attr.kind, FileType::Directory);
        assert!(store.is_dirty());
    }

    #[test]
    fn make_dir_increments_parent_nlink() {
        let mut store = make_store();
        let parent_nlink_before = store.get_node(ROOT_INODE).unwrap().attr.nlink;
        store
            .make_dir(ROOT_INODE, OsStr::new("mydir"), 0o755, 0)
            .unwrap();
        let parent_nlink_after = store.get_node(ROOT_INODE).unwrap().attr.nlink;
        assert_eq!(parent_nlink_after, parent_nlink_before + 1);
    }

    #[test]
    fn make_dir_existing_name_returns_eexist() {
        let mut store = make_store();
        store
            .make_dir(ROOT_INODE, OsStr::new("d"), 0o755, 0)
            .unwrap();
        assert_errno(
            store
                .make_dir(ROOT_INODE, OsStr::new("d"), 0o755, 0)
                .unwrap_err(),
            fuser::Errno::EEXIST,
        );
    }

    #[test]
    fn make_dir_applies_umask() {
        let mut store = make_store();
        let node = store
            .make_dir(ROOT_INODE, OsStr::new("masked"), 0o777, 0o022)
            .unwrap();
        assert_eq!(node.attr.perm, 0o755);
    }

    // --- unlink ---
    #[test]
    fn unlink_removes_file_completely() {
        let (mut store, ino) = make_store_with_file(b"hi");
        store.unlink(ROOT_INODE, OsStr::new("test.txt")).unwrap();
        assert!(store.get_node(ino).is_none());
        assert!(store.node_ids.get(&ino).is_none());
        assert!(store.get_children(ROOT_INODE).unwrap().is_empty());
        assert!(store.is_dirty());
    }

    #[test]
    fn unlink_nonexistent_returns_enoent() {
        let mut store = make_store();
        assert_errno(
            store
                .unlink(ROOT_INODE, OsStr::new("ghost.txt"))
                .unwrap_err(),
            fuser::Errno::ENOENT,
        );
    }

    #[test]
    fn unlink_directory_returns_eperm_or_eisdir() {
        let mut store = make_store();
        store
            .make_dir(ROOT_INODE, OsStr::new("d"), 0o755, 0)
            .unwrap();
        let err = store.unlink(ROOT_INODE, OsStr::new("d")).unwrap_err();
        assert!(
            errno_eq(err, fuser::Errno::EPERM) || errno_eq(err, fuser::Errno::EISDIR),
            "expected EPERM or EISDIR, got {:?}",
            err
        );
    }

    #[test]
    fn unlink_then_recreate_same_name() {
        let (mut store, old_ino) = make_store_with_file(b"hi");
        store.unlink(ROOT_INODE, OsStr::new("test.txt")).unwrap();
        let new_node = store
            .create_file(ROOT_INODE, OsStr::new("test.txt"), 0o644)
            .unwrap();
        assert_ne!(new_node.attr.ino.0, old_ino);
        assert_eq!(store.get_children(ROOT_INODE).unwrap().len(), 1);
    }

    // --- mark_clean ---
    #[test]
    fn mark_clean_transitions_created_to_loaded() {
        let (mut store, ino) = make_store_with_file(b"hello");
        assert!(store.is_dirty());
        store.mark_clean();
        assert!(!store.is_dirty());
        let node = store.get_node(ino).unwrap();
        assert!(matches!(
            node.content,
            NodeContent::File(FileContent::Loaded { .. })
        ));
    }

    #[test]
    fn mark_clean_created_with_password_gets_cipher() {
        let mut store =
            ArchiveStore::new_for_test(PathBuf::from("/tmp/t.pna"), Some("secret".to_string()));
        let node = store
            .create_file(ROOT_INODE, OsStr::new("f.txt"), 0o644)
            .unwrap();
        let ino = node.attr.ino.0;
        store.mark_clean();
        let node = store.get_node(ino).unwrap();
        if let NodeContent::File(FileContent::Loaded { cipher, .. }) = &node.content {
            assert!(cipher.is_some());
        } else {
            panic!("expected Loaded");
        }
    }

    // --- create_file: case 5 ---
    #[test]
    fn create_file_in_subdir() {
        let mut store = make_store();
        let subdir = store
            .make_dir(ROOT_INODE, OsStr::new("sub"), 0o755, 0)
            .unwrap();
        let subdir_ino = subdir.attr.ino.0;
        let node = store
            .create_file(subdir_ino, OsStr::new("nested.txt"), 0o644)
            .unwrap();
        assert_eq!(node.name, OsStr::new("nested.txt"));
    }

    // --- write_file: case 2 ---
    #[test]
    fn write_file_append_to_existing() {
        let (mut store, ino) = make_store_with_file(b"hello");
        let written = store.write_file(ino, 5, b" world").unwrap();
        assert_eq!(written, 6);
        if let NodeContent::File(FileContent::Created(data)) = &store.get_node(ino).unwrap().content
        {
            assert_eq!(data.as_slice(), b"hello world");
        } else {
            panic!("expected Created");
        }
    }

    // --- write_file: case 5 ---
    #[test]
    fn write_file_overwrites_loaded_becomes_modified() {
        // Get a Loaded node: create → write → mark_clean
        let mut store = make_store();
        let node = store
            .create_file(ROOT_INODE, OsStr::new("f.txt"), 0o644)
            .unwrap();
        let ino = node.attr.ino.0;
        store.write_file(ino, 0, b"abc").unwrap();
        store.mark_clean();
        // Now content is Loaded{[a,b,c], cipher:None}
        assert!(matches!(
            store.get_node(ino).unwrap().content,
            NodeContent::File(FileContent::Loaded { .. })
        ));
        // Write → should transition to Modified
        let written = store.write_file(ino, 1, b"XY").unwrap();
        assert_eq!(written, 2);
        let node = store.get_node(ino).unwrap();
        if let NodeContent::File(FileContent::Modified { data, cipher }) = &node.content {
            assert_eq!(data.as_slice(), b"aXY");
            assert!(cipher.is_none());
        } else {
            panic!("expected Modified, got something else");
        }
    }

    // --- write_file: case 6 ---
    #[test]
    fn write_file_on_loaded_from_archive() {
        let dir = TempDir::new().unwrap();
        let path = {
            use pna::{Archive, Metadata, WriteOptions};
            use std::io::Write as IoWrite;
            let p = dir.path().join("test.pna");
            let mut archive = Archive::write_header(std::fs::File::create(&p).unwrap()).unwrap();
            archive
                .write_file(
                    pna::EntryName::from_lossy("file.txt"),
                    Metadata::new(),
                    WriteOptions::builder().build(),
                    |w| {
                        w.write_all(b"original")?;
                        Ok(())
                    },
                )
                .unwrap();
            archive.finalize().unwrap();
            p
        };
        let mut store = crate::archive_io::load(&path, None).unwrap();
        let children = store.get_children(ROOT_INODE).unwrap();
        let ino = children[0].attr.ino.0;
        // write_file() doesn't write fSIZ, so the entry is force-loaded on load.
        assert!(matches!(
            store.get_node(ino).unwrap().content,
            NodeContent::File(FileContent::Loaded { .. })
        ));
        // write_file should transition Loaded -> Modified
        let written = store.write_file(ino, 0, b"data").unwrap();
        assert_eq!(written, 4);
        assert!(matches!(
            store.get_node(ino).unwrap().content,
            NodeContent::File(FileContent::Modified { .. })
        ));
    }

    #[test]
    fn write_file_empty_data_noop_on_loaded() {
        // Get Loaded state: create → write → mark_clean
        let mut store = make_store();
        let node = store
            .create_file(ROOT_INODE, OsStr::new("h.txt"), 0o644)
            .unwrap();
        let ino = node.attr.ino.0;
        store.write_file(ino, 0, b"abc").unwrap();
        store.mark_clean();
        // Verify Loaded
        assert!(matches!(
            store.get_node(ino).unwrap().content,
            NodeContent::File(FileContent::Loaded { .. })
        ));
        store.mark_clean();
        // Empty write — should stay Loaded, NOT become Modified
        store.write_file(ino, 0, b"").unwrap();
        assert!(
            matches!(
                store.get_node(ino).unwrap().content,
                NodeContent::File(FileContent::Loaded { .. })
            ),
            "empty write should not transition Loaded to Modified"
        );
    }

    // --- set_size: case 4 ---
    #[test]
    fn set_size_same_length_noop() {
        let (mut store, ino) = make_store_with_file(b"hello");
        store.set_size(ino, 5).unwrap();
        let node = store.get_node(ino).unwrap();
        assert_eq!(node.attr.size, 5);
        if let NodeContent::File(FileContent::Created(data)) = &node.content {
            assert_eq!(data.as_slice(), b"hello");
        } else {
            panic!("expected Created");
        }
    }

    // --- set_size: case 5 ---
    #[test]
    fn set_size_on_loaded_from_archive() {
        let dir = TempDir::new().unwrap();
        let path = {
            use pna::{Archive, Metadata, WriteOptions};
            use std::io::Write as IoWrite;
            let p = dir.path().join("test2.pna");
            let mut archive = Archive::write_header(std::fs::File::create(&p).unwrap()).unwrap();
            archive
                .write_file(
                    pna::EntryName::from_lossy("f.txt"),
                    Metadata::new(),
                    WriteOptions::builder().build(),
                    |w| {
                        w.write_all(b"hello")?;
                        Ok(())
                    },
                )
                .unwrap();
            archive.finalize().unwrap();
            p
        };
        let mut store = crate::archive_io::load(&path, None).unwrap();
        let children = store.get_children(ROOT_INODE).unwrap();
        let ino = children[0].attr.ino.0;
        // write_file() doesn't write fSIZ, so the entry is force-loaded on load.
        assert!(matches!(
            store.get_node(ino).unwrap().content,
            NodeContent::File(FileContent::Loaded { .. })
        ));
        store.set_size(ino, 0).unwrap();
        let node = store.get_node(ino).unwrap();
        assert_eq!(node.attr.size, 0);
        // Should be Modified after Loaded + truncate
        assert!(matches!(
            node.content,
            NodeContent::File(FileContent::Modified { .. })
        ));
    }

    // --- set_size: case 7 ---
    #[test]
    fn set_size_bad_ino_returns_enoent() {
        let mut store = make_store();
        let err = store.set_size(9999, 0).unwrap_err();
        assert!(errno_eq(err, fuser::Errno::ENOENT));
    }

    // --- set_times: case 2 ---
    #[test]
    fn set_times_now_atime_only() {
        let (mut store, ino) = make_store_with_file(b"");
        let before = SystemTime::now();
        let mtime_before = store.get_node(ino).unwrap().attr.mtime;
        store.set_times(ino, Some(TimeOrNow::Now), None).unwrap();
        let node = store.get_node(ino).unwrap();
        assert!(node.attr.atime >= before);
        assert_eq!(node.attr.mtime, mtime_before);
    }

    // --- set_times: case 3 ---
    #[test]
    fn set_times_mtime_only() {
        let (mut store, ino) = make_store_with_file(b"");
        let atime_before = store.get_node(ino).unwrap().attr.atime;
        let t2 = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(5000);
        store
            .set_times(ino, None, Some(TimeOrNow::SpecificTime(t2)))
            .unwrap();
        let node = store.get_node(ino).unwrap();
        assert_eq!(node.attr.atime, atime_before);
        assert_eq!(node.attr.mtime, t2);
    }

    // --- set_times: case 4 ---
    #[test]
    fn set_times_none_none_stays_clean() {
        let (mut store2, ino) = make_store_with_file(b"");
        store2.mark_clean(); // clear dirty
        assert!(!store2.is_dirty());
        store2.set_times(ino, None, None).unwrap();
        assert!(!store2.is_dirty());
    }

    // --- set_times: case 5 (Now mtime only) ---
    #[test]
    fn set_times_now_mtime_only() {
        let (mut store, ino) = make_store_with_file(b"");
        let before = SystemTime::now();
        store.set_times(ino, None, Some(TimeOrNow::Now)).unwrap();
        let node = store.get_node(ino).unwrap();
        assert!(node.attr.mtime >= before);
    }

    // --- set_times: on directory ---
    #[test]
    fn set_times_on_directory_succeeds() {
        let mut store = make_store();
        let t = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(5000);
        store
            .set_times(ROOT_INODE, Some(TimeOrNow::SpecificTime(t)), None)
            .unwrap();
        assert_eq!(store.get_node(ROOT_INODE).unwrap().attr.atime, t);
    }

    // --- write_file: empty data does not set dirty ---
    #[test]
    fn write_file_empty_data_does_not_set_dirty() {
        let (mut store, ino) = make_store_with_file(b"hello");
        store.mark_clean();
        assert!(!store.is_dirty());
        store.write_file(ino, 0, b"").unwrap();
        assert!(!store.is_dirty());
    }

    // --- set_times: case 6 ---
    #[test]
    fn set_times_loaded_no_state_change() {
        let dir = TempDir::new().unwrap();
        let path = {
            use pna::{Archive, Metadata, WriteOptions};
            use std::io::Write as IoWrite;
            let p = dir.path().join("test3.pna");
            let mut archive = Archive::write_header(std::fs::File::create(&p).unwrap()).unwrap();
            archive
                .write_file(
                    pna::EntryName::from_lossy("u.txt"),
                    Metadata::new(),
                    WriteOptions::builder().build(),
                    |w| {
                        w.write_all(b"data")?;
                        Ok(())
                    },
                )
                .unwrap();
            archive.finalize().unwrap();
            p
        };
        let mut store = crate::archive_io::load(&path, None).unwrap();
        let children = store.get_children(ROOT_INODE).unwrap();
        let ino = children[0].attr.ino.0;
        // write_file() doesn't write fSIZ, so the entry is force-loaded on load.
        assert!(matches!(
            store.get_node(ino).unwrap().content,
            NodeContent::File(FileContent::Loaded { .. })
        ));
        let t = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(7000);
        store
            .set_times(ino, Some(TimeOrNow::SpecificTime(t)), None)
            .unwrap();
        // Should still be Loaded (set_times doesn't change content state)
        assert!(matches!(
            store.get_node(ino).unwrap().content,
            NodeContent::File(FileContent::Loaded { .. })
        ));
        assert_eq!(store.get_node(ino).unwrap().attr.atime, t);
    }

    // --- make_dir: case 3 ---
    #[test]
    fn make_dir_bad_parent_returns_enoent() {
        let mut store = make_store();
        let err = store.make_dir(9999, OsStr::new("x"), 0o755, 0).unwrap_err();
        assert!(errno_eq(err, fuser::Errno::ENOENT));
    }

    // --- make_dir: case 4 ---
    #[test]
    fn make_dir_parent_is_file_returns_enotdir() {
        let (mut store, file_ino) = make_store_with_file(b"");
        let err = store
            .make_dir(file_ino, OsStr::new("x"), 0o755, 0)
            .unwrap_err();
        assert!(errno_eq(err, fuser::Errno::ENOTDIR));
    }

    // --- mark_clean: case 1 (Modified → Loaded) ---
    #[test]
    fn mark_clean_modified_to_loaded() {
        // Get Modified state: create → write → mark_clean → write again
        let mut store = make_store();
        let node = store
            .create_file(ROOT_INODE, OsStr::new("g.txt"), 0o644)
            .unwrap();
        let ino = node.attr.ino.0;
        store.write_file(ino, 0, b"abc").unwrap();
        store.mark_clean();
        // Now Loaded; write again → Modified
        store.write_file(ino, 0, b"XYZ").unwrap();
        assert!(matches!(
            store.get_node(ino).unwrap().content,
            NodeContent::File(FileContent::Modified { .. })
        ));
        store.mark_clean();
        assert!(!store.is_dirty());
        let node = store.get_node(ino).unwrap();
        if let NodeContent::File(FileContent::Loaded { data, cipher }) = &node.content {
            assert_eq!(data.as_slice(), b"XYZ");
            assert!(cipher.is_none());
        } else {
            panic!("expected Loaded");
        }
    }

    // --- mark_clean: case 2 (Modified{cipher=Some} → Loaded{cipher=Some}) ---
    #[test]
    fn mark_clean_modified_with_cipher_preserves_cipher() {
        // Build a store with password so cipher gets set
        let mut store =
            ArchiveStore::new_for_test(PathBuf::from("/tmp/t.pna"), Some("secret".to_string()));
        // Create file → write → mark_clean: Created+pwd → Loaded{cipher=Some(AES-CTR)}
        let node = store
            .create_file(ROOT_INODE, OsStr::new("enc.txt"), 0o644)
            .unwrap();
        let ino = node.attr.ino.0;
        store.write_file(ino, 0, b"hello").unwrap();
        store.mark_clean();
        // Now Loaded{data=[hello], cipher=Some(AES-CTR)}
        let cipher_cfg = {
            let node = store.get_node(ino).unwrap();
            match &node.content {
                NodeContent::File(FileContent::Loaded {
                    cipher: Some(c), ..
                }) => CipherConfig {
                    encryption: c.encryption,
                    cipher_mode: c.cipher_mode,
                },
                _ => panic!("expected Loaded with cipher"),
            }
        };
        // Write again: Loaded → Modified{data, cipher=Some(AES-CTR)}
        store.write_file(ino, 0, b"world").unwrap();
        assert!(matches!(
            store.get_node(ino).unwrap().content,
            NodeContent::File(FileContent::Modified {
                cipher: Some(_),
                ..
            })
        ));
        // mark_clean: Modified{cipher=Some} → Loaded{cipher=Some}, cipher preserved
        store.mark_clean();
        assert!(!store.is_dirty());
        let node = store.get_node(ino).unwrap();
        match &node.content {
            NodeContent::File(FileContent::Loaded {
                data,
                cipher: Some(c),
            }) => {
                assert_eq!(data.as_slice(), b"world");
                assert_eq!(c.encryption as u8, cipher_cfg.encryption as u8);
                assert_eq!(c.cipher_mode as u8, cipher_cfg.cipher_mode as u8);
            }
            _ => panic!("expected Loaded with cipher"),
        }
    }

    // --- mark_clean: case 4 (Created + no password -> Loaded{cipher=None}) ---
    #[test]
    fn mark_clean_created_without_password_gets_no_cipher() {
        let (mut store, ino) = make_store_with_file(b"data");
        store.mark_clean();
        let node = store.get_node(ino).unwrap();
        if let NodeContent::File(FileContent::Loaded { cipher, .. }) = &node.content {
            assert!(cipher.is_none());
        } else {
            panic!("expected Loaded");
        }
    }

    // --- write_file: mid-file overwrite preserves trailing bytes ---
    #[test]
    fn write_file_mid_overwrite_preserves_trailing() {
        let (mut store, ino) = make_store_with_file(b"hello");
        store.write_file(ino, 1, b"XY").unwrap();
        if let NodeContent::File(FileContent::Created(data)) = &store.get_node(ino).unwrap().content
        {
            assert_eq!(data.as_slice(), b"hXYlo");
        } else {
            panic!("expected Created");
        }
    }

    // --- unlink→recreate data isolation ---
    #[test]
    fn unlink_recreate_data_isolation() {
        let (mut store, old_ino) = make_store_with_file(b"old data");
        store.unlink(ROOT_INODE, OsStr::new("test.txt")).unwrap();
        let new_node = store
            .create_file(ROOT_INODE, OsStr::new("test.txt"), 0o644)
            .unwrap();
        let new_ino = new_node.attr.ino.0;
        store.write_file(new_ino, 0, b"new data").unwrap();
        // Verify old data is completely gone
        assert!(store.get_node(old_ino).is_none());
        if let NodeContent::File(FileContent::Created(data)) =
            &store.get_node(new_ino).unwrap().content
        {
            assert_eq!(data.as_slice(), b"new data");
        } else {
            panic!("expected Created");
        }
    }

    // --- write→truncate interaction ---
    #[test]
    fn write_then_truncate_preserves_prefix() {
        let (mut store, ino) = make_store_with_file(b"hello world");
        store.set_size(ino, 5).unwrap();
        let node = store.get_node(ino).unwrap();
        assert_eq!(node.attr.size, 5);
        if let NodeContent::File(FileContent::Created(data)) = &node.content {
            assert_eq!(data.as_slice(), b"hello");
        } else {
            panic!("expected Created");
        }
    }

    // --- mark_clean: case 3 (Loaded stays Loaded) ---
    #[test]
    fn mark_clean_loaded_stays_loaded() {
        let dir = TempDir::new().unwrap();
        let path = {
            use pna::{Archive, Metadata, WriteOptions};
            use std::io::Write as IoWrite;
            let p = dir.path().join("test4.pna");
            let mut archive = Archive::write_header(std::fs::File::create(&p).unwrap()).unwrap();
            archive
                .write_file(
                    pna::EntryName::from_lossy("lazy.txt"),
                    Metadata::new(),
                    WriteOptions::builder().build(),
                    |w| {
                        w.write_all(b"lazy")?;
                        Ok(())
                    },
                )
                .unwrap();
            archive.finalize().unwrap();
            p
        };
        let mut store = crate::archive_io::load(&path, None).unwrap();
        let children = store.get_children(ROOT_INODE).unwrap();
        let ino = children[0].attr.ino.0;
        // write_file() doesn't write fSIZ, so the entry is force-loaded on load.
        assert!(matches!(
            store.get_node(ino).unwrap().content,
            NodeContent::File(FileContent::Loaded { .. })
        ));
        store.mark_clean();
        assert!(!store.is_dirty());
        // Should still be Loaded
        assert!(matches!(
            store.get_node(ino).unwrap().content,
            NodeContent::File(FileContent::Loaded { .. })
        ));
    }
}
