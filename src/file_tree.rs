use fuser::{Errno, FileAttr, FileType, INodeNo, TimeOrNow};
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

impl FileData {
    /// Clean -> Dirty. No-op when already Dirty or New.
    pub(crate) fn promote_to_dirty(&mut self) {
        if let FileData::Clean { data, cipher } = self {
            let data = std::mem::take(data);
            let cipher = cipher.take();
            *self = FileData::Dirty { data, cipher };
        }
    }

    pub(crate) fn make_clean(&mut self, has_password: bool) {
        match self {
            FileData::Dirty { data, cipher } => {
                let data = std::mem::take(data);
                let cipher = cipher.take();
                *self = FileData::Clean { data, cipher };
            }
            FileData::New(data) => {
                let data = std::mem::take(data);
                let cipher = if has_password {
                    Some(CipherConfig {
                        encryption: pna::Encryption::Aes,
                        cipher_mode: pna::CipherMode::CTR,
                    })
                } else {
                    None
                };
                *self = FileData::Clean { data, cipher };
            }
            FileData::Clean { .. } => {}
        }
    }

    pub(crate) fn data_mut(&mut self) -> &mut Vec<u8> {
        match self {
            FileData::Clean { data, .. } | FileData::Dirty { data, .. } | FileData::New(data) => {
                data
            }
        }
    }

    pub(crate) fn data(&self) -> &[u8] {
        match self {
            FileData::Clean { data, .. } | FileData::Dirty { data, .. } | FileData::New(data) => {
                data
            }
        }
    }
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

// Static assertion: FileTree must be Send so it can live in RwLock<FileTree>.
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

    #[cfg(test)]
    pub(crate) fn clear_password(&mut self) {
        self.password = None;
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

    // ── Write API ─────────────────────────────────────────────────────

    pub(crate) fn create_file(
        &mut self,
        parent: Inode,
        name: &OsStr,
        mode: u32,
    ) -> Result<&FsNode, Errno> {
        let parent_node = self.inodes.get(&parent).ok_or(Errno::ENOENT)?;
        if !matches!(parent_node.content, FsContent::Directory(_)) {
            return Err(Errno::ENOTDIR);
        }
        if self.lookup_child(parent, name).is_some() {
            return Err(Errno::EEXIST);
        }
        let ino = self.next_inode();
        let now = SystemTime::now();
        let node = FsNode {
            name: name.to_owned(),
            parent: None,
            xattrs: HashMap::new(),
            content: FsContent::File(FileData::New(Vec::new())),
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
        Ok(self.inodes.get(&ino).unwrap())
    }

    pub(crate) fn write_file(
        &mut self,
        ino: Inode,
        offset: u64,
        data: &[u8],
    ) -> Result<usize, Errno> {
        if data.is_empty() {
            return Ok(0);
        }
        let offset = usize::try_from(offset).map_err(|_| Errno::EFBIG)?;

        let node = self.inodes.get_mut(&ino).ok_or(Errno::ENOENT)?;
        let file_data = match &mut node.content {
            FsContent::Directory(_) => return Err(Errno::EISDIR),
            FsContent::Symlink(_) => return Err(Errno::EIO),
            FsContent::File(fd) => fd,
        };
        file_data.promote_to_dirty();
        let buf = file_data.data_mut();
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
        let node = self.inodes.get_mut(&ino).ok_or(Errno::ENOENT)?;
        let file_data = match &mut node.content {
            FsContent::Directory(_) => return Err(Errno::EISDIR),
            FsContent::Symlink(_) => return Err(Errno::EIO),
            FsContent::File(fd) => fd,
        };
        file_data.promote_to_dirty();
        let size_usize = usize::try_from(size).map_err(|_| Errno::EFBIG)?;
        let buf = file_data.data_mut();
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
        let node = self.inodes.get_mut(&ino).ok_or(Errno::ENOENT)?;
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
    ) -> Result<&FsNode, Errno> {
        let parent_node = self.inodes.get(&parent).ok_or(Errno::ENOENT)?;
        if !matches!(parent_node.content, FsContent::Directory(_)) {
            return Err(Errno::ENOTDIR);
        }
        if self.lookup_child(parent, name).is_some() {
            return Err(Errno::EEXIST);
        }
        let ino = self.next_inode();
        let effective_mode = (mode & !umask) as u16;
        let mut node = make_dir_node(ino, name.to_owned());
        node.attr.perm = effective_mode;
        self.insert_node(node, Some(parent))
            .map_err(|_| Errno::EIO)?;
        self.inodes.get_mut(&parent).unwrap().attr.nlink += 1;
        self.dirty = true;
        Ok(self.inodes.get(&ino).unwrap())
    }

    pub(crate) fn unlink(&mut self, parent: Inode, name: &OsStr) -> Result<(), Errno> {
        let target_ino = {
            let parent_node = self.inodes.get(&parent).ok_or(Errno::ENOENT)?;
            let dir = match &parent_node.content {
                FsContent::Directory(d) => d,
                _ => return Err(Errno::ENOTDIR),
            };
            dir.get(name).ok_or(Errno::ENOENT)?
        };
        let target = self.inodes.get(&target_ino).ok_or(Errno::ENOENT)?;
        if matches!(target.content, FsContent::Directory(_)) {
            #[cfg(target_os = "macos")]
            return Err(Errno::EPERM);
            #[cfg(not(target_os = "macos"))]
            return Err(Errno::EISDIR);
        }
        // Remove from parent's children
        let parent_node = self.inodes.get_mut(&parent).unwrap();
        if let FsContent::Directory(dir) = &mut parent_node.content {
            dir.children.remove(name);
        }
        // Remove from inodes
        self.inodes.remove(&target_ino);
        self.dirty = true;
        Ok(())
    }

    /// Transitions all in-memory dirty file states back to clean equivalents
    /// and clears the `dirty` flag.
    ///
    /// - `Dirty { data, cipher }` -> `Clean { data, cipher }`
    /// - `New(data)` + password present -> `Clean { data, cipher: Some(Aes/CTR) }`
    /// - `New(data)` + no password -> `Clean { data, cipher: None }`
    /// - `Clean` -> unchanged
    pub(crate) fn mark_clean(&mut self) {
        let has_password = self.password.is_some();
        for node in self.inodes.values_mut() {
            if let FsContent::File(ref mut file_data) = node.content {
                file_data.make_clean(has_password);
            }
        }
        self.dirty = false;
    }

    // ── ENOSYS stubs ─────────────────────────────────────────────────

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
    ) -> Result<&FsNode, Errno> {
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

    // ── Traversal / bulk helpers ───────────────────────────────────

    /// Return every node except root in depth-first order together with its
    /// full archive path (e.g. `"dir/subdir/file.txt"`).
    pub(crate) fn collect_dfs(&self) -> Vec<(Inode, &FsNode, String)> {
        let mut result = Vec::new();
        if let Some(root) = self.inodes.get(&ROOT_INODE) {
            if let FsContent::Directory(ref dir) = root.content {
                self.collect_dfs_recurse(dir, &mut result, &mut String::new());
            }
        }
        result
    }

    fn collect_dfs_recurse<'a>(
        &'a self,
        dir: &DirContent,
        result: &mut Vec<(Inode, &'a FsNode, String)>,
        prefix: &mut String,
    ) {
        for (name, &ino) in &dir.children {
            let node = match self.inodes.get(&ino) {
                Some(n) => n,
                None => continue,
            };
            let path = if prefix.is_empty() {
                name.to_string_lossy().into_owned()
            } else {
                format!("{}/{}", prefix, name.to_string_lossy())
            };
            result.push((ino, node, path.clone()));
            if let FsContent::Directory(ref child_dir) = node.content {
                let mut child_prefix = path;
                self.collect_dfs_recurse(child_dir, result, &mut child_prefix);
            }
        }
    }

    /// Create all intermediate directories along `path`, starting from
    /// `parent`. Returns the inode of the deepest directory.
    ///
    /// Does **not** set `self.dirty` — this is used during archive load,
    /// which should leave the tree in a clean state.
    pub(crate) fn make_dir_all(&mut self, path: &Path, mut parent: Inode) -> io::Result<Inode> {
        for component in path.components() {
            let name = component.as_os_str();
            if let Some(child) = self.lookup_child(parent, name) {
                parent = child.attr.ino.0;
            } else {
                let ino = self.next_inode();
                let dir_node = make_dir_node(ino, name.to_owned());
                self.insert_node(dir_node, Some(parent))?;
                parent = ino;
            }
        }
        Ok(parent)
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
    use fuser::TimeOrNow;
    use std::ffi::OsStr;
    use std::path::PathBuf;

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

    fn make_tree() -> FileTree {
        FileTree::new_for_test(PathBuf::from("/tmp/test.pna"), None)
    }

    fn make_tree_with_file(content: &[u8]) -> (FileTree, Inode) {
        let mut tree = make_tree();
        let node = tree
            .create_file(ROOT_INODE, OsStr::new("test.txt"), 0o644)
            .unwrap();
        let ino = node.attr.ino.0;
        if !content.is_empty() {
            tree.write_file(ino, 0, content).unwrap();
        }
        (tree, ino)
    }

    // ── Read API (carried over from Task 1) ─────────────────────────

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

    // ── create_file ─────────────────────────────────────────────────

    #[test]
    fn create_file_happy_path() {
        let mut tree = make_tree();
        let node = tree
            .create_file(ROOT_INODE, OsStr::new("a.txt"), 0o644)
            .unwrap();
        assert_eq!(node.name, "a.txt");
        assert_eq!(node.attr.kind, FileType::RegularFile);
        assert_eq!(node.attr.perm, 0o644);
        assert!(matches!(
            node.content,
            FsContent::File(FileData::New(ref d)) if d.is_empty()
        ));
        assert!(tree.is_dirty());
    }

    #[test]
    fn create_file_existing_name_returns_eexist() {
        let mut tree = make_tree();
        tree.create_file(ROOT_INODE, OsStr::new("a.txt"), 0o644)
            .unwrap();
        let err = tree
            .create_file(ROOT_INODE, OsStr::new("a.txt"), 0o644)
            .unwrap_err();
        assert_errno(err, fuser::Errno::EEXIST);
    }

    #[test]
    fn create_file_bad_parent_returns_enoent() {
        let mut tree = make_tree();
        let err = tree
            .create_file(9999, OsStr::new("x.txt"), 0o644)
            .unwrap_err();
        assert_errno(err, fuser::Errno::ENOENT);
    }

    #[test]
    fn create_file_parent_is_file_returns_enotdir() {
        let (mut tree, file_ino) = make_tree_with_file(b"");
        let err = tree
            .create_file(file_ino, OsStr::new("x.txt"), 0o644)
            .unwrap_err();
        assert_errno(err, fuser::Errno::ENOTDIR);
    }

    #[test]
    fn create_file_in_subdir() {
        let mut tree = make_tree();
        let subdir = tree
            .make_dir(ROOT_INODE, OsStr::new("sub"), 0o755, 0)
            .unwrap();
        let subdir_ino = subdir.attr.ino.0;
        let node = tree
            .create_file(subdir_ino, OsStr::new("nested.txt"), 0o644)
            .unwrap();
        assert_eq!(node.name, OsStr::new("nested.txt"));
    }

    // ── write_file ──────────────────────────────────────────────────

    #[test]
    fn write_file_at_offset_zero() {
        let (mut tree, ino) = make_tree_with_file(b"");
        let written = tree.write_file(ino, 0, b"hello").unwrap();
        assert_eq!(written, 5);
        let node = tree.get(ino).unwrap();
        assert_eq!(node.attr.size, 5);
    }

    #[test]
    fn write_file_sparse_zero_fills() {
        let (mut tree, ino) = make_tree_with_file(b"hello");
        tree.write_file(ino, 10, b"!").unwrap();
        let node = tree.get(ino).unwrap();
        assert_eq!(node.attr.size, 11);
        if let FsContent::File(fd) = &node.content {
            let data = fd.data();
            assert_eq!(data[5..10], [0u8; 5]);
            assert_eq!(data[10], b'!');
        } else {
            panic!("expected File content");
        }
    }

    #[test]
    fn write_file_empty_data_is_noop() {
        let (mut tree, ino) = make_tree_with_file(b"hello");
        let written = tree.write_file(ino, 0, b"").unwrap();
        assert_eq!(written, 0);
        assert_eq!(tree.get(ino).unwrap().attr.size, 5);
    }

    #[test]
    fn write_file_bad_ino_returns_enoent() {
        let mut tree = make_tree();
        assert_errno(
            tree.write_file(9999, 0, b"x").unwrap_err(),
            fuser::Errno::ENOENT,
        );
    }

    #[test]
    fn write_file_on_dir_returns_eisdir() {
        let mut tree = make_tree();
        assert_errno(
            tree.write_file(ROOT_INODE, 0, b"x").unwrap_err(),
            fuser::Errno::EISDIR,
        );
    }

    #[test]
    fn write_file_append_to_existing() {
        let (mut tree, ino) = make_tree_with_file(b"hello");
        let written = tree.write_file(ino, 5, b" world").unwrap();
        assert_eq!(written, 6);
        if let FsContent::File(fd) = &tree.get(ino).unwrap().content {
            assert_eq!(fd.data(), b"hello world");
        } else {
            panic!("expected File content");
        }
    }

    #[test]
    fn write_file_overwrites_clean_becomes_dirty() {
        // Get a Clean node: create -> write -> mark_clean
        let mut tree = make_tree();
        let node = tree
            .create_file(ROOT_INODE, OsStr::new("f.txt"), 0o644)
            .unwrap();
        let ino = node.attr.ino.0;
        tree.write_file(ino, 0, b"abc").unwrap();
        tree.mark_clean();
        // Now content is Clean{[a,b,c], cipher:None}
        assert!(matches!(
            tree.get(ino).unwrap().content,
            FsContent::File(FileData::Clean { .. })
        ));
        // Write -> should transition to Dirty
        let written = tree.write_file(ino, 1, b"XY").unwrap();
        assert_eq!(written, 2);
        let node = tree.get(ino).unwrap();
        if let FsContent::File(FileData::Dirty { data, cipher }) = &node.content {
            assert_eq!(data.as_slice(), b"aXY");
            assert!(cipher.is_none());
        } else {
            panic!("expected Dirty, got something else");
        }
    }

    #[test]
    fn write_file_empty_data_noop_on_clean() {
        // Get Clean state: create -> write -> mark_clean
        let mut tree = make_tree();
        let node = tree
            .create_file(ROOT_INODE, OsStr::new("h.txt"), 0o644)
            .unwrap();
        let ino = node.attr.ino.0;
        tree.write_file(ino, 0, b"abc").unwrap();
        tree.mark_clean();
        // Verify Clean
        assert!(matches!(
            tree.get(ino).unwrap().content,
            FsContent::File(FileData::Clean { .. })
        ));
        tree.mark_clean();
        // Empty write -- should stay Clean, NOT become Dirty
        tree.write_file(ino, 0, b"").unwrap();
        assert!(
            matches!(
                tree.get(ino).unwrap().content,
                FsContent::File(FileData::Clean { .. })
            ),
            "empty write should not transition Clean to Dirty"
        );
    }

    #[test]
    fn write_file_empty_data_does_not_set_dirty() {
        let (mut tree, ino) = make_tree_with_file(b"hello");
        tree.mark_clean();
        assert!(!tree.is_dirty());
        tree.write_file(ino, 0, b"").unwrap();
        assert!(!tree.is_dirty());
    }

    #[test]
    fn write_file_mid_overwrite_preserves_trailing() {
        let (mut tree, ino) = make_tree_with_file(b"hello");
        tree.write_file(ino, 1, b"XY").unwrap();
        if let FsContent::File(fd) = &tree.get(ino).unwrap().content {
            assert_eq!(fd.data(), b"hXYlo");
        } else {
            panic!("expected File content");
        }
    }

    // Equivalent of write_file_on_loaded_from_archive: use mark_clean to
    // simulate the Clean state that archive_io::load would produce.
    #[test]
    fn write_file_on_clean_from_mark_clean() {
        let (mut tree, ino) = make_tree_with_file(b"original");
        tree.mark_clean();
        assert!(matches!(
            tree.get(ino).unwrap().content,
            FsContent::File(FileData::Clean { .. })
        ));
        let written = tree.write_file(ino, 0, b"data").unwrap();
        assert_eq!(written, 4);
        assert!(matches!(
            tree.get(ino).unwrap().content,
            FsContent::File(FileData::Dirty { .. })
        ));
    }

    // ── set_size ────────────────────────────────────────────────────

    #[test]
    fn set_size_truncate() {
        let (mut tree, ino) = make_tree_with_file(b"hello");
        tree.set_size(ino, 3).unwrap();
        let node = tree.get(ino).unwrap();
        assert_eq!(node.attr.size, 3);
        if let FsContent::File(fd) = &node.content {
            assert_eq!(fd.data(), b"hel");
        } else {
            panic!("expected File");
        }
    }

    #[test]
    fn set_size_truncate_to_zero() {
        let (mut tree, ino) = make_tree_with_file(b"hello");
        tree.set_size(ino, 0).unwrap();
        assert_eq!(tree.get(ino).unwrap().attr.size, 0);
        if let FsContent::File(fd) = &tree.get(ino).unwrap().content {
            assert!(fd.data().is_empty());
        } else {
            panic!("expected File");
        }
    }

    #[test]
    fn set_size_extend_zero_pads() {
        let (mut tree, ino) = make_tree_with_file(b"hi");
        tree.set_size(ino, 5).unwrap();
        let node = tree.get(ino).unwrap();
        assert_eq!(node.attr.size, 5);
        if let FsContent::File(fd) = &tree.get(ino).unwrap().content {
            let data = fd.data();
            assert_eq!(&data[..2], b"hi");
            assert_eq!(&data[2..5], &[0, 0, 0]);
        } else {
            panic!("expected File");
        }
    }

    #[test]
    fn set_size_on_dir_returns_eisdir() {
        let mut tree = make_tree();
        assert_errno(
            tree.set_size(ROOT_INODE, 0).unwrap_err(),
            fuser::Errno::EISDIR,
        );
    }

    #[test]
    fn set_size_same_length_noop() {
        let (mut tree, ino) = make_tree_with_file(b"hello");
        tree.set_size(ino, 5).unwrap();
        let node = tree.get(ino).unwrap();
        assert_eq!(node.attr.size, 5);
        if let FsContent::File(fd) = &node.content {
            assert_eq!(fd.data(), b"hello");
        } else {
            panic!("expected File");
        }
    }

    // Equivalent of set_size_on_loaded_from_archive: use mark_clean.
    #[test]
    fn set_size_on_clean_from_mark_clean() {
        let (mut tree, ino) = make_tree_with_file(b"hello");
        tree.mark_clean();
        assert!(matches!(
            tree.get(ino).unwrap().content,
            FsContent::File(FileData::Clean { .. })
        ));
        tree.set_size(ino, 0).unwrap();
        let node = tree.get(ino).unwrap();
        assert_eq!(node.attr.size, 0);
        // Should be Dirty after Clean + truncate
        assert!(matches!(
            node.content,
            FsContent::File(FileData::Dirty { .. })
        ));
    }

    #[test]
    fn set_size_bad_ino_returns_enoent() {
        let mut tree = make_tree();
        let err = tree.set_size(9999, 0).unwrap_err();
        assert!(errno_eq(err, fuser::Errno::ENOENT));
    }

    // ── set_times ───────────────────────────────────────────────────

    #[test]
    fn set_times_specific() {
        let (mut tree, ino) = make_tree_with_file(b"");
        let t1 = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1000);
        let t2 = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(2000);
        tree.set_times(
            ino,
            Some(TimeOrNow::SpecificTime(t1)),
            Some(TimeOrNow::SpecificTime(t2)),
        )
        .unwrap();
        let node = tree.get(ino).unwrap();
        assert_eq!(node.attr.atime, t1);
        assert_eq!(node.attr.mtime, t2);
        assert!(tree.is_dirty());
    }

    #[test]
    fn set_times_bad_ino_returns_enoent() {
        let mut tree = make_tree();
        assert_errno(
            tree.set_times(9999, None, None).unwrap_err(),
            fuser::Errno::ENOENT,
        );
    }

    #[test]
    fn set_times_now_atime_only() {
        let (mut tree, ino) = make_tree_with_file(b"");
        let before = SystemTime::now();
        let mtime_before = tree.get(ino).unwrap().attr.mtime;
        tree.set_times(ino, Some(TimeOrNow::Now), None).unwrap();
        let node = tree.get(ino).unwrap();
        assert!(node.attr.atime >= before);
        assert_eq!(node.attr.mtime, mtime_before);
    }

    #[test]
    fn set_times_mtime_only() {
        let (mut tree, ino) = make_tree_with_file(b"");
        let atime_before = tree.get(ino).unwrap().attr.atime;
        let t2 = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(5000);
        tree.set_times(ino, None, Some(TimeOrNow::SpecificTime(t2)))
            .unwrap();
        let node = tree.get(ino).unwrap();
        assert_eq!(node.attr.atime, atime_before);
        assert_eq!(node.attr.mtime, t2);
    }

    #[test]
    fn set_times_none_none_stays_clean() {
        let (mut tree, ino) = make_tree_with_file(b"");
        tree.mark_clean(); // clear dirty
        assert!(!tree.is_dirty());
        tree.set_times(ino, None, None).unwrap();
        assert!(!tree.is_dirty());
    }

    #[test]
    fn set_times_now_mtime_only() {
        let (mut tree, ino) = make_tree_with_file(b"");
        let before = SystemTime::now();
        tree.set_times(ino, None, Some(TimeOrNow::Now)).unwrap();
        let node = tree.get(ino).unwrap();
        assert!(node.attr.mtime >= before);
    }

    #[test]
    fn set_times_on_directory_succeeds() {
        let mut tree = make_tree();
        let t = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(5000);
        tree.set_times(ROOT_INODE, Some(TimeOrNow::SpecificTime(t)), None)
            .unwrap();
        assert_eq!(tree.get(ROOT_INODE).unwrap().attr.atime, t);
    }

    // Equivalent of set_times_loaded_no_state_change: use mark_clean to get Clean.
    #[test]
    fn set_times_clean_no_state_change() {
        let (mut tree, ino) = make_tree_with_file(b"data");
        tree.mark_clean();
        assert!(matches!(
            tree.get(ino).unwrap().content,
            FsContent::File(FileData::Clean { .. })
        ));
        let t = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(7000);
        tree.set_times(ino, Some(TimeOrNow::SpecificTime(t)), None)
            .unwrap();
        // Should still be Clean (set_times doesn't change content state)
        assert!(matches!(
            tree.get(ino).unwrap().content,
            FsContent::File(FileData::Clean { .. })
        ));
        assert_eq!(tree.get(ino).unwrap().attr.atime, t);
    }

    // ── make_dir ────────────────────────────────────────────────────

    #[test]
    fn make_dir_happy_path() {
        let mut tree = make_tree();
        let node = tree
            .make_dir(ROOT_INODE, OsStr::new("mydir"), 0o755, 0)
            .unwrap();
        assert_eq!(node.attr.nlink, 2);
        assert_eq!(node.attr.kind, FileType::Directory);
        assert!(tree.is_dirty());
    }

    #[test]
    fn make_dir_increments_parent_nlink() {
        let mut tree = make_tree();
        let parent_nlink_before = tree.get(ROOT_INODE).unwrap().attr.nlink;
        tree.make_dir(ROOT_INODE, OsStr::new("mydir"), 0o755, 0)
            .unwrap();
        let parent_nlink_after = tree.get(ROOT_INODE).unwrap().attr.nlink;
        assert_eq!(parent_nlink_after, parent_nlink_before + 1);
    }

    #[test]
    fn make_dir_existing_name_returns_eexist() {
        let mut tree = make_tree();
        tree.make_dir(ROOT_INODE, OsStr::new("d"), 0o755, 0)
            .unwrap();
        assert_errno(
            tree.make_dir(ROOT_INODE, OsStr::new("d"), 0o755, 0)
                .unwrap_err(),
            fuser::Errno::EEXIST,
        );
    }

    #[test]
    fn make_dir_applies_umask() {
        let mut tree = make_tree();
        let node = tree
            .make_dir(ROOT_INODE, OsStr::new("masked"), 0o777, 0o022)
            .unwrap();
        assert_eq!(node.attr.perm, 0o755);
    }

    #[test]
    fn make_dir_bad_parent_returns_enoent() {
        let mut tree = make_tree();
        let err = tree.make_dir(9999, OsStr::new("x"), 0o755, 0).unwrap_err();
        assert!(errno_eq(err, fuser::Errno::ENOENT));
    }

    #[test]
    fn make_dir_parent_is_file_returns_enotdir() {
        let (mut tree, file_ino) = make_tree_with_file(b"");
        let err = tree
            .make_dir(file_ino, OsStr::new("x"), 0o755, 0)
            .unwrap_err();
        assert!(errno_eq(err, fuser::Errno::ENOTDIR));
    }

    // ── unlink ──────────────────────────────────────────────────────

    #[test]
    fn unlink_removes_file_completely() {
        let (mut tree, ino) = make_tree_with_file(b"hi");
        tree.unlink(ROOT_INODE, OsStr::new("test.txt")).unwrap();
        assert!(tree.get(ino).is_none());
        let children: Vec<_> = tree.children(ROOT_INODE).unwrap().collect();
        assert!(children.is_empty());
        assert!(tree.is_dirty());
    }

    #[test]
    fn unlink_nonexistent_returns_enoent() {
        let mut tree = make_tree();
        assert_errno(
            tree.unlink(ROOT_INODE, OsStr::new("ghost.txt"))
                .unwrap_err(),
            fuser::Errno::ENOENT,
        );
    }

    #[test]
    fn unlink_directory_returns_eperm_or_eisdir() {
        let mut tree = make_tree();
        tree.make_dir(ROOT_INODE, OsStr::new("d"), 0o755, 0)
            .unwrap();
        let err = tree.unlink(ROOT_INODE, OsStr::new("d")).unwrap_err();
        assert!(
            errno_eq(err, fuser::Errno::EPERM) || errno_eq(err, fuser::Errno::EISDIR),
            "expected EPERM or EISDIR, got {:?}",
            err
        );
    }

    #[test]
    fn unlink_then_recreate_same_name() {
        let (mut tree, old_ino) = make_tree_with_file(b"hi");
        tree.unlink(ROOT_INODE, OsStr::new("test.txt")).unwrap();
        let new_node = tree
            .create_file(ROOT_INODE, OsStr::new("test.txt"), 0o644)
            .unwrap();
        assert_ne!(new_node.attr.ino.0, old_ino);
        let children: Vec<_> = tree.children(ROOT_INODE).unwrap().collect();
        assert_eq!(children.len(), 1);
    }

    #[test]
    fn unlink_recreate_data_isolation() {
        let (mut tree, old_ino) = make_tree_with_file(b"old data");
        tree.unlink(ROOT_INODE, OsStr::new("test.txt")).unwrap();
        let new_node = tree
            .create_file(ROOT_INODE, OsStr::new("test.txt"), 0o644)
            .unwrap();
        let new_ino = new_node.attr.ino.0;
        tree.write_file(new_ino, 0, b"new data").unwrap();
        // Verify old data is completely gone
        assert!(tree.get(old_ino).is_none());
        if let FsContent::File(fd) = &tree.get(new_ino).unwrap().content {
            assert_eq!(fd.data(), b"new data");
        } else {
            panic!("expected File");
        }
    }

    // ── write + truncate interaction ────────────────────────────────

    #[test]
    fn write_then_truncate_preserves_prefix() {
        let (mut tree, ino) = make_tree_with_file(b"hello world");
        tree.set_size(ino, 5).unwrap();
        let node = tree.get(ino).unwrap();
        assert_eq!(node.attr.size, 5);
        if let FsContent::File(fd) = &node.content {
            assert_eq!(fd.data(), b"hello");
        } else {
            panic!("expected File");
        }
    }

    // ── mark_clean ──────────────────────────────────────────────────

    #[test]
    fn mark_clean_transitions_new_to_clean() {
        let (mut tree, ino) = make_tree_with_file(b"hello");
        assert!(tree.is_dirty());
        tree.mark_clean();
        assert!(!tree.is_dirty());
        let node = tree.get(ino).unwrap();
        assert!(matches!(
            node.content,
            FsContent::File(FileData::Clean { .. })
        ));
    }

    #[test]
    fn mark_clean_new_with_password_gets_cipher() {
        let mut tree =
            FileTree::new_for_test(PathBuf::from("/tmp/t.pna"), Some("secret".to_string()));
        let node = tree
            .create_file(ROOT_INODE, OsStr::new("f.txt"), 0o644)
            .unwrap();
        let ino = node.attr.ino.0;
        tree.mark_clean();
        let node = tree.get(ino).unwrap();
        if let FsContent::File(FileData::Clean { cipher, .. }) = &node.content {
            assert!(cipher.is_some());
        } else {
            panic!("expected Clean");
        }
    }

    #[test]
    fn mark_clean_new_without_password_gets_no_cipher() {
        let (mut tree, ino) = make_tree_with_file(b"data");
        tree.mark_clean();
        let node = tree.get(ino).unwrap();
        if let FsContent::File(FileData::Clean { cipher, .. }) = &node.content {
            assert!(cipher.is_none());
        } else {
            panic!("expected Clean");
        }
    }

    #[test]
    fn mark_clean_dirty_to_clean() {
        // Get Dirty state: create -> write -> mark_clean -> write again
        let mut tree = make_tree();
        let node = tree
            .create_file(ROOT_INODE, OsStr::new("g.txt"), 0o644)
            .unwrap();
        let ino = node.attr.ino.0;
        tree.write_file(ino, 0, b"abc").unwrap();
        tree.mark_clean();
        // Now Clean; write again -> Dirty
        tree.write_file(ino, 0, b"XYZ").unwrap();
        assert!(matches!(
            tree.get(ino).unwrap().content,
            FsContent::File(FileData::Dirty { .. })
        ));
        tree.mark_clean();
        assert!(!tree.is_dirty());
        let node = tree.get(ino).unwrap();
        if let FsContent::File(FileData::Clean { data, cipher }) = &node.content {
            assert_eq!(data.as_slice(), b"XYZ");
            assert!(cipher.is_none());
        } else {
            panic!("expected Clean");
        }
    }

    #[test]
    fn mark_clean_dirty_with_cipher_preserves_cipher() {
        // Build a tree with password so cipher gets set
        let mut tree =
            FileTree::new_for_test(PathBuf::from("/tmp/t.pna"), Some("secret".to_string()));
        // Create file -> write -> mark_clean: New+pwd -> Clean{cipher=Some(AES-CTR)}
        let node = tree
            .create_file(ROOT_INODE, OsStr::new("enc.txt"), 0o644)
            .unwrap();
        let ino = node.attr.ino.0;
        tree.write_file(ino, 0, b"hello").unwrap();
        tree.mark_clean();
        // Now Clean{data=[hello], cipher=Some(AES-CTR)}
        let cipher_cfg = {
            let node = tree.get(ino).unwrap();
            match &node.content {
                FsContent::File(FileData::Clean {
                    cipher: Some(c), ..
                }) => CipherConfig {
                    encryption: c.encryption,
                    cipher_mode: c.cipher_mode,
                },
                _ => panic!("expected Clean with cipher"),
            }
        };
        // Write again: Clean -> Dirty{data, cipher=Some(AES-CTR)}
        tree.write_file(ino, 0, b"world").unwrap();
        assert!(matches!(
            tree.get(ino).unwrap().content,
            FsContent::File(FileData::Dirty {
                cipher: Some(_),
                ..
            })
        ));
        // mark_clean: Dirty{cipher=Some} -> Clean{cipher=Some}, cipher preserved
        tree.mark_clean();
        assert!(!tree.is_dirty());
        let node = tree.get(ino).unwrap();
        match &node.content {
            FsContent::File(FileData::Clean {
                data,
                cipher: Some(c),
            }) => {
                assert_eq!(data.as_slice(), b"world");
                assert_eq!(c.encryption as u8, cipher_cfg.encryption as u8);
                assert_eq!(c.cipher_mode as u8, cipher_cfg.cipher_mode as u8);
            }
            _ => panic!("expected Clean with cipher"),
        }
    }

    // Equivalent of mark_clean_loaded_stays_loaded: use mark_clean twice.
    #[test]
    fn mark_clean_clean_stays_clean() {
        let (mut tree, ino) = make_tree_with_file(b"lazy");
        tree.mark_clean();
        assert!(matches!(
            tree.get(ino).unwrap().content,
            FsContent::File(FileData::Clean { .. })
        ));
        tree.mark_clean();
        assert!(!tree.is_dirty());
        // Should still be Clean
        assert!(matches!(
            tree.get(ino).unwrap().content,
            FsContent::File(FileData::Clean { .. })
        ));
    }

    // ── ENOSYS stubs ────────────────────────────────────────────────

    #[test]
    fn rmdir_returns_enosys() {
        let mut tree = make_tree();
        assert_errno(
            tree.rmdir(ROOT_INODE, OsStr::new("x")).unwrap_err(),
            fuser::Errno::ENOSYS,
        );
    }

    #[test]
    fn rename_returns_enosys() {
        let mut tree = make_tree();
        assert_errno(
            tree.rename(
                ROOT_INODE,
                OsStr::new("a"),
                ROOT_INODE,
                OsStr::new("b"),
                fuser::RenameFlags::empty(),
            )
            .unwrap_err(),
            fuser::Errno::ENOSYS,
        );
    }

    #[test]
    fn set_attr_full_returns_enosys() {
        let mut tree = make_tree();
        assert_errno(
            tree.set_attr_full(ROOT_INODE, None, None, None)
                .unwrap_err(),
            fuser::Errno::ENOSYS,
        );
    }

    #[test]
    fn create_symlink_returns_enosys() {
        let mut tree = make_tree();
        assert_errno(
            tree.create_symlink(ROOT_INODE, OsStr::new("s"), Path::new("/target"))
                .unwrap_err(),
            fuser::Errno::ENOSYS,
        );
    }

    #[test]
    fn create_hardlink_returns_enosys() {
        let mut tree = make_tree();
        assert_errno(
            tree.create_hardlink(ROOT_INODE, OsStr::new("h"), 2)
                .unwrap_err(),
            fuser::Errno::ENOSYS,
        );
    }

    #[test]
    fn set_xattr_returns_enosys() {
        let mut tree = make_tree();
        assert_errno(
            tree.set_xattr(ROOT_INODE, OsStr::new("x"), b"v")
                .unwrap_err(),
            fuser::Errno::ENOSYS,
        );
    }

    #[test]
    fn remove_xattr_returns_enosys() {
        let mut tree = make_tree();
        assert_errno(
            tree.remove_xattr(ROOT_INODE, OsStr::new("x")).unwrap_err(),
            fuser::Errno::ENOSYS,
        );
    }

    // ── FileData state machine unit tests ───────────────────────────

    #[test]
    fn promote_to_dirty_from_clean() {
        let mut fd = FileData::Clean {
            data: vec![1, 2, 3],
            cipher: None,
        };
        fd.promote_to_dirty();
        assert!(matches!(fd, FileData::Dirty { .. }));
        if let FileData::Dirty { data, cipher } = &fd {
            assert_eq!(data, &[1, 2, 3]);
            assert!(cipher.is_none());
        }
    }

    #[test]
    fn promote_to_dirty_noop_on_dirty() {
        let mut fd = FileData::Dirty {
            data: vec![4, 5],
            cipher: None,
        };
        fd.promote_to_dirty();
        assert!(matches!(fd, FileData::Dirty { .. }));
    }

    #[test]
    fn promote_to_dirty_noop_on_new() {
        let mut fd = FileData::New(vec![6, 7]);
        fd.promote_to_dirty();
        assert!(matches!(fd, FileData::New(_)));
    }

    #[test]
    fn make_clean_from_dirty() {
        let mut fd = FileData::Dirty {
            data: vec![10, 20],
            cipher: Some(CipherConfig {
                encryption: pna::Encryption::Aes,
                cipher_mode: pna::CipherMode::CTR,
            }),
        };
        fd.make_clean(false);
        if let FileData::Clean { data, cipher } = &fd {
            assert_eq!(data, &[10, 20]);
            assert!(cipher.is_some());
        } else {
            panic!("expected Clean");
        }
    }

    #[test]
    fn make_clean_from_new_with_password() {
        let mut fd = FileData::New(vec![30]);
        fd.make_clean(true);
        if let FileData::Clean { data, cipher } = &fd {
            assert_eq!(data, &[30]);
            assert!(cipher.is_some());
        } else {
            panic!("expected Clean");
        }
    }

    #[test]
    fn make_clean_from_new_without_password() {
        let mut fd = FileData::New(vec![40]);
        fd.make_clean(false);
        if let FileData::Clean { data, cipher } = &fd {
            assert_eq!(data, &[40]);
            assert!(cipher.is_none());
        } else {
            panic!("expected Clean");
        }
    }

    #[test]
    fn make_clean_noop_on_clean() {
        let mut fd = FileData::Clean {
            data: vec![50],
            cipher: None,
        };
        fd.make_clean(true);
        if let FileData::Clean { data, cipher } = &fd {
            assert_eq!(data, &[50]);
            // cipher should remain None since Clean is a no-op
            assert!(cipher.is_none());
        } else {
            panic!("expected Clean");
        }
    }

    #[test]
    fn data_and_data_mut_accessors() {
        let mut fd = FileData::New(vec![1, 2, 3]);
        assert_eq!(fd.data(), &[1, 2, 3]);
        fd.data_mut().push(4);
        assert_eq!(fd.data(), &[1, 2, 3, 4]);
    }

    // ── collect_dfs ─────────────────────────────────────────────────

    #[test]
    fn collect_dfs_empty_tree() {
        let tree = make_tree();
        let result = tree.collect_dfs();
        assert!(result.is_empty());
    }

    #[test]
    fn collect_dfs_single_file() {
        let mut tree = make_tree();
        tree.create_file(ROOT_INODE, OsStr::new("a.txt"), 0o644)
            .unwrap();
        let result = tree.collect_dfs();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].2, "a.txt");
    }

    #[test]
    fn collect_dfs_nested_dirs() {
        let mut tree = make_tree();
        let dir = tree
            .make_dir(ROOT_INODE, OsStr::new("sub"), 0o755, 0)
            .unwrap();
        let dir_ino = dir.attr.ino.0;
        tree.create_file(dir_ino, OsStr::new("file.txt"), 0o644)
            .unwrap();
        let result = tree.collect_dfs();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].2, "sub");
        assert_eq!(result[1].2, "sub/file.txt");
    }

    #[test]
    fn collect_dfs_sorted_order() {
        let mut tree = make_tree();
        tree.create_file(ROOT_INODE, OsStr::new("z.txt"), 0o644)
            .unwrap();
        tree.create_file(ROOT_INODE, OsStr::new("a.txt"), 0o644)
            .unwrap();
        let result = tree.collect_dfs();
        assert_eq!(result[0].2, "a.txt");
        assert_eq!(result[1].2, "z.txt");
    }

    // ── make_dir_all ────────────────────────────────────────────────

    #[test]
    fn make_dir_all_creates_nested() {
        let mut tree = make_tree();
        let ino = tree.make_dir_all(Path::new("a/b/c"), ROOT_INODE).unwrap();
        assert!(tree.get(ino).is_some());
        let a = tree.lookup_child(ROOT_INODE, OsStr::new("a")).unwrap();
        let b = tree.lookup_child(a.attr.ino.0, OsStr::new("b")).unwrap();
        let c = tree.lookup_child(b.attr.ino.0, OsStr::new("c")).unwrap();
        assert_eq!(c.attr.ino.0, ino);
    }

    #[test]
    fn make_dir_all_reuses_existing() {
        let mut tree = make_tree();
        tree.make_dir(ROOT_INODE, OsStr::new("existing"), 0o755, 0)
            .unwrap();
        let ino = tree
            .make_dir_all(Path::new("existing/new"), ROOT_INODE)
            .unwrap();
        // "existing" was reused (not duplicated), "new" was created
        let children: Vec<_> = tree.children(ROOT_INODE).unwrap().collect();
        assert_eq!(children.len(), 1); // only "existing", not a duplicate
        assert!(
            tree.lookup_child(children[0].attr.ino.0, OsStr::new("new"))
                .is_some()
        );
    }

    // ── BTreeMap sorted children order ────────────────────────────

    #[test]
    fn children_returns_sorted_order() {
        let mut tree = make_tree();
        tree.create_file(ROOT_INODE, OsStr::new("z.txt"), 0o644)
            .unwrap();
        tree.create_file(ROOT_INODE, OsStr::new("a.txt"), 0o644)
            .unwrap();
        tree.create_file(ROOT_INODE, OsStr::new("m.txt"), 0o644)
            .unwrap();
        let names: Vec<_> = tree
            .children(ROOT_INODE)
            .unwrap()
            .map(|n| n.name.to_string_lossy().into_owned())
            .collect();
        assert_eq!(names, vec!["a.txt", "m.txt", "z.txt"]);
    }

    // ── Parent back-pointer correctness ───────────────────────────

    #[test]
    fn parent_pointer_set_on_create_file() {
        let mut tree = make_tree();
        tree.create_file(ROOT_INODE, OsStr::new("f.txt"), 0o644)
            .unwrap();
        let child = tree.lookup_child(ROOT_INODE, OsStr::new("f.txt")).unwrap();
        assert_eq!(child.parent, Some(ROOT_INODE));
    }

    #[test]
    fn parent_pointer_for_nested_file() {
        let mut tree = make_tree();
        let dir = tree
            .make_dir(ROOT_INODE, OsStr::new("sub"), 0o755, 0)
            .unwrap();
        let dir_ino = dir.attr.ino.0;
        tree.create_file(dir_ino, OsStr::new("f.txt"), 0o644)
            .unwrap();
        let child = tree.lookup_child(dir_ino, OsStr::new("f.txt")).unwrap();
        assert_eq!(child.parent, Some(dir_ino));
    }

    #[test]
    fn parent_pointer_for_make_dir() {
        let mut tree = make_tree();
        let dir = tree
            .make_dir(ROOT_INODE, OsStr::new("sub"), 0o755, 0)
            .unwrap();
        assert_eq!(dir.parent, Some(ROOT_INODE));
    }

    // ── is_dirty on non-file mutations ────────────────────────────

    #[test]
    fn is_dirty_after_make_dir() {
        let mut tree = make_tree();
        tree.make_dir(ROOT_INODE, OsStr::new("d"), 0o755, 0)
            .unwrap();
        assert!(tree.is_dirty());
    }

    #[test]
    fn is_dirty_after_unlink() {
        let mut tree = make_tree();
        tree.create_file(ROOT_INODE, OsStr::new("f.txt"), 0o644)
            .unwrap();
        tree.mark_clean();
        tree.unlink(ROOT_INODE, OsStr::new("f.txt")).unwrap();
        assert!(tree.is_dirty());
    }

    #[test]
    fn is_dirty_after_set_times_with_value() {
        let mut tree = make_tree();
        tree.create_file(ROOT_INODE, OsStr::new("f.txt"), 0o644)
            .unwrap();
        tree.mark_clean();
        let t = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1000);
        let ino = tree
            .lookup_child(ROOT_INODE, OsStr::new("f.txt"))
            .unwrap()
            .attr
            .ino
            .0;
        tree.set_times(ino, Some(TimeOrNow::SpecificTime(t)), None)
            .unwrap();
        assert!(tree.is_dirty());
    }

    #[test]
    fn not_dirty_after_set_times_none_none() {
        let mut tree = make_tree();
        tree.create_file(ROOT_INODE, OsStr::new("f.txt"), 0o644)
            .unwrap();
        tree.mark_clean();
        let ino = tree
            .lookup_child(ROOT_INODE, OsStr::new("f.txt"))
            .unwrap()
            .attr
            .ino
            .0;
        tree.set_times(ino, None, None).unwrap();
        assert!(!tree.is_dirty());
    }
}
