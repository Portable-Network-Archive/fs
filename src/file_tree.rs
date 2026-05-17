use fuser::{Errno, FileAttr, FileType, INodeNo, TimeOrNow};
#[cfg(unix)]
use nix::unistd::{Gid, Group, Uid, User};
use pna::Permission;
use std::collections::{BTreeMap, HashMap};
use std::ffi::{OsStr, OsString};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::SystemTime;

pub(crate) type Inode = u64;
pub(crate) const ROOT_INODE: Inode = 1;

/// POSIX permission-bit mask: the low 12 bits cover rwx + setuid/setgid/sticky.
const PERM_MASK: u32 = 0o7777;

/// Owner identity used when creating a new inode. FUSE handlers fill it from
/// `Request::{uid, gid}`; archive load fills it from PNA permission metadata.
#[derive(Copy, Clone, Debug)]
pub(crate) struct Owner {
    pub uid: u32,
    pub gid: u32,
}

impl Owner {
    pub(crate) fn new(uid: u32, gid: u32) -> Self {
        Self { uid, gid }
    }
}

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
    pub(crate) fn default_for_password() -> Self {
        Self {
            encryption: pna::Encryption::Aes,
            cipher_mode: pna::CipherMode::CTR,
        }
    }

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
                    Some(CipherConfig::default_for_password())
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

    pub(crate) fn cipher(&self) -> Option<&CipherConfig> {
        match self {
            FileData::Clean { cipher, .. } | FileData::Dirty { cipher, .. } => cipher.as_ref(),
            FileData::New(_) => None,
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

    pub(crate) fn remove(&mut self, name: &OsStr) -> Option<Inode> {
        self.children.remove(name)
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&OsString, &Inode)> {
        self.children.iter()
    }
}

/// Identifies the four POSIX "special" inode types pnafs supports
/// in-memory only. PNA's archive format does not yet have a `DataKind`
/// for any of these, so a `Special` node round-trips through save/load
/// only as long as the mount stays up.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum SpecialKind {
    BlockDevice,
    CharDevice,
    Fifo,
    Socket,
}

/// What `mknod` was asked to make. Encodes every accepted `S_IFMT` slice
/// of a `mode_t` so the FUSE handler can dispatch exhaustively without an
/// "unreachable" fallback.
pub(crate) enum NodeKind {
    Regular,
    Special(SpecialKind),
}

impl NodeKind {
    /// Decode the file-type bits of a `mode_t`. `mode_t` slices we don't
    /// allow (directory, symlink, anything we don't recognise) yield
    /// `EINVAL` so callers get a single error path.
    pub(crate) fn from_mode(mode: u32) -> Result<Self, Errno> {
        match mode & libc::S_IFMT {
            // Some callers pass S_IFREG as 0; treat both as a regular file.
            0 | libc::S_IFREG => Ok(Self::Regular),
            libc::S_IFBLK => Ok(Self::Special(SpecialKind::BlockDevice)),
            libc::S_IFCHR => Ok(Self::Special(SpecialKind::CharDevice)),
            libc::S_IFIFO => Ok(Self::Special(SpecialKind::Fifo)),
            libc::S_IFSOCK => Ok(Self::Special(SpecialKind::Socket)),
            _ => Err(Errno::EINVAL),
        }
    }
}

impl SpecialKind {
    pub(crate) fn to_file_type(self) -> FileType {
        match self {
            Self::BlockDevice => FileType::BlockDevice,
            Self::CharDevice => FileType::CharDevice,
            Self::Fifo => FileType::NamedPipe,
            Self::Socket => FileType::Socket,
        }
    }
}

/// In-memory metadata for a special file. Carries `rdev` for block/char
/// devices; fifos and sockets use a zero `rdev` per POSIX.
///
/// Forward-compatibility note: when the PNA on-disk format gains a
/// representation for these node types, only `archive_io::save` and
/// `archive_io::load` need to learn about it — the rest of the FS already
/// treats these as first-class via `FsContent::Special`.
#[derive(Clone, Debug)]
pub(crate) struct SpecialFile {
    pub kind: SpecialKind,
    pub rdev: u32,
}

pub(crate) enum FsContent {
    Directory(DirContent),
    File(FileData),
    Symlink(OsString),
    Special(SpecialFile),
}

pub(crate) struct FsNode {
    pub name: OsString,
    pub parent: Option<Inode>,
    pub attr: FileAttr,
    pub content: FsContent,
    /// Extended attributes for this inode. Names are UTF-8 strings to match
    /// PNA's wire type (`pna::ExtendedAttribute::name() -> &str`); on the
    /// FUSE side `getxattr`/`listxattr` accept arbitrary `OsStr` but every
    /// xattr we currently materialise comes from archive load, which is
    /// already `String`. The map is ordered (`BTreeMap`) so the save path
    /// serialises attributes in a deterministic order — `HashMap`'s
    /// run-to-run iteration order would otherwise make two saves of the
    /// same tree produce byte-different archives, which the round-trip
    /// property test (`plain_save_is_byte_identical_when_replayed`)
    /// caught.
    pub xattrs: BTreeMap<String, Vec<u8>>,
    /// Live count of file descriptors held by clients for this inode.
    /// `(attr.nlink, open_count)` drives the inode lifecycle:
    ///
    /// - `nlink ≥ 1`                       — normal, reachable from a dir entry
    /// - `nlink == 0 && open_count > 0`    — orphan: still readable through
    ///   any existing fd, invisible to lookups, never persisted on save
    /// - `nlink == 0 && open_count == 0`   — freed (removed from `inodes`)
    ///
    /// Atomic so `open` / `release` can run under a read lock on the tree
    /// (the orphan-collection path upgrades to a write lock when transitioning
    /// to `0`). Not persisted — PNA has no per-inode fd schema.
    pub open_count: AtomicU32,
}

impl FsNode {
    /// Construct a fresh node ready for `insert_node`. Centralises the
    /// `now`-stamped timestamps, fixed defaults (`blksize`, `flags`,
    /// `xattrs`, `open_count`), and the perm-bit mask so the per-kind
    /// create paths only have to supply what differs.
    #[allow(clippy::too_many_arguments)]
    fn new_node(
        ino: Inode,
        name: OsString,
        kind: FileType,
        perm: u16,
        owner: Owner,
        content: FsContent,
        rdev: u32,
        size: u64,
        nlink: u32,
    ) -> Self {
        let now = SystemTime::now();
        FsNode {
            name,
            parent: None,
            xattrs: BTreeMap::new(),
            open_count: AtomicU32::new(0),
            content,
            attr: FileAttr {
                ino: INodeNo(ino),
                size,
                blocks: 1,
                atime: now,
                mtime: now,
                ctime: now,
                crtime: now,
                kind,
                perm: perm & PERM_MASK as u16,
                nlink,
                uid: owner.uid,
                gid: owner.gid,
                rdev,
                blksize: 512,
                flags: 0,
            },
        }
    }
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

    /// Walk the directory tree starting from root and resolve `path` to its
    /// inode. Returns `None` if any component is missing or hits a
    /// non-directory along the way. Leading `/` and `.` components are
    /// ignored, matching how PNA stores entry paths.
    pub(crate) fn resolve_path(&self, path: &Path) -> Option<Inode> {
        let mut current = ROOT_INODE;
        for component in path.components() {
            let name = component.as_os_str();
            if name == OsStr::new("/") || name == OsStr::new(".") {
                continue;
            }
            current = self.lookup_child(current, name)?.attr.ino.0;
        }
        Some(current)
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

    /// Iterate `(name, node)` pairs for a directory's children. The `name` is
    /// taken from the directory's own map, not from `FsNode.name`, so callers
    /// see the correct entry name even when an inode is multiply linked.
    pub(crate) fn children(
        &self,
        parent: Inode,
    ) -> Option<impl Iterator<Item = (&OsStr, &FsNode)>> {
        let parent_node = self.inodes.get(&parent)?;
        match &parent_node.content {
            FsContent::Directory(dir) => {
                let inodes = &self.inodes;
                Some(dir.iter().filter_map(move |(name, &ino)| {
                    inodes.get(&ino).map(|n| (name.as_os_str(), n))
                }))
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

    /// Validate that `parent` exists, is a directory, and has no child named
    /// `name`. Used at the start of every create_* path.
    fn validate_parent_for_create(&self, parent: Inode, name: &OsStr) -> Result<(), Errno> {
        let parent_node = self.inodes.get(&parent).ok_or(Errno::ENOENT)?;
        if !matches!(parent_node.content, FsContent::Directory(_)) {
            return Err(Errno::ENOTDIR);
        }
        if self.lookup_child(parent, name).is_some() {
            return Err(Errno::EEXIST);
        }
        Ok(())
    }

    /// Touch parent timestamps (mtime + ctime) after structural changes.
    fn touch_parent(&mut self, parent: Inode, now: SystemTime) {
        if let Some(p) = self.inodes.get_mut(&parent) {
            p.attr.mtime = now;
            p.attr.ctime = now;
        }
    }

    /// Drop one reference to `ino`. If other hardlinks remain, decrement
    /// `nlink` and bump `ctime`. Otherwise mark it nlink == 0; the inode is
    /// freed iff no fd is currently open against it (otherwise it lives on
    /// as an orphan, freed by the matching `release_open`).
    fn drop_link(&mut self, ino: Inode, now: SystemTime) {
        if let Some(node) = self.inodes.get_mut(&ino) {
            if node.attr.nlink > 1 {
                node.attr.nlink -= 1;
                node.attr.ctime = now;
                return;
            }
            node.attr.nlink = 0;
            node.attr.ctime = now;
        }
        self.maybe_free_inode(ino);
    }

    /// Free `ino` iff it has no remaining links **and** no open fds. The
    /// single chokepoint for orphan-collection — `drop_link` calls it under
    /// the write lock after decrementing `nlink`; `release_open` schedules
    /// a write-locked call when its decrement transitions to zero.
    fn maybe_free_inode(&mut self, ino: Inode) {
        if let Some(node) = self.inodes.get(&ino)
            && node.attr.nlink == 0
            && node.open_count.load(Ordering::Acquire) == 0
        {
            self.inodes.remove(&ino);
        }
    }

    /// Bump the open-fd counter for `ino`. Takes `&self` so callers can hold
    /// the tree's read lock; the counter is atomic. Pairs 1:1 with
    /// `release_open`.
    pub(crate) fn bump_open(&self, ino: Inode) -> Result<(), Errno> {
        let node = self.inodes.get(&ino).ok_or(Errno::ENOENT)?;
        node.open_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Drain the open-fd counter for `ino`. Returns `true` when the caller
    /// must upgrade to a write lock and call `try_free_orphan` — i.e. when
    /// this fd was the last and the inode is already unlinked. Defensive
    /// against unknown inodes (no-op + `false`).
    #[must_use]
    pub(crate) fn release_open(&self, ino: Inode) -> bool {
        let Some(node) = self.inodes.get(&ino) else {
            return false;
        };
        let prev = node.open_count.fetch_sub(1, Ordering::AcqRel);
        prev == 1 && node.attr.nlink == 0
    }

    /// Free `ino` if it's a fully-released orphan. Re-checks the
    /// `(nlink, open_count)` pair under the write lock so a racing reopen
    /// between the prior `release_open` and this call doesn't lose data.
    pub(crate) fn try_free_orphan(&mut self, ino: Inode) {
        self.maybe_free_inode(ino);
    }

    pub(crate) fn create_file(
        &mut self,
        parent: Inode,
        name: &OsStr,
        mode: u32,
        owner: Owner,
    ) -> Result<&FsNode, Errno> {
        self.validate_parent_for_create(parent, name)?;
        let ino = self.next_inode();
        let node = FsNode::new_node(
            ino,
            name.to_owned(),
            FileType::RegularFile,
            mode as u16,
            owner,
            FsContent::File(FileData::New(Vec::new())),
            0,
            0,
            1,
        );
        self.insert_node(node, Some(parent))
            .map_err(|_| Errno::EIO)?;
        self.touch_parent(parent, SystemTime::now());
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
            FsContent::Symlink(_) | FsContent::Special(_) => return Err(Errno::EINVAL),
            FsContent::File(fd) => fd,
        };
        file_data.promote_to_dirty();
        let buf = file_data.data_mut();
        if offset > buf.len() {
            buf.resize(offset, 0);
        }
        let end = offset.checked_add(data.len()).ok_or(Errno::EFBIG)?;
        if end > buf.len() {
            buf.resize(end, 0);
        }
        buf[offset..end].copy_from_slice(data);
        node.attr.size = buf.len() as u64;
        let now = SystemTime::now();
        node.attr.mtime = now;
        node.attr.ctime = now;
        self.dirty = true;
        Ok(data.len())
    }

    /// POSIX `fallocate` / `posix_fallocate`. The `mode` mirrors the Linux
    /// flags accepted by `fallocate(2)`:
    ///
    /// * `0` — grow buffer to `offset + length` with zero-fill, update `attr.size`
    /// * `FALLOC_FL_KEEP_SIZE` (0x01) — same as 0 but preserve size
    /// * `FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE` (0x03) — zero out `[offset, offset+length)`
    /// * `FALLOC_FL_ZERO_RANGE` (0x10) — zero out range, grow if needed
    ///
    /// Anything else (collapse / insert range, or `PUNCH_HOLE` without
    /// `KEEP_SIZE`) returns the matching errno so callers can fall back.
    pub(crate) fn fallocate(
        &mut self,
        ino: Inode,
        offset: u64,
        length: u64,
        mode: i32,
    ) -> Result<(), Errno> {
        const FALLOC_FL_KEEP_SIZE: i32 = 0x01;
        const FALLOC_FL_PUNCH_HOLE: i32 = 0x02;
        const FALLOC_FL_ZERO_RANGE: i32 = 0x10;
        const SUPPORTED: i32 = FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE | FALLOC_FL_ZERO_RANGE;

        if length == 0 {
            return Err(Errno::EINVAL);
        }
        if mode & !SUPPORTED != 0 {
            return Err(Errno::ENOTSUP);
        }
        let punch = mode & FALLOC_FL_PUNCH_HOLE != 0;
        let zero_range = mode & FALLOC_FL_ZERO_RANGE != 0;
        let keep_size = mode & FALLOC_FL_KEEP_SIZE != 0;
        if punch && !keep_size {
            return Err(Errno::EINVAL);
        }

        let node = self.inodes.get_mut(&ino).ok_or(Errno::ENOENT)?;
        let file_data = match &mut node.content {
            FsContent::Directory(_) => return Err(Errno::EISDIR),
            FsContent::Symlink(_) | FsContent::Special(_) => return Err(Errno::EINVAL),
            FsContent::File(fd) => fd,
        };
        let offset = usize::try_from(offset).map_err(|_| Errno::EFBIG)?;
        let length = usize::try_from(length).map_err(|_| Errno::EFBIG)?;
        let end = offset.checked_add(length).ok_or(Errno::EFBIG)?;

        file_data.promote_to_dirty();
        let buf = file_data.data_mut();

        if punch {
            // PUNCH_HOLE only operates within current size; never grows.
            let logical_size = node.attr.size as usize;
            let zero_end = end.min(logical_size).min(buf.len());
            if offset < zero_end {
                buf[offset..zero_end].fill(0);
            }
        } else if zero_range {
            // Grow first, then zero the whole range — `resize` only zeros
            // the *new* tail, so any pre-existing bytes inside [offset..end)
            // need an explicit fill.
            if end > buf.len() {
                buf.resize(end, 0);
            }
            if offset < end {
                buf[offset..end].fill(0);
            }
            if !keep_size && end as u64 > node.attr.size {
                node.attr.size = end as u64;
            }
        } else {
            // Plain fallocate: grow buffer, optionally update size.
            if end > buf.len() {
                buf.resize(end, 0);
            }
            if !keep_size && end as u64 > node.attr.size {
                node.attr.size = end as u64;
            }
        }

        let now = SystemTime::now();
        node.attr.mtime = now;
        node.attr.ctime = now;
        self.dirty = true;
        Ok(())
    }

    /// POSIX `copy_file_range(2)`. Returns the number of bytes actually
    /// copied, which may be less than `len` (truncated at source EOF) or
    /// zero (if `src_offset` is at or past EOF). Source and destination
    /// may be the same inode, including overlapping ranges — we read into
    /// an owned buffer first, so any overlap resolves cleanly.
    pub(crate) fn copy_file_range(
        &mut self,
        src_ino: Inode,
        src_offset: u64,
        dst_ino: Inode,
        dst_offset: u64,
        len: u64,
    ) -> Result<usize, Errno> {
        let src_offset = usize::try_from(src_offset).map_err(|_| Errno::EFBIG)?;
        let dst_offset_u64 = dst_offset;
        let len = usize::try_from(len).map_err(|_| Errno::EFBIG)?;
        if len == 0 {
            return Ok(0);
        }

        let chunk: Vec<u8> = {
            let src_node = self.inodes.get(&src_ino).ok_or(Errno::ENOENT)?;
            let src_data = match &src_node.content {
                FsContent::File(fd) => fd.data(),
                FsContent::Directory(_) => return Err(Errno::EISDIR),
                FsContent::Symlink(_) | FsContent::Special(_) => return Err(Errno::EINVAL),
            };
            // Validate dst before touching anything.
            let dst_node = self.inodes.get(&dst_ino).ok_or(Errno::ENOENT)?;
            match &dst_node.content {
                FsContent::File(_) => {}
                FsContent::Directory(_) => return Err(Errno::EISDIR),
                FsContent::Symlink(_) | FsContent::Special(_) => return Err(Errno::EINVAL),
            }

            let avail = src_data.len();
            if src_offset >= avail {
                return Ok(0);
            }
            let copy_end = (src_offset + len).min(avail);
            src_data[src_offset..copy_end].to_vec()
        };

        if chunk.is_empty() {
            return Ok(0);
        }
        // write_file does the sparse-grow + dirty bookkeeping for us.
        self.write_file(dst_ino, dst_offset_u64, &chunk)
    }

    pub(crate) fn set_size(&mut self, ino: Inode, size: u64) -> Result<(), Errno> {
        let node = self.inodes.get_mut(&ino).ok_or(Errno::ENOENT)?;
        if size == node.attr.size {
            return Ok(());
        }
        let file_data = match &mut node.content {
            FsContent::Directory(_) => return Err(Errno::EISDIR),
            FsContent::Symlink(_) | FsContent::Special(_) => return Err(Errno::EINVAL),
            FsContent::File(fd) => fd,
        };
        file_data.promote_to_dirty();
        let size_usize = usize::try_from(size).map_err(|_| Errno::EFBIG)?;
        let buf = file_data.data_mut();
        buf.resize(size_usize, 0);
        node.attr.size = size;
        let now = SystemTime::now();
        node.attr.mtime = now;
        node.attr.ctime = now;
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
            // Per POSIX, modifying atime/mtime updates ctime too.
            node.attr.ctime = SystemTime::now();
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
        owner: Owner,
    ) -> Result<&FsNode, Errno> {
        self.validate_parent_for_create(parent, name)?;
        let ino = self.next_inode();
        let effective_mode = (mode & !umask) as u16;
        let node = FsNode::new_node(
            ino,
            name.to_owned(),
            FileType::Directory,
            effective_mode,
            owner,
            FsContent::Directory(DirContent::new()),
            0,
            512,
            2,
        );
        self.insert_node(node, Some(parent))
            .map_err(|_| Errno::EIO)?;
        if let Some(parent_node) = self.inodes.get_mut(&parent) {
            parent_node.attr.nlink += 1;
        }
        self.touch_parent(parent, SystemTime::now());
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
        let now = SystemTime::now();
        if let Some(parent_node) = self.inodes.get_mut(&parent)
            && let FsContent::Directory(dir) = &mut parent_node.content
        {
            dir.remove(name);
        }
        self.touch_parent(parent, now);
        self.drop_link(target_ino, now);
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

    pub(crate) fn rmdir(&mut self, parent: Inode, name: &OsStr) -> Result<(), Errno> {
        let target_ino = {
            let parent_node = self.inodes.get(&parent).ok_or(Errno::ENOENT)?;
            let dir = match &parent_node.content {
                FsContent::Directory(d) => d,
                _ => return Err(Errno::ENOTDIR),
            };
            dir.get(name).ok_or(Errno::ENOENT)?
        };
        if target_ino == ROOT_INODE {
            return Err(Errno::EBUSY);
        }
        let target = self.inodes.get(&target_ino).ok_or(Errno::ENOENT)?;
        let target_dir = match &target.content {
            FsContent::Directory(d) => d,
            _ => return Err(Errno::ENOTDIR),
        };
        if target_dir.iter().next().is_some() {
            return Err(Errno::ENOTEMPTY);
        }
        let now = SystemTime::now();
        if let Some(parent_node) = self.inodes.get_mut(&parent) {
            if let FsContent::Directory(dir) = &mut parent_node.content {
                dir.remove(name);
            }
            if parent_node.attr.nlink > 0 {
                parent_node.attr.nlink -= 1;
            }
        }
        self.touch_parent(parent, now);
        self.inodes.remove(&target_ino);
        self.dirty = true;
        Ok(())
    }

    pub(crate) fn rename(
        &mut self,
        old_parent: Inode,
        old_name: &OsStr,
        new_parent: Inode,
        new_name: &OsStr,
        flags: fuser::RenameFlags,
    ) -> Result<(), Errno> {
        // RENAME_NOREPLACE and RENAME_EXCHANGE are mutually exclusive per
        // the rename(2) man page. RENAME_WHITEOUT needs an overlay
        // backend we don't provide; reject it explicitly.
        let noreplace = flags.contains(fuser::RenameFlags::RENAME_NOREPLACE);
        let exchange = flags.contains(fuser::RenameFlags::RENAME_EXCHANGE);
        if flags.contains(fuser::RenameFlags::RENAME_WHITEOUT) {
            return Err(Errno::ENOTSUP);
        }
        if noreplace && exchange {
            return Err(Errno::EINVAL);
        }

        let source_ino = {
            let p = self.inodes.get(&old_parent).ok_or(Errno::ENOENT)?;
            let dir = match &p.content {
                FsContent::Directory(d) => d,
                _ => return Err(Errno::ENOTDIR),
            };
            dir.get(old_name).ok_or(Errno::ENOENT)?
        };
        {
            let np = self.inodes.get(&new_parent).ok_or(Errno::ENOENT)?;
            if !matches!(np.content, FsContent::Directory(_)) {
                return Err(Errno::ENOTDIR);
            }
        }
        // No-op when source and destination resolve to the same path
        // (POSIX rename(2)). EXCHANGE on the same path is also a no-op.
        if old_parent == new_parent && old_name == new_name {
            return Ok(());
        }

        let dest_existing = {
            let np = self.inodes.get(&new_parent).unwrap();
            if let FsContent::Directory(d) = &np.content {
                d.get(new_name)
            } else {
                None
            }
        };

        if exchange {
            // Both endpoints must already exist for EXCHANGE.
            let dest_ino = dest_existing.ok_or(Errno::ENOENT)?;
            if dest_ino == source_ino {
                return Ok(());
            }
            return self.rename_exchange(
                old_parent, old_name, new_parent, new_name, source_ino, dest_ino,
            );
        }

        let source_is_dir = matches!(
            self.inodes.get(&source_ino).map(|n| &n.content),
            Some(FsContent::Directory(_))
        );

        // If renaming a directory across parents, refuse to move it into its
        // own descendant subtree (would create a cycle).
        if source_is_dir && old_parent != new_parent {
            let mut walker = Some(new_parent);
            while let Some(cur) = walker {
                if cur == source_ino {
                    return Err(Errno::EINVAL);
                }
                walker = self.inodes.get(&cur).and_then(|n| n.parent);
            }
        }

        if let Some(dest_ino) = dest_existing {
            if dest_ino == source_ino {
                return Ok(());
            }
            if noreplace {
                return Err(Errno::EEXIST);
            }
            let dest_node = self.inodes.get(&dest_ino).ok_or(Errno::ENOENT)?;
            let dest_is_dir = matches!(dest_node.content, FsContent::Directory(_));
            match (source_is_dir, dest_is_dir) {
                (false, true) => return Err(Errno::EISDIR),
                (true, false) => return Err(Errno::ENOTDIR),
                (true, true) => {
                    // Destination directory must be empty to be replaced.
                    if let FsContent::Directory(d) = &dest_node.content
                        && d.iter().next().is_some()
                    {
                        return Err(Errno::ENOTEMPTY);
                    }
                }
                (false, false) => {}
            }
            let np = self.inodes.get_mut(&new_parent).unwrap();
            if let FsContent::Directory(d) = &mut np.content {
                d.remove(new_name);
            }
            // Cross-parent dir→dir replace loses one subdir from new_parent
            // here; the matching `nlink += 1` later (when source is a dir
            // and old_parent != new_parent) restores the count to a stable
            // total. Same-parent replaces touch one slot on either side of
            // a single parent and net out to zero, so they need no change.
            if dest_is_dir
                && old_parent != new_parent
                && let Some(np) = self.inodes.get_mut(&new_parent)
                && np.attr.nlink > 0
            {
                np.attr.nlink -= 1;
            }
            self.drop_link(dest_ino, SystemTime::now());
        }

        {
            let op = self.inodes.get_mut(&old_parent).unwrap();
            if let FsContent::Directory(d) = &mut op.content {
                d.remove(old_name);
            }
        }
        {
            let np = self.inodes.get_mut(&new_parent).unwrap();
            if let FsContent::Directory(d) = &mut np.content {
                d.insert(new_name.to_owned(), source_ino);
            }
        }
        let now = SystemTime::now();
        if let Some(node) = self.inodes.get_mut(&source_ino) {
            node.name = new_name.to_owned();
            node.parent = Some(new_parent);
            node.attr.ctime = now;
        }
        if source_is_dir && old_parent != new_parent {
            if let Some(op) = self.inodes.get_mut(&old_parent)
                && op.attr.nlink > 0
            {
                op.attr.nlink -= 1;
            }
            if let Some(np) = self.inodes.get_mut(&new_parent) {
                np.attr.nlink += 1;
            }
        }
        self.touch_parent(old_parent, now);
        self.touch_parent(new_parent, now);
        self.dirty = true;
        Ok(())
    }

    /// Atomic swap of two existing dir-entries (RENAME_EXCHANGE). nlink
    /// totals on the parents are unchanged: both sides just point at the
    /// other inode under the same name. ctime bumps on both inodes.
    fn rename_exchange(
        &mut self,
        old_parent: Inode,
        old_name: &OsStr,
        new_parent: Inode,
        new_name: &OsStr,
        source_ino: Inode,
        dest_ino: Inode,
    ) -> Result<(), Errno> {
        // Forbid moving a directory into its own subtree on either side.
        for (mover, into) in [(source_ino, new_parent), (dest_ino, old_parent)] {
            let is_dir = matches!(
                self.inodes.get(&mover).map(|n| &n.content),
                Some(FsContent::Directory(_))
            );
            if !is_dir {
                continue;
            }
            let mut walker = Some(into);
            while let Some(cur) = walker {
                if cur == mover {
                    return Err(Errno::EINVAL);
                }
                walker = self.inodes.get(&cur).and_then(|n| n.parent);
            }
        }
        if let Some(op) = self.inodes.get_mut(&old_parent)
            && let FsContent::Directory(d) = &mut op.content
        {
            d.insert(old_name.to_owned(), dest_ino);
        }
        if let Some(np) = self.inodes.get_mut(&new_parent)
            && let FsContent::Directory(d) = &mut np.content
        {
            d.insert(new_name.to_owned(), source_ino);
        }
        let now = SystemTime::now();
        if let Some(n) = self.inodes.get_mut(&source_ino) {
            n.name = new_name.to_owned();
            n.parent = Some(new_parent);
            n.attr.ctime = now;
        }
        if let Some(n) = self.inodes.get_mut(&dest_ino) {
            n.name = old_name.to_owned();
            n.parent = Some(old_parent);
            n.attr.ctime = now;
        }
        self.touch_parent(old_parent, now);
        if old_parent != new_parent {
            self.touch_parent(new_parent, now);
        }
        self.dirty = true;
        Ok(())
    }

    pub(crate) fn set_attr_full(
        &mut self,
        ino: Inode,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
    ) -> Result<(), Errno> {
        let node = self.inodes.get_mut(&ino).ok_or(Errno::ENOENT)?;
        let mut changed = false;
        if let Some(m) = mode {
            node.attr.perm = (m & 0o7777) as u16;
            changed = true;
        }
        if let Some(u) = uid {
            node.attr.uid = u;
            changed = true;
        }
        if let Some(g) = gid {
            node.attr.gid = g;
            changed = true;
        }
        if changed {
            node.attr.ctime = SystemTime::now();
            self.dirty = true;
        }
        Ok(())
    }

    /// Set or replace an extended attribute.
    ///
    /// `flags` follows Linux `setxattr(2)`:
    /// * `XATTR_CREATE` — fail with `EEXIST` if the name already exists.
    /// * `XATTR_REPLACE` — fail with `ENODATA` if the name does not yet exist.
    /// * `XATTR_CREATE | XATTR_REPLACE` is `EINVAL`.
    /// * `0` — replace if present, create otherwise.
    pub(crate) fn setxattr(
        &mut self,
        ino: Inode,
        name: &str,
        value: &[u8],
        flags: i32,
    ) -> Result<(), Errno> {
        if flags & libc::XATTR_CREATE != 0 && flags & libc::XATTR_REPLACE != 0 {
            return Err(Errno::EINVAL);
        }
        let node = self.inodes.get_mut(&ino).ok_or(Errno::ENOENT)?;
        let exists = node.xattrs.contains_key(name);
        if flags & libc::XATTR_CREATE != 0 && exists {
            return Err(Errno::EEXIST);
        }
        if flags & libc::XATTR_REPLACE != 0 && !exists {
            return Err(Errno::ENODATA);
        }
        node.xattrs.insert(name.to_owned(), value.to_vec());
        node.attr.ctime = SystemTime::now();
        self.dirty = true;
        Ok(())
    }

    /// Remove an extended attribute. Returns `ENODATA` if the attribute is
    /// not set; `ENOENT` if the inode does not exist.
    pub(crate) fn removexattr(&mut self, ino: Inode, name: &str) -> Result<(), Errno> {
        let node = self.inodes.get_mut(&ino).ok_or(Errno::ENOENT)?;
        if node.xattrs.remove(name).is_none() {
            return Err(Errno::ENODATA);
        }
        node.attr.ctime = SystemTime::now();
        self.dirty = true;
        Ok(())
    }

    pub(crate) fn create_symlink(
        &mut self,
        parent: Inode,
        name: &OsStr,
        target: &std::path::Path,
        owner: Owner,
    ) -> Result<&FsNode, Errno> {
        self.validate_parent_for_create(parent, name)?;
        let target_os: OsString = target.as_os_str().to_owned();
        let size = target_os.len() as u64;
        let ino = self.next_inode();
        let node = FsNode::new_node(
            ino,
            name.to_owned(),
            FileType::Symlink,
            0o777,
            owner,
            FsContent::Symlink(target_os),
            0,
            size,
            1,
        );
        self.insert_node(node, Some(parent))
            .map_err(|_| Errno::EIO)?;
        self.touch_parent(parent, SystemTime::now());
        self.dirty = true;
        Ok(self.inodes.get(&ino).unwrap())
    }

    /// Create a special file (block device, char device, fifo, or socket).
    ///
    /// Forward-compatibility note: pnafs keeps these in memory only because
    /// PNA's archive format does not yet have a `DataKind` for them; once it
    /// does, the only code that needs to change is `archive_io::save` /
    /// `archive_io::load` (this routine and the FUSE handler are agnostic to
    /// whether the node is persisted).
    pub(crate) fn create_special(
        &mut self,
        parent: Inode,
        name: &OsStr,
        kind: SpecialKind,
        mode: u16,
        rdev: u32,
        owner: Owner,
    ) -> Result<&FsNode, Errno> {
        self.validate_parent_for_create(parent, name)?;
        let ino = self.next_inode();
        let node = FsNode::new_node(
            ino,
            name.to_owned(),
            kind.to_file_type(),
            mode,
            owner,
            FsContent::Special(SpecialFile { kind, rdev }),
            rdev,
            0,
            1,
        );
        self.insert_node(node, Some(parent))
            .map_err(|_| Errno::EIO)?;
        self.touch_parent(parent, SystemTime::now());
        self.dirty = true;
        Ok(self.inodes.get(&ino).unwrap())
    }

    /// Add a directory entry under `(parent, name)` that points at the same
    /// inode as an existing entry (the "source"). The source must be a
    /// non-directory; the destination must not already exist. On success the
    /// shared node's `nlink` is incremented and its `ctime` is updated, and
    /// the parent directory's timestamps are bumped.
    pub(crate) fn create_hardlink(
        &mut self,
        parent: Inode,
        name: &OsStr,
        source: Inode,
    ) -> Result<&FsNode, Errno> {
        let parent_node = self.inodes.get(&parent).ok_or(Errno::ENOENT)?;
        if !matches!(parent_node.content, FsContent::Directory(_)) {
            return Err(Errno::ENOTDIR);
        }
        if self.lookup_child(parent, name).is_some() {
            return Err(Errno::EEXIST);
        }
        // POSIX: hardlinking a directory is EPERM.
        let source_node = self.inodes.get(&source).ok_or(Errno::ENOENT)?;
        if matches!(source_node.content, FsContent::Directory(_)) {
            return Err(Errno::EPERM);
        }
        let now = SystemTime::now();
        if let Some(parent_mut) = self.inodes.get_mut(&parent)
            && let FsContent::Directory(dir) = &mut parent_mut.content
        {
            dir.insert(name.to_owned(), source);
        }
        self.touch_parent(parent, now);
        if let Some(src_mut) = self.inodes.get_mut(&source) {
            src_mut.attr.nlink += 1;
            src_mut.attr.ctime = now;
        }
        self.dirty = true;
        Ok(self.inodes.get(&source).unwrap())
    }

    // ── Traversal / bulk helpers ───────────────────────────────────

    /// Return every node except root in pre-order traversal (children sorted
    /// lexicographically by name) together with its full archive path
    /// (e.g. `"dir/subdir/file.txt"`).
    pub(crate) fn collect_dfs(&self) -> Vec<(Inode, &FsNode, String)> {
        let mut result = Vec::new();
        if let Some(root) = self.inodes.get(&ROOT_INODE)
            && let FsContent::Directory(ref dir) = root.content
        {
            self.collect_dfs_recurse(dir, &mut result, &mut String::new());
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

    /// POSIX: every directory's `nlink` is `2 + #subdirectories` (`.` self
    /// link plus one `..` from each child directory). The archive load path
    /// builds the tree out of unrelated PNA entries and has no convenient
    /// place to maintain that invariant inline, so we recompute it once at
    /// the end of `load()`.
    pub(crate) fn recompute_directory_nlinks(&mut self) {
        let counts: Vec<(Inode, u32)> = self
            .inodes
            .iter()
            .filter_map(|(ino, n)| {
                let FsContent::Directory(d) = &n.content else {
                    return None;
                };
                let subdirs = d
                    .iter()
                    .filter(|(_, child_ino)| {
                        matches!(
                            self.inodes.get(*child_ino).map(|n| &n.content),
                            Some(FsContent::Directory(_))
                        )
                    })
                    .count();
                Some((*ino, 2 + subdirs as u32))
            })
            .collect();
        for (ino, nlink) in counts {
            if let Some(node) = self.inodes.get_mut(&ino) {
                node.attr.nlink = nlink;
            }
        }
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

/// Build an [`FsNode`] representing an empty directory. Used both for the
/// archive's root node (load) and for synthesized parent dirs in
/// `make_dir_all`. `make_dir` has its own path that goes through
/// `FsNode::new_node` directly so it can honour the request's umask + uid.
pub(crate) fn make_dir_node(ino: Inode, name: OsString) -> FsNode {
    FsNode::new_node(
        ino,
        name,
        FileType::Directory,
        0o775,
        Owner::new(current_uid(), current_gid()),
        FsContent::Directory(DirContent::new()),
        0,
        512,
        2,
    )
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

/// Resolve the UID for a PNA permission entry. If the archive's
/// `uname` or numeric uid resolves to a local user we prefer that
/// (handles uid remapping across systems where the same user has a
/// different id). Otherwise the numeric uid from the archive is
/// authoritative — silently substituting the running process's uid
/// would lose the saved owner across a save → load cycle. Only when
/// no `Permission` is attached at all do we fall back to the caller's
/// uid.
pub(crate) fn get_uid(permission: Option<&Permission>) -> u32 {
    #[cfg(unix)]
    {
        match permission {
            Some(p) => search_owner(p.uname(), p.uid()).map_or(p.uid() as u32, |u| u.uid.as_raw()),
            None => Uid::current().as_raw(),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = permission;
        0
    }
}

/// Resolve the GID for a PNA permission entry. Mirrors [`get_uid`]:
/// archive's numeric gid is authoritative when neither `gname` nor the
/// numeric id resolves locally; the process gid only applies when no
/// permission record is present.
pub(crate) fn get_gid(permission: Option<&Permission>) -> u32 {
    #[cfg(unix)]
    {
        match permission {
            Some(p) => search_group(p.gname(), p.gid()).map_or(p.gid() as u32, |g| g.gid.as_raw()),
            None => Gid::current().as_raw(),
        }
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
            .create_file(ROOT_INODE, OsStr::new("test.txt"), 0o644, Owner::new(0, 0))
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
            .create_file(ROOT_INODE, OsStr::new("a.txt"), 0o644, Owner::new(0, 0))
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
        tree.create_file(ROOT_INODE, OsStr::new("a.txt"), 0o644, Owner::new(0, 0))
            .unwrap();
        let err = tree
            .create_file(ROOT_INODE, OsStr::new("a.txt"), 0o644, Owner::new(0, 0))
            .unwrap_err();
        assert_errno(err, fuser::Errno::EEXIST);
    }

    #[test]
    fn create_file_bad_parent_returns_enoent() {
        let mut tree = make_tree();
        let err = tree
            .create_file(9999, OsStr::new("x.txt"), 0o644, Owner::new(0, 0))
            .unwrap_err();
        assert_errno(err, fuser::Errno::ENOENT);
    }

    #[test]
    fn create_file_parent_is_file_returns_enotdir() {
        let (mut tree, file_ino) = make_tree_with_file(b"");
        let err = tree
            .create_file(file_ino, OsStr::new("x.txt"), 0o644, Owner::new(0, 0))
            .unwrap_err();
        assert_errno(err, fuser::Errno::ENOTDIR);
    }

    #[test]
    fn create_file_in_subdir() {
        let mut tree = make_tree();
        let subdir = tree
            .make_dir(ROOT_INODE, OsStr::new("sub"), 0o755, 0, Owner::new(0, 0))
            .unwrap();
        let subdir_ino = subdir.attr.ino.0;
        let node = tree
            .create_file(
                subdir_ino,
                OsStr::new("nested.txt"),
                0o644,
                Owner::new(0, 0),
            )
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
            .create_file(ROOT_INODE, OsStr::new("f.txt"), 0o644, Owner::new(0, 0))
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
            .create_file(ROOT_INODE, OsStr::new("h.txt"), 0o644, Owner::new(0, 0))
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
            .make_dir(ROOT_INODE, OsStr::new("mydir"), 0o755, 0, Owner::new(0, 0))
            .unwrap();
        assert_eq!(node.attr.nlink, 2);
        assert_eq!(node.attr.kind, FileType::Directory);
        assert!(tree.is_dirty());
    }

    #[test]
    fn make_dir_increments_parent_nlink() {
        let mut tree = make_tree();
        let parent_nlink_before = tree.get(ROOT_INODE).unwrap().attr.nlink;
        tree.make_dir(ROOT_INODE, OsStr::new("mydir"), 0o755, 0, Owner::new(0, 0))
            .unwrap();
        let parent_nlink_after = tree.get(ROOT_INODE).unwrap().attr.nlink;
        assert_eq!(parent_nlink_after, parent_nlink_before + 1);
    }

    #[test]
    fn make_dir_existing_name_returns_eexist() {
        let mut tree = make_tree();
        tree.make_dir(ROOT_INODE, OsStr::new("d"), 0o755, 0, Owner::new(0, 0))
            .unwrap();
        assert_errno(
            tree.make_dir(ROOT_INODE, OsStr::new("d"), 0o755, 0, Owner::new(0, 0))
                .unwrap_err(),
            fuser::Errno::EEXIST,
        );
    }

    #[test]
    fn make_dir_applies_umask() {
        let mut tree = make_tree();
        let node = tree
            .make_dir(
                ROOT_INODE,
                OsStr::new("masked"),
                0o777,
                0o022,
                Owner::new(0, 0),
            )
            .unwrap();
        assert_eq!(node.attr.perm, 0o755);
    }

    #[test]
    fn make_dir_bad_parent_returns_enoent() {
        let mut tree = make_tree();
        let err = tree
            .make_dir(9999, OsStr::new("x"), 0o755, 0, Owner::new(0, 0))
            .unwrap_err();
        assert!(errno_eq(err, fuser::Errno::ENOENT));
    }

    #[test]
    fn make_dir_parent_is_file_returns_enotdir() {
        let (mut tree, file_ino) = make_tree_with_file(b"");
        let err = tree
            .make_dir(file_ino, OsStr::new("x"), 0o755, 0, Owner::new(0, 0))
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
        tree.make_dir(ROOT_INODE, OsStr::new("d"), 0o755, 0, Owner::new(0, 0))
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
            .create_file(ROOT_INODE, OsStr::new("test.txt"), 0o644, Owner::new(0, 0))
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
            .create_file(ROOT_INODE, OsStr::new("test.txt"), 0o644, Owner::new(0, 0))
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
            .create_file(ROOT_INODE, OsStr::new("f.txt"), 0o644, Owner::new(0, 0))
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
            .create_file(ROOT_INODE, OsStr::new("g.txt"), 0o644, Owner::new(0, 0))
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
            .create_file(ROOT_INODE, OsStr::new("enc.txt"), 0o644, Owner::new(0, 0))
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

    // ── rmdir / rename / set_attr_full / create_symlink ─────────────

    #[test]
    fn rmdir_empty_dir_succeeds() {
        let mut tree = make_tree();
        tree.make_dir(ROOT_INODE, OsStr::new("d"), 0o755, 0, Owner::new(0, 0))
            .unwrap();
        tree.rmdir(ROOT_INODE, OsStr::new("d")).unwrap();
        assert!(tree.lookup_child(ROOT_INODE, OsStr::new("d")).is_none());
    }

    #[test]
    fn rmdir_non_empty_returns_enotempty() {
        let mut tree = make_tree();
        let dir = tree
            .make_dir(ROOT_INODE, OsStr::new("d"), 0o755, 0, Owner::new(0, 0))
            .unwrap();
        let dir_ino = dir.attr.ino.0;
        tree.create_file(dir_ino, OsStr::new("f"), 0o644, Owner::new(0, 0))
            .unwrap();
        assert_errno(
            tree.rmdir(ROOT_INODE, OsStr::new("d")).unwrap_err(),
            fuser::Errno::ENOTEMPTY,
        );
    }

    #[test]
    fn rmdir_on_file_returns_enotdir() {
        let (mut tree, _) = make_tree_with_file(b"");
        assert_errno(
            tree.rmdir(ROOT_INODE, OsStr::new("test.txt")).unwrap_err(),
            fuser::Errno::ENOTDIR,
        );
    }

    #[test]
    fn rename_same_parent_succeeds() {
        let (mut tree, _) = make_tree_with_file(b"hi");
        tree.rename(
            ROOT_INODE,
            OsStr::new("test.txt"),
            ROOT_INODE,
            OsStr::new("renamed.txt"),
            fuser::RenameFlags::empty(),
        )
        .unwrap();
        assert!(
            tree.lookup_child(ROOT_INODE, OsStr::new("test.txt"))
                .is_none()
        );
        assert!(
            tree.lookup_child(ROOT_INODE, OsStr::new("renamed.txt"))
                .is_some()
        );
    }

    #[test]
    fn rename_replace_existing_file() {
        let (mut tree, _) = make_tree_with_file(b"src");
        tree.create_file(ROOT_INODE, OsStr::new("dst.txt"), 0o644, Owner::new(0, 0))
            .unwrap();
        tree.rename(
            ROOT_INODE,
            OsStr::new("test.txt"),
            ROOT_INODE,
            OsStr::new("dst.txt"),
            fuser::RenameFlags::empty(),
        )
        .unwrap();
        let node = tree
            .lookup_child(ROOT_INODE, OsStr::new("dst.txt"))
            .unwrap();
        if let FsContent::File(fd) = &node.content {
            assert_eq!(fd.data(), b"src");
        } else {
            panic!("expected file content");
        }
    }

    #[test]
    fn rename_cross_parent_directory() {
        let mut tree = make_tree();
        let a = tree
            .make_dir(ROOT_INODE, OsStr::new("a"), 0o755, 0, Owner::new(0, 0))
            .unwrap();
        let a_ino = a.attr.ino.0;
        let b = tree
            .make_dir(ROOT_INODE, OsStr::new("b"), 0o755, 0, Owner::new(0, 0))
            .unwrap();
        let b_ino = b.attr.ino.0;
        tree.create_file(a_ino, OsStr::new("f"), 0o644, Owner::new(0, 0))
            .unwrap();
        tree.rename(
            a_ino,
            OsStr::new("f"),
            b_ino,
            OsStr::new("f"),
            fuser::RenameFlags::empty(),
        )
        .unwrap();
        assert!(tree.lookup_child(a_ino, OsStr::new("f")).is_none());
        assert!(tree.lookup_child(b_ino, OsStr::new("f")).is_some());
    }

    #[test]
    fn rename_dir_into_descendant_returns_einval() {
        let mut tree = make_tree();
        let outer = tree
            .make_dir(ROOT_INODE, OsStr::new("a"), 0o755, 0, Owner::new(0, 0))
            .unwrap();
        let outer_ino = outer.attr.ino.0;
        let inner = tree
            .make_dir(outer_ino, OsStr::new("b"), 0o755, 0, Owner::new(0, 0))
            .unwrap();
        let inner_ino = inner.attr.ino.0;
        // Try to move 'a' under 'a/b' -> EINVAL.
        assert_errno(
            tree.rename(
                ROOT_INODE,
                OsStr::new("a"),
                inner_ino,
                OsStr::new("a"),
                fuser::RenameFlags::empty(),
            )
            .unwrap_err(),
            fuser::Errno::EINVAL,
        );
    }

    #[test]
    fn rename_replacing_dir_with_dir_keeps_parent_nlink_correct() {
        // Reproducer for a subtle parent-nlink invariant in rename:
        // the dest-existing branch calls `drop_link` on the
        // destination's inode, which decrements the *child's* nlink,
        // but the parent dir's nlink (which counts ".." backrefs from
        // each child dir) must also be adjusted in step. Replacing
        // dir-with-dir under the same parent leaves the child count
        // unchanged, so the parent's nlink must stay where it was.
        // A naive implementation that only handles the child side
        // leaks a `+1` into the parent on every dir-replacing-dir
        // rename.
        let mut tree = make_tree();
        // Setup: /a (subdir), /b (empty subdir to be the rename target).
        let a_ino = tree
            .make_dir(ROOT_INODE, OsStr::new("a"), 0o755, 0, Owner::new(0, 0))
            .unwrap()
            .attr
            .ino
            .0;
        tree.make_dir(ROOT_INODE, OsStr::new("b"), 0o755, 0, Owner::new(0, 0))
            .unwrap();
        let _ = a_ino;
        let parent_nlink_before = tree.get(ROOT_INODE).unwrap().attr.nlink;
        // mv /a /b — replaces dir b with dir a.
        tree.rename(
            ROOT_INODE,
            OsStr::new("a"),
            ROOT_INODE,
            OsStr::new("b"),
            fuser::RenameFlags::empty(),
        )
        .unwrap();
        let parent_nlink_after = tree.get(ROOT_INODE).unwrap().attr.nlink;
        assert_eq!(
            parent_nlink_after, parent_nlink_before,
            "replacing dir-with-dir within the same parent must not change parent nlink"
        );
    }

    #[test]
    fn rename_replacing_dir_across_parents_balances_nlink() {
        // Cross-parent: moving /src/a (dir) onto /dst/b (dir).
        // Expected effect on nlink:
        //   src parent: -1 (loses /src/a as a subdir)
        //   dst parent: unchanged (loses /dst/b as a subdir, gains /dst/a)
        let mut tree = make_tree();
        let src = tree
            .make_dir(ROOT_INODE, OsStr::new("src"), 0o755, 0, Owner::new(0, 0))
            .unwrap()
            .attr
            .ino
            .0;
        let dst = tree
            .make_dir(ROOT_INODE, OsStr::new("dst"), 0o755, 0, Owner::new(0, 0))
            .unwrap()
            .attr
            .ino
            .0;
        tree.make_dir(src, OsStr::new("a"), 0o755, 0, Owner::new(0, 0))
            .unwrap();
        tree.make_dir(dst, OsStr::new("b"), 0o755, 0, Owner::new(0, 0))
            .unwrap();
        let src_before = tree.get(src).unwrap().attr.nlink;
        let dst_before = tree.get(dst).unwrap().attr.nlink;
        tree.rename(
            src,
            OsStr::new("a"),
            dst,
            OsStr::new("b"),
            fuser::RenameFlags::empty(),
        )
        .unwrap();
        assert_eq!(tree.get(src).unwrap().attr.nlink, src_before - 1);
        assert_eq!(tree.get(dst).unwrap().attr.nlink, dst_before);
    }

    #[test]
    fn rename_with_noreplace_returns_eexist_when_dst_exists() {
        // RENAME_NOREPLACE (linux 3.15+): refuse to clobber. Verifies
        // the FUSE flags arg is honoured and not silently dropped.
        let mut tree = make_tree();
        tree.create_file(ROOT_INODE, OsStr::new("a.txt"), 0o644, Owner::new(0, 0))
            .unwrap();
        tree.create_file(ROOT_INODE, OsStr::new("b.txt"), 0o644, Owner::new(0, 0))
            .unwrap();
        let mut flags = fuser::RenameFlags::empty();
        flags.insert(fuser::RenameFlags::RENAME_NOREPLACE);
        assert_errno(
            tree.rename(
                ROOT_INODE,
                OsStr::new("a.txt"),
                ROOT_INODE,
                OsStr::new("b.txt"),
                flags,
            )
            .unwrap_err(),
            fuser::Errno::EEXIST,
        );
        // Both still exist.
        assert!(tree.lookup_child(ROOT_INODE, OsStr::new("a.txt")).is_some());
        assert!(tree.lookup_child(ROOT_INODE, OsStr::new("b.txt")).is_some());
    }

    #[test]
    fn rename_with_exchange_atomically_swaps_two_entries() {
        // RENAME_EXCHANGE (linux 3.15+): the two entries swap ino/contents.
        let mut tree = make_tree();
        let a = tree
            .create_file(ROOT_INODE, OsStr::new("a.txt"), 0o644, Owner::new(0, 0))
            .unwrap()
            .attr
            .ino
            .0;
        tree.write_file(a, 0, b"AAA").unwrap();
        let b = tree
            .create_file(ROOT_INODE, OsStr::new("b.txt"), 0o644, Owner::new(0, 0))
            .unwrap()
            .attr
            .ino
            .0;
        tree.write_file(b, 0, b"BBB").unwrap();
        let mut flags = fuser::RenameFlags::empty();
        flags.insert(fuser::RenameFlags::RENAME_EXCHANGE);
        tree.rename(
            ROOT_INODE,
            OsStr::new("a.txt"),
            ROOT_INODE,
            OsStr::new("b.txt"),
            flags,
        )
        .unwrap();
        // a.txt still exists but now points to inode `b` (the BBB content),
        // and b.txt points to `a` (the AAA content).
        let new_a = tree
            .lookup_child(ROOT_INODE, OsStr::new("a.txt"))
            .unwrap()
            .attr
            .ino
            .0;
        let new_b = tree
            .lookup_child(ROOT_INODE, OsStr::new("b.txt"))
            .unwrap()
            .attr
            .ino
            .0;
        assert_eq!(new_a, b);
        assert_eq!(new_b, a);
        let bytes_a = read_file_data(&tree, new_a);
        assert_eq!(bytes_a, b"BBB");
    }

    // ── setxattr / removexattr ────────────────────────────────────

    #[test]
    fn setxattr_new_value_stores_and_dirties() {
        let (mut tree, ino) = make_tree_with_file(b"x");
        tree.mark_clean();
        tree.setxattr(ino, "user.tag", b"red", 0).unwrap();
        let n = tree.get(ino).unwrap();
        assert_eq!(n.xattrs.get("user.tag").unwrap(), b"red");
        assert!(tree.is_dirty(), "setxattr should dirty the tree");
    }

    #[test]
    fn setxattr_default_replaces_existing() {
        let (mut tree, ino) = make_tree_with_file(b"x");
        tree.setxattr(ino, "user.tag", b"red", 0).unwrap();
        tree.setxattr(ino, "user.tag", b"blue", 0).unwrap();
        assert_eq!(
            tree.get(ino).unwrap().xattrs.get("user.tag").unwrap(),
            b"blue"
        );
    }

    #[test]
    fn setxattr_create_flag_fails_if_exists() {
        let (mut tree, ino) = make_tree_with_file(b"x");
        tree.setxattr(ino, "user.tag", b"red", 0).unwrap();
        let err = tree
            .setxattr(ino, "user.tag", b"blue", libc::XATTR_CREATE)
            .unwrap_err();
        assert_errno(err, Errno::EEXIST);
    }

    #[test]
    fn setxattr_replace_flag_fails_if_missing() {
        let (mut tree, ino) = make_tree_with_file(b"x");
        let err = tree
            .setxattr(ino, "user.tag", b"red", libc::XATTR_REPLACE)
            .unwrap_err();
        assert_errno(err, Errno::ENODATA);
    }

    #[test]
    fn setxattr_both_flags_returns_einval() {
        let (mut tree, ino) = make_tree_with_file(b"x");
        let err = tree
            .setxattr(
                ino,
                "user.tag",
                b"red",
                libc::XATTR_CREATE | libc::XATTR_REPLACE,
            )
            .unwrap_err();
        assert_errno(err, Errno::EINVAL);
    }

    #[test]
    fn setxattr_bad_inode_returns_enoent() {
        let mut tree = make_tree();
        let err = tree.setxattr(9999, "user.tag", b"x", 0).unwrap_err();
        assert_errno(err, Errno::ENOENT);
    }

    #[test]
    fn removexattr_existing_succeeds_and_dirties() {
        let (mut tree, ino) = make_tree_with_file(b"x");
        tree.setxattr(ino, "user.tag", b"red", 0).unwrap();
        tree.mark_clean();
        tree.removexattr(ino, "user.tag").unwrap();
        assert!(tree.get(ino).unwrap().xattrs.is_empty());
        assert!(tree.is_dirty(), "removexattr should dirty the tree");
    }

    #[test]
    fn removexattr_missing_returns_enodata() {
        let (mut tree, ino) = make_tree_with_file(b"x");
        let err = tree.removexattr(ino, "user.tag").unwrap_err();
        assert_errno(err, Errno::ENODATA);
    }

    #[test]
    fn removexattr_bad_inode_returns_enoent() {
        let mut tree = make_tree();
        let err = tree.removexattr(9999, "user.tag").unwrap_err();
        assert_errno(err, Errno::ENOENT);
    }

    #[test]
    fn setxattr_round_trips_through_save_and_load() {
        // End-to-end: setxattr is observable after save → load.
        let dir = tempfile::TempDir::new().unwrap();
        let archive = dir.path().join("xattr.pna");
        std::fs::write(dir.path().join("seed.txt"), b"seed").unwrap();
        // Build an empty plaintext archive so load succeeds.
        let mut a = pna::Archive::write_header(std::fs::File::create(&archive).unwrap()).unwrap();
        let mut b = pna::EntryBuilder::new_file(
            pna::EntryName::from_lossy("doc.txt"),
            pna::WriteOptions::builder().build(),
        )
        .unwrap();
        std::io::Write::write_all(&mut b, b"hello").unwrap();
        a.add_entry(b.build().unwrap()).unwrap();
        a.finalize().unwrap();

        let mut tree = crate::archive_io::load(&archive, None).unwrap();
        let (_, node) = tree
            .children(ROOT_INODE)
            .unwrap()
            .find(|(n, _)| n.to_str() == Some("doc.txt"))
            .unwrap();
        let ino = node.attr.ino.0;
        tree.setxattr(ino, "user.color", b"green", 0).unwrap();
        crate::archive_io::save(&mut tree).unwrap();

        let reloaded = crate::archive_io::load(&archive, None).unwrap();
        let (_, n) = reloaded
            .children(ROOT_INODE)
            .unwrap()
            .find(|(n, _)| n.to_str() == Some("doc.txt"))
            .unwrap();
        assert_eq!(
            reloaded
                .get(n.attr.ino.0)
                .unwrap()
                .xattrs
                .get("user.color")
                .unwrap(),
            b"green"
        );
    }

    // ── get_uid / get_gid fallback ────────────────────────────────

    /// PNA archives store `uname` / `gname` plus numeric ids. When the
    /// archive came from a different host, the names won't resolve
    /// locally and the numeric id may also have no matching entry in
    /// /etc/passwd or /etc/group. In that case the saved numeric id is
    /// authoritative — substituting the *current process's* uid/gid
    /// silently loses the recorded owner on every save → load cycle.
    /// This test pins that behaviour for a uid/gid that won't normally
    /// exist on a CI host.
    #[test]
    fn get_uid_preserves_archive_id_when_name_does_not_resolve() {
        // gname empty + numeric id that is unlikely to exist in /etc/group.
        let permission = pna::Permission::new(
            0xfeed_face as u64,
            String::new(),
            0u64,
            String::new(),
            0o644,
        );
        let uid = get_uid(Some(&permission));
        // Archive uid must round-trip even though there's no local user.
        assert_eq!(uid, 0xfeed_face);
    }

    #[test]
    fn get_gid_preserves_archive_id_when_name_does_not_resolve() {
        let permission = pna::Permission::new(
            0u64,
            String::new(),
            0xdead_beef as u64,
            String::new(),
            0o644,
        );
        let gid = get_gid(Some(&permission));
        assert_eq!(gid, 0xdead_beef);
    }

    /// Sanity check the unchanged path: `None` permission means there's
    /// no archive record to honour, so falling back to the caller's
    /// id is correct. (Observable here only as "the result is *some*
    /// integer" — the actual value depends on the test runner.)
    #[test]
    fn get_uid_falls_back_to_current_when_permission_absent() {
        // Should not panic; value is whatever the test process is.
        let _ = get_uid(None);
        let _ = get_gid(None);
    }

    #[test]
    fn set_attr_full_changes_perm_and_ctime() {
        let (mut tree, ino) = make_tree_with_file(b"x");
        let before = tree.get(ino).unwrap().attr.ctime;
        // Sleep a little so ctime changes detectably.
        std::thread::sleep(std::time::Duration::from_millis(2));
        tree.set_attr_full(ino, Some(0o600), None, None).unwrap();
        let after = tree.get(ino).unwrap();
        assert_eq!(after.attr.perm, 0o600);
        assert!(after.attr.ctime > before, "ctime should advance");
    }

    #[test]
    fn create_symlink_creates_link_node() {
        let mut tree = make_tree();
        let node = tree
            .create_symlink(
                ROOT_INODE,
                OsStr::new("s"),
                Path::new("/target"),
                Owner::new(0, 0),
            )
            .unwrap();
        assert_eq!(node.attr.kind, FileType::Symlink);
        if let FsContent::Symlink(t) = &node.content {
            assert_eq!(t, OsStr::new("/target"));
        } else {
            panic!("expected symlink content");
        }
    }

    // ── ENOSYS stubs (architectural — PNA format limitations) ───────

    #[test]
    fn create_hardlink_increments_nlink() {
        let (mut tree, ino) = make_tree_with_file(b"shared");
        let before = tree.get(ino).unwrap().attr.nlink;
        let node = tree
            .create_hardlink(ROOT_INODE, OsStr::new("link.txt"), ino)
            .unwrap();
        assert_eq!(node.attr.ino.0, ino, "hardlink shares the source inode");
        assert_eq!(node.attr.nlink, before + 1);
        // Both names resolve to the same inode.
        let a = tree
            .lookup_child(ROOT_INODE, OsStr::new("test.txt"))
            .unwrap()
            .attr
            .ino
            .0;
        let b = tree
            .lookup_child(ROOT_INODE, OsStr::new("link.txt"))
            .unwrap()
            .attr
            .ino
            .0;
        assert_eq!(a, b);
    }

    #[test]
    fn create_hardlink_existing_dest_returns_eexist() {
        let (mut tree, ino) = make_tree_with_file(b"x");
        tree.create_file(ROOT_INODE, OsStr::new("dest.txt"), 0o644, Owner::new(0, 0))
            .unwrap();
        assert_errno(
            tree.create_hardlink(ROOT_INODE, OsStr::new("dest.txt"), ino)
                .unwrap_err(),
            fuser::Errno::EEXIST,
        );
    }

    #[test]
    fn create_hardlink_to_directory_returns_eperm() {
        let mut tree = make_tree();
        let dir = tree
            .make_dir(ROOT_INODE, OsStr::new("d"), 0o755, 0, Owner::new(0, 0))
            .unwrap();
        let dir_ino = dir.attr.ino.0;
        assert_errno(
            tree.create_hardlink(ROOT_INODE, OsStr::new("link"), dir_ino)
                .unwrap_err(),
            fuser::Errno::EPERM,
        );
    }

    // ── Special files (block / char / fifo / socket) ─────────────────

    #[test]
    fn create_special_fifo_sets_kind_and_mode() {
        let mut tree = make_tree();
        let node = tree
            .create_special(
                ROOT_INODE,
                OsStr::new("p"),
                SpecialKind::Fifo,
                0o644,
                0,
                Owner::new(1000, 100),
            )
            .unwrap();
        assert_eq!(node.attr.kind, FileType::NamedPipe);
        assert_eq!(node.attr.perm, 0o644);
        assert_eq!(node.attr.uid, 1000);
        assert_eq!(node.attr.gid, 100);
        assert_eq!(node.attr.rdev, 0);
        assert!(matches!(
            node.content,
            FsContent::Special(SpecialFile {
                kind: SpecialKind::Fifo,
                rdev: 0,
            })
        ));
    }

    #[test]
    fn create_special_block_carries_rdev() {
        let mut tree = make_tree();
        let node = tree
            .create_special(
                ROOT_INODE,
                OsStr::new("b"),
                SpecialKind::BlockDevice,
                0o600,
                0x0301,
                Owner::new(0, 0),
            )
            .unwrap();
        assert_eq!(node.attr.kind, FileType::BlockDevice);
        assert_eq!(node.attr.rdev, 0x0301);
    }

    #[test]
    fn create_special_eexist_when_target_taken() {
        let mut tree = make_tree();
        tree.create_file(ROOT_INODE, OsStr::new("dup"), 0o644, Owner::new(0, 0))
            .unwrap();
        assert_errno(
            tree.create_special(
                ROOT_INODE,
                OsStr::new("dup"),
                SpecialKind::Fifo,
                0o644,
                0,
                Owner::new(0, 0),
            )
            .unwrap_err(),
            fuser::Errno::EEXIST,
        );
    }

    #[test]
    fn write_to_special_returns_einval() {
        let mut tree = make_tree();
        let node = tree
            .create_special(
                ROOT_INODE,
                OsStr::new("p"),
                SpecialKind::Fifo,
                0o644,
                0,
                Owner::new(0, 0),
            )
            .unwrap();
        let ino = node.attr.ino.0;
        assert_errno(
            tree.write_file(ino, 0, b"x").unwrap_err(),
            fuser::Errno::EINVAL,
        );
    }

    #[test]
    fn hardlink_to_special_shares_inode_and_bumps_nlink() {
        let mut tree = make_tree();
        let node = tree
            .create_special(
                ROOT_INODE,
                OsStr::new("p"),
                SpecialKind::Fifo,
                0o644,
                0,
                Owner::new(0, 0),
            )
            .unwrap();
        let ino = node.attr.ino.0;
        let linked = tree
            .create_hardlink(ROOT_INODE, OsStr::new("p2"), ino)
            .unwrap();
        assert_eq!(linked.attr.ino.0, ino);
        assert_eq!(linked.attr.nlink, 2);
    }

    // ── Open-fd refcount + orphan inodes ────────────────────────────

    #[test]
    fn unlink_with_open_fd_keeps_inode_as_orphan() {
        let (mut tree, ino) = make_tree_with_file(b"hello");
        tree.bump_open(ino).unwrap();
        tree.unlink(ROOT_INODE, OsStr::new("test.txt")).unwrap();
        // Directory entry is gone…
        assert!(
            tree.lookup_child(ROOT_INODE, OsStr::new("test.txt"))
                .is_none()
        );
        // …but the inode is still reachable through the held fd.
        let node = tree.get(ino).expect("orphan inode must survive unlink");
        assert_eq!(node.attr.nlink, 0);
        assert_eq!(node.open_count.load(Ordering::Relaxed), 1);
        if let FsContent::File(fd) = &node.content {
            assert_eq!(fd.data(), b"hello");
        } else {
            panic!("expected file content on orphan");
        }
    }

    #[test]
    fn release_of_last_fd_frees_orphan_inode() {
        let (mut tree, ino) = make_tree_with_file(b"x");
        tree.bump_open(ino).unwrap();
        tree.unlink(ROOT_INODE, OsStr::new("test.txt")).unwrap();
        assert!(tree.get(ino).is_some(), "orphan must persist while fd held");
        let _ = tree.release_open(ino);
        tree.try_free_orphan(ino);
        assert!(
            tree.get(ino).is_none(),
            "inode must be freed after final release"
        );
    }

    #[test]
    fn release_of_one_of_many_fds_keeps_orphan() {
        let (mut tree, ino) = make_tree_with_file(b"x");
        tree.bump_open(ino).unwrap();
        tree.bump_open(ino).unwrap();
        tree.unlink(ROOT_INODE, OsStr::new("test.txt")).unwrap();
        let _ = tree.release_open(ino);
        tree.try_free_orphan(ino);
        let node = tree.get(ino).expect("still orphaned with 1 fd left");
        assert_eq!(node.open_count.load(Ordering::Relaxed), 1);
        let _ = tree.release_open(ino);
        tree.try_free_orphan(ino);
        assert!(tree.get(ino).is_none());
    }

    #[test]
    fn unlink_without_open_fd_frees_inode_immediately() {
        let (mut tree, ino) = make_tree_with_file(b"x");
        tree.unlink(ROOT_INODE, OsStr::new("test.txt")).unwrap();
        assert!(tree.get(ino).is_none());
    }

    #[test]
    fn release_open_on_unknown_inode_is_noop() {
        let tree = make_tree();
        // Defensive against weird kernel sequencing or replayed releases.
        assert!(!tree.release_open(9999));
    }

    #[test]
    fn rename_overwrite_with_open_fd_orphans_target() {
        let (mut tree, _src_ino) = make_tree_with_file(b"src");
        let dst = tree
            .create_file(ROOT_INODE, OsStr::new("dst.txt"), 0o644, Owner::new(0, 0))
            .unwrap();
        let dst_ino = dst.attr.ino.0;
        tree.bump_open(dst_ino).unwrap();
        tree.rename(
            ROOT_INODE,
            OsStr::new("test.txt"),
            ROOT_INODE,
            OsStr::new("dst.txt"),
            fuser::RenameFlags::empty(),
        )
        .unwrap();
        // The previous "dst.txt" inode lost its dir entry but a fd is open,
        // so it survives as an orphan.
        let orphan = tree.get(dst_ino).expect("overwritten target must orphan");
        assert_eq!(orphan.attr.nlink, 0);
        assert_eq!(orphan.open_count.load(Ordering::Relaxed), 1);
        let _ = tree.release_open(dst_ino);
        tree.try_free_orphan(dst_ino);
        assert!(tree.get(dst_ino).is_none());
    }

    /// End-to-end regression for the unlink-while-open (orphan) lifecycle.
    ///
    /// Walks the full contract documented on `FsNode::open_count`:
    /// create + write, open a handle, unlink the path, the path lookup
    /// then fails (the FUSE `lookup` maps this `None` to `ENOENT`), the
    /// still-open handle keeps reading the pre-unlink content, the final
    /// release frees the orphan, and a save/reload no longer carries the
    /// deleted file. Deterministic and seed-free: a fixed name and fixed
    /// bytes, no `fsstress`, no randomness.
    #[test]
    fn unlink_while_open_orphan_lifecycle_round_trips_through_save() {
        let dir = tempfile::TempDir::new().unwrap();
        let archive = dir.path().join("orphan.pna");

        // Build an archive holding a single file with known content so
        // the load path produces a real (non-test) FileTree to mutate.
        let mut a = pna::Archive::write_header(std::fs::File::create(&archive).unwrap()).unwrap();
        let mut b = pna::EntryBuilder::new_file(
            pna::EntryName::from_lossy("doomed.txt"),
            pna::WriteOptions::builder().build(),
        )
        .unwrap();
        std::io::Write::write_all(&mut b, b"orphan-payload").unwrap();
        a.add_entry(b.build().unwrap()).unwrap();
        a.finalize().unwrap();

        let mut tree = crate::archive_io::load(&archive, None).unwrap();
        let (_, node) = tree
            .children(ROOT_INODE)
            .unwrap()
            .find(|(n, _)| n.to_str() == Some("doomed.txt"))
            .unwrap();
        let ino = node.attr.ino.0;

        // Open a handle, then unlink the only directory entry.
        tree.bump_open(ino).unwrap();
        tree.unlink(ROOT_INODE, OsStr::new("doomed.txt")).unwrap();

        // Path lookup now fails — FUSE `lookup` turns this into ENOENT.
        assert!(
            tree.lookup_child(ROOT_INODE, OsStr::new("doomed.txt"))
                .is_none(),
            "unlinked path must no longer resolve"
        );

        // The held fd keeps the inode alive as an orphan and still reads
        // the content written before the unlink.
        let orphan = tree.get(ino).expect("orphan inode must survive unlink");
        assert_eq!(orphan.attr.nlink, 0);
        assert_eq!(orphan.open_count.load(Ordering::Relaxed), 1);
        match &orphan.content {
            FsContent::File(fd) => assert_eq!(fd.data(), b"orphan-payload"),
            _ => panic!("expected file content on orphan"),
        }

        // Closing the last fd frees the orphan inode.
        let was_last = tree.release_open(ino);
        assert!(was_last, "release of the only fd on an orphan is the last");
        tree.try_free_orphan(ino);
        assert!(
            tree.get(ino).is_none(),
            "orphan must be freed after the final release"
        );

        // Save and reload: the deleted file must be absent from the archive.
        crate::archive_io::save(&tree).unwrap();
        let reloaded = crate::archive_io::load(&archive, None).unwrap();
        assert!(
            reloaded
                .lookup_child(ROOT_INODE, OsStr::new("doomed.txt"))
                .is_none(),
            "deleted file must not reappear after save/reload"
        );
    }

    #[test]
    fn unlink_keeps_inode_when_other_links_exist() {
        let (mut tree, ino) = make_tree_with_file(b"shared");
        tree.create_hardlink(ROOT_INODE, OsStr::new("link.txt"), ino)
            .unwrap();
        // Remove the original; the inode should survive via the hardlink.
        tree.unlink(ROOT_INODE, OsStr::new("test.txt")).unwrap();
        let surviving = tree
            .lookup_child(ROOT_INODE, OsStr::new("link.txt"))
            .unwrap();
        assert_eq!(surviving.attr.ino.0, ino);
        assert_eq!(surviving.attr.nlink, 1);
        if let FsContent::File(fd) = &surviving.content {
            assert_eq!(fd.data(), b"shared");
        } else {
            panic!("expected file content");
        }
    }

    #[test]
    fn unlink_drops_inode_when_last_link_removed() {
        let (mut tree, ino) = make_tree_with_file(b"x");
        tree.create_hardlink(ROOT_INODE, OsStr::new("link.txt"), ino)
            .unwrap();
        tree.unlink(ROOT_INODE, OsStr::new("test.txt")).unwrap();
        tree.unlink(ROOT_INODE, OsStr::new("link.txt")).unwrap();
        assert!(
            tree.get(ino).is_none(),
            "inode must be freed after last link"
        );
    }

    // ── fallocate ────────────────────────────────────────────────────
    //
    // Linux POSIX-fallocate semantics:
    //   mode == 0                                : grow to offset+length, zero-fill, update size
    //   FALLOC_FL_KEEP_SIZE                      : grow buffer, do NOT update size
    //   FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE: zero out [offset, offset+length)
    //   FALLOC_FL_ZERO_RANGE                     : zero out range, grow if needed
    //   FALLOC_FL_PUNCH_HOLE without KEEP_SIZE   : EINVAL
    //   length == 0                              : EINVAL
    //   non-regular file                         : EISDIR / EINVAL
    //   missing inode                            : ENOENT
    //   unknown flag bits                        : ENOTSUP (kernel falls back to manual zero-write)

    fn read_file_data(tree: &FileTree, ino: Inode) -> Vec<u8> {
        if let FsContent::File(fd) = &tree.get(ino).unwrap().content {
            fd.data().to_vec()
        } else {
            panic!("not a regular file");
        }
    }

    #[test]
    fn fallocate_grows_file_with_zero_fill() {
        let (mut tree, ino) = make_tree_with_file(b"abc");
        tree.fallocate(ino, 0, 16, 0).unwrap();
        let data = read_file_data(&tree, ino);
        assert_eq!(data.len(), 16);
        assert_eq!(&data[..3], b"abc");
        assert_eq!(&data[3..], &[0u8; 13]);
        assert_eq!(tree.get(ino).unwrap().attr.size, 16);
    }

    #[test]
    fn fallocate_keep_size_grows_buffer_but_not_size() {
        const FALLOC_FL_KEEP_SIZE: i32 = 0x01;
        let (mut tree, ino) = make_tree_with_file(b"abc");
        tree.fallocate(ino, 0, 16, FALLOC_FL_KEEP_SIZE).unwrap();
        let data = read_file_data(&tree, ino);
        assert_eq!(data.len(), 16, "buffer should be reserved");
        assert_eq!(
            tree.get(ino).unwrap().attr.size,
            3,
            "size stays at original"
        );
    }

    #[test]
    fn fallocate_offset_grows_to_offset_plus_length() {
        let (mut tree, ino) = make_tree_with_file(b"abc");
        tree.fallocate(ino, 10, 5, 0).unwrap();
        let data = read_file_data(&tree, ino);
        assert_eq!(data.len(), 15);
        assert_eq!(&data[..3], b"abc");
        assert_eq!(&data[3..], &[0u8; 12]);
        assert_eq!(tree.get(ino).unwrap().attr.size, 15);
    }

    #[test]
    fn fallocate_punch_hole_zeroes_range_within_file() {
        const FLAGS: i32 = 0x02 | 0x01; // PUNCH_HOLE | KEEP_SIZE
        let (mut tree, ino) = make_tree_with_file(b"AAAABBBBCCCC");
        tree.fallocate(ino, 4, 4, FLAGS).unwrap();
        let data = read_file_data(&tree, ino);
        assert_eq!(data.as_slice(), b"AAAA\0\0\0\0CCCC");
        assert_eq!(tree.get(ino).unwrap().attr.size, 12);
    }

    #[test]
    fn fallocate_punch_hole_past_eof_is_noop_within_size() {
        const FLAGS: i32 = 0x02 | 0x01;
        let (mut tree, ino) = make_tree_with_file(b"abc");
        tree.fallocate(ino, 100, 50, FLAGS).unwrap();
        let data = read_file_data(&tree, ino);
        assert_eq!(data.as_slice(), b"abc", "no growth, no change");
        assert_eq!(tree.get(ino).unwrap().attr.size, 3);
    }

    #[test]
    fn fallocate_punch_hole_without_keep_size_is_einval() {
        const FLAGS: i32 = 0x02; // PUNCH_HOLE alone
        let (mut tree, ino) = make_tree_with_file(b"abc");
        assert_errno(
            tree.fallocate(ino, 0, 3, FLAGS).unwrap_err(),
            fuser::Errno::EINVAL,
        );
    }

    #[test]
    fn fallocate_zero_range_grows_and_zeroes() {
        const FALLOC_FL_ZERO_RANGE: i32 = 0x10;
        let (mut tree, ino) = make_tree_with_file(b"AAAA");
        tree.fallocate(ino, 2, 6, FALLOC_FL_ZERO_RANGE).unwrap();
        let data = read_file_data(&tree, ino);
        assert_eq!(data.len(), 8);
        assert_eq!(&data[..2], b"AA");
        assert_eq!(&data[2..], &[0u8; 6]);
        assert_eq!(tree.get(ino).unwrap().attr.size, 8);
    }

    #[test]
    fn fallocate_zero_length_is_einval() {
        let (mut tree, ino) = make_tree_with_file(b"abc");
        assert_errno(
            tree.fallocate(ino, 0, 0, 0).unwrap_err(),
            fuser::Errno::EINVAL,
        );
    }

    #[test]
    fn fallocate_unknown_flags_returns_enotsup() {
        const FALLOC_FL_COLLAPSE_RANGE: i32 = 0x08;
        let (mut tree, ino) = make_tree_with_file(b"abc");
        assert_errno(
            tree.fallocate(ino, 0, 3, FALLOC_FL_COLLAPSE_RANGE)
                .unwrap_err(),
            fuser::Errno::ENOTSUP,
        );
    }

    #[test]
    fn fallocate_on_directory_returns_eisdir() {
        let mut tree = make_tree();
        assert_errno(
            tree.fallocate(ROOT_INODE, 0, 4, 0).unwrap_err(),
            fuser::Errno::EISDIR,
        );
    }

    #[test]
    fn fallocate_on_unknown_inode_returns_enoent() {
        let mut tree = make_tree();
        assert_errno(
            tree.fallocate(9999, 0, 4, 0).unwrap_err(),
            fuser::Errno::ENOENT,
        );
    }

    #[test]
    fn fallocate_bumps_mtime_and_ctime() {
        let (mut tree, ino) = make_tree_with_file(b"abc");
        let before_m = tree.get(ino).unwrap().attr.mtime;
        let before_c = tree.get(ino).unwrap().attr.ctime;
        std::thread::sleep(std::time::Duration::from_millis(2));
        tree.fallocate(ino, 0, 8, 0).unwrap();
        assert!(tree.get(ino).unwrap().attr.mtime > before_m);
        assert!(tree.get(ino).unwrap().attr.ctime > before_c);
    }

    // ── copy_file_range ──────────────────────────────────────────────
    //
    // Linux semantics (CoW filesystems may copy with reflinks; we just
    // memcpy):
    //   - returns the number of bytes actually copied (may be < len)
    //   - if src_offset >= src_size: returns 0
    //   - copy stops at src EOF — caller can repeat to chain copies
    //   - dst grows to dst_offset + copied_len
    //   - src and dst may be the same inode; ranges may overlap
    //   - directories return EISDIR
    //   - missing inode returns ENOENT

    #[test]
    fn copy_file_range_appends_to_empty_dst() {
        let (mut tree, src) = make_tree_with_file(b"hello world");
        let dst = tree
            .create_file(ROOT_INODE, OsStr::new("dst.txt"), 0o644, Owner::new(0, 0))
            .unwrap()
            .attr
            .ino
            .0;
        let n = tree.copy_file_range(src, 6, dst, 0, 5).unwrap();
        assert_eq!(n, 5);
        assert_eq!(read_file_data(&tree, dst), b"world");
    }

    #[test]
    fn copy_file_range_grows_dst_with_zero_fill() {
        let (mut tree, src) = make_tree_with_file(b"abcdefghij");
        let dst = tree
            .create_file(ROOT_INODE, OsStr::new("dst.txt"), 0o644, Owner::new(0, 0))
            .unwrap()
            .attr
            .ino
            .0;
        let n = tree.copy_file_range(src, 0, dst, 4, 3).unwrap();
        assert_eq!(n, 3);
        let data = read_file_data(&tree, dst);
        assert_eq!(&data[..4], &[0u8; 4]);
        assert_eq!(&data[4..], b"abc");
    }

    #[test]
    fn copy_file_range_truncates_at_src_eof() {
        let (mut tree, src) = make_tree_with_file(b"short");
        let dst = tree
            .create_file(ROOT_INODE, OsStr::new("dst.txt"), 0o644, Owner::new(0, 0))
            .unwrap()
            .attr
            .ino
            .0;
        let n = tree.copy_file_range(src, 2, dst, 0, 100).unwrap();
        assert_eq!(n, 3, "only 3 bytes from offset 2 to EOF");
        assert_eq!(read_file_data(&tree, dst), b"ort");
    }

    #[test]
    fn copy_file_range_src_offset_at_or_past_eof_returns_zero() {
        let (mut tree, src) = make_tree_with_file(b"abc");
        let dst = tree
            .create_file(ROOT_INODE, OsStr::new("dst.txt"), 0o644, Owner::new(0, 0))
            .unwrap()
            .attr
            .ino
            .0;
        assert_eq!(tree.copy_file_range(src, 3, dst, 0, 100).unwrap(), 0);
        assert_eq!(tree.copy_file_range(src, 99, dst, 0, 100).unwrap(), 0);
    }

    #[test]
    fn copy_file_range_within_same_inode_non_overlapping() {
        let (mut tree, ino) = make_tree_with_file(b"AAAA....BBBB");
        let n = tree.copy_file_range(ino, 0, ino, 4, 4).unwrap();
        assert_eq!(n, 4);
        assert_eq!(read_file_data(&tree, ino), b"AAAAAAAABBBB");
    }

    #[test]
    fn copy_file_range_directory_src_returns_eisdir() {
        let mut tree = make_tree();
        let dst = tree
            .create_file(ROOT_INODE, OsStr::new("dst.txt"), 0o644, Owner::new(0, 0))
            .unwrap()
            .attr
            .ino
            .0;
        assert_errno(
            tree.copy_file_range(ROOT_INODE, 0, dst, 0, 1).unwrap_err(),
            fuser::Errno::EISDIR,
        );
    }

    #[test]
    fn copy_file_range_unknown_inode_returns_enoent() {
        let (mut tree, src) = make_tree_with_file(b"abc");
        assert_errno(
            tree.copy_file_range(src, 0, 9999, 0, 1).unwrap_err(),
            fuser::Errno::ENOENT,
        );
        assert_errno(
            tree.copy_file_range(9999, 0, src, 0, 1).unwrap_err(),
            fuser::Errno::ENOENT,
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
    fn promote_to_dirty_preserves_cipher() {
        let mut fd = FileData::Clean {
            data: vec![1, 2, 3],
            cipher: Some(CipherConfig {
                encryption: pna::Encryption::Aes,
                cipher_mode: pna::CipherMode::CTR,
            }),
        };
        fd.promote_to_dirty();
        if let FileData::Dirty {
            cipher: Some(c), ..
        } = &fd
        {
            assert!(matches!(c.encryption, pna::Encryption::Aes));
        } else {
            panic!("expected Dirty with cipher");
        }
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
        tree.create_file(ROOT_INODE, OsStr::new("a.txt"), 0o644, Owner::new(0, 0))
            .unwrap();
        let result = tree.collect_dfs();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].2, "a.txt");
    }

    #[test]
    fn collect_dfs_nested_dirs() {
        let mut tree = make_tree();
        let dir = tree
            .make_dir(ROOT_INODE, OsStr::new("sub"), 0o755, 0, Owner::new(0, 0))
            .unwrap();
        let dir_ino = dir.attr.ino.0;
        tree.create_file(dir_ino, OsStr::new("file.txt"), 0o644, Owner::new(0, 0))
            .unwrap();
        let result = tree.collect_dfs();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].2, "sub");
        assert_eq!(result[1].2, "sub/file.txt");
    }

    #[test]
    fn collect_dfs_sorted_order() {
        let mut tree = make_tree();
        tree.create_file(ROOT_INODE, OsStr::new("z.txt"), 0o644, Owner::new(0, 0))
            .unwrap();
        tree.create_file(ROOT_INODE, OsStr::new("a.txt"), 0o644, Owner::new(0, 0))
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
        tree.make_dir(
            ROOT_INODE,
            OsStr::new("existing"),
            0o755,
            0,
            Owner::new(0, 0),
        )
        .unwrap();
        tree.make_dir_all(Path::new("existing/new"), ROOT_INODE)
            .unwrap();
        // "existing" was reused (not duplicated), "new" was created
        let children: Vec<_> = tree.children(ROOT_INODE).unwrap().collect();
        assert_eq!(children.len(), 1); // only "existing", not a duplicate
        assert!(
            tree.lookup_child(children[0].1.attr.ino.0, OsStr::new("new"))
                .is_some()
        );
    }

    // ── BTreeMap sorted children order ────────────────────────────

    #[test]
    fn children_returns_sorted_order() {
        let mut tree = make_tree();
        tree.create_file(ROOT_INODE, OsStr::new("z.txt"), 0o644, Owner::new(0, 0))
            .unwrap();
        tree.create_file(ROOT_INODE, OsStr::new("a.txt"), 0o644, Owner::new(0, 0))
            .unwrap();
        tree.create_file(ROOT_INODE, OsStr::new("m.txt"), 0o644, Owner::new(0, 0))
            .unwrap();
        let names: Vec<_> = tree
            .children(ROOT_INODE)
            .unwrap()
            .map(|(name, _)| name.to_string_lossy().into_owned())
            .collect();
        assert_eq!(names, vec!["a.txt", "m.txt", "z.txt"]);
    }

    // ── Parent back-pointer correctness ───────────────────────────

    #[test]
    fn parent_pointer_set_on_create_file() {
        let mut tree = make_tree();
        tree.create_file(ROOT_INODE, OsStr::new("f.txt"), 0o644, Owner::new(0, 0))
            .unwrap();
        let child = tree.lookup_child(ROOT_INODE, OsStr::new("f.txt")).unwrap();
        assert_eq!(child.parent, Some(ROOT_INODE));
    }

    #[test]
    fn parent_pointer_for_nested_file() {
        let mut tree = make_tree();
        let dir = tree
            .make_dir(ROOT_INODE, OsStr::new("sub"), 0o755, 0, Owner::new(0, 0))
            .unwrap();
        let dir_ino = dir.attr.ino.0;
        tree.create_file(dir_ino, OsStr::new("f.txt"), 0o644, Owner::new(0, 0))
            .unwrap();
        let child = tree.lookup_child(dir_ino, OsStr::new("f.txt")).unwrap();
        assert_eq!(child.parent, Some(dir_ino));
    }

    #[test]
    fn parent_pointer_for_make_dir() {
        let mut tree = make_tree();
        let dir = tree
            .make_dir(ROOT_INODE, OsStr::new("sub"), 0o755, 0, Owner::new(0, 0))
            .unwrap();
        assert_eq!(dir.parent, Some(ROOT_INODE));
    }

    // ── is_dirty on non-file mutations ────────────────────────────

    #[test]
    fn is_dirty_after_make_dir() {
        let mut tree = make_tree();
        tree.make_dir(ROOT_INODE, OsStr::new("d"), 0o755, 0, Owner::new(0, 0))
            .unwrap();
        assert!(tree.is_dirty());
    }

    #[test]
    fn is_dirty_after_unlink() {
        let mut tree = make_tree();
        tree.create_file(ROOT_INODE, OsStr::new("f.txt"), 0o644, Owner::new(0, 0))
            .unwrap();
        tree.mark_clean();
        tree.unlink(ROOT_INODE, OsStr::new("f.txt")).unwrap();
        assert!(tree.is_dirty());
    }

    #[test]
    fn is_dirty_after_set_times_with_value() {
        let mut tree = make_tree();
        tree.create_file(ROOT_INODE, OsStr::new("f.txt"), 0o644, Owner::new(0, 0))
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
        tree.create_file(ROOT_INODE, OsStr::new("f.txt"), 0o644, Owner::new(0, 0))
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
