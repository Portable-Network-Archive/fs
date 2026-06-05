use crate::archive_io;
use crate::file_tree::{FileTree, FsContent, NodeKind, Owner, ROOT_INODE};
use fuser::{
    BsdFileFlags, Errno, FileHandle, Filesystem, FopenFlags, Generation, INodeNo, LockOwner,
    OpenAccMode, OpenFlags, RenameFlags, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory,
    ReplyEmpty, ReplyEntry, ReplyOpen, ReplyStatfs, ReplyWrite, ReplyXattr, Request, TimeOrNow,
    WriteFlags,
};
use log::info;
use std::collections::HashSet;
use std::ffi::{CString, OsStr};
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Duration, SystemTime};

/// When to flush dirty data back to the archive.
#[derive(Copy, Clone, PartialEq, clap::ValueEnum)]
pub(crate) enum WriteStrategy {
    /// Flush only on unmount (destroy).
    Lazy,
    /// Flush on every file close (release).
    Immediate,
}

pub(crate) struct PnaFS {
    tree: RwLock<FileTree>,
    write_strategy: Option<WriteStrategy>,
}

impl PnaFS {
    pub(crate) fn new(
        archive: PathBuf,
        password: Option<String>,
        write_strategy: Option<WriteStrategy>,
    ) -> io::Result<Self> {
        let tree = archive_io::load(&archive, password)?;
        Ok(Self {
            tree: RwLock::new(tree),
            write_strategy,
        })
    }

    fn require_writable(&self) -> Result<(), Errno> {
        if self.write_strategy.is_none() {
            Err(Errno::EROFS)
        } else {
            Ok(())
        }
    }

    /// Acquire the tree read lock, fail-stop on poisoning.
    ///
    /// All tree mutations happen under the write lock, so a panic in any
    /// handler can leave the tree half-mutated. Once that happens the
    /// only trustworthy state is the last successfully saved archive:
    /// every subsequent operation fails with `EIO` instead of observing
    /// (or persisting) a possibly-inconsistent tree.
    fn read_tree(&self) -> Result<RwLockReadGuard<'_, FileTree>, Errno> {
        self.tree.read().map_err(|_| {
            log::error!("tree lock poisoned by an earlier panic; failing with EIO");
            Errno::EIO
        })
    }

    /// Acquire the tree write lock, fail-stop on poisoning.
    /// See [`Self::read_tree`] for the rationale.
    fn write_tree(&self) -> Result<RwLockWriteGuard<'_, FileTree>, Errno> {
        self.tree.write().map_err(|_| {
            log::error!("tree lock poisoned by an earlier panic; failing with EIO");
            Errno::EIO
        })
    }

    /// POSIX NAME_MAX on Linux. The kernel does not always enforce this before
    /// passing the request to FUSE, so reject oversized names here to match
    /// the syscall semantics tests expect.
    fn check_name(name: &OsStr) -> Result<(), Errno> {
        const NAME_MAX: usize = 255;
        if name.as_bytes().len() > NAME_MAX {
            Err(Errno::ENAMETOOLONG)
        } else {
            Ok(())
        }
    }

    /// Save the archive and mark the tree clean. Returns `Ok(())` even when
    /// there is nothing to save.
    fn save_if_dirty(tree: &mut FileTree) -> io::Result<()> {
        if tree.is_dirty() {
            archive_io::save(tree)?;
            tree.mark_clean();
        }
        Ok(())
    }

    /// Walk the directory tree from the root and total the byte size and
    /// inode count of every reachable node. A hard-linked inode reachable
    /// through multiple directory entries is counted once (the visited
    /// set), matching how a real filesystem reports usage. Orphans
    /// (`nlink == 0`, only reachable through an open fd) are excluded
    /// because they are not part of the persisted archive.
    fn usage(tree: &FileTree) -> (u64, u64) {
        let mut visited = HashSet::new();
        let mut stack = vec![ROOT_INODE];
        let mut bytes = 0u64;
        while let Some(ino) = stack.pop() {
            if !visited.insert(ino) {
                continue;
            }
            if let Some(node) = tree.get(ino) {
                bytes = bytes.saturating_add(node.attr.size);
            }
            if let Some(children) = tree.children(ino) {
                for (_, child) in children {
                    stack.push(child.attr.ino.0);
                }
            }
        }
        (bytes, visited.len() as u64)
    }
}

/// Block size reported by `statfs`. Matches `FileAttr::blksize` so
/// per-file `st_blocks` and the filesystem-wide block accounting use the
/// same unit.
const STATFS_BSIZE: u32 = 512;

impl Filesystem for PnaFS {
    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        info!("[Implemented] lookup(parent: {parent:#x?}, name {name:?})");
        if let Err(e) = Self::check_name(name) {
            reply.error(e);
            return;
        }
        let tree = match self.read_tree() {
            Ok(tree) => tree,
            Err(e) => return reply.error(e),
        };
        if let Some(node) = tree.lookup_child(parent.0, name) {
            let ttl = Duration::from_secs(1);
            reply.entry(&ttl, &node.attr, Generation(0));
        } else {
            reply.error(Errno::ENOENT);
        }
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, fh: Option<FileHandle>, reply: ReplyAttr) {
        info!("[Implemented] getattr(ino: {ino:#x?}, fh: {fh:#x?})");
        let ttl = Duration::from_secs(1);
        let tree = match self.read_tree() {
            Ok(tree) => tree,
            Err(e) => return reply.error(e),
        };
        if let Some(node) = tree.get(ino.0) {
            reply.attr(&ttl, &node.attr);
        } else {
            reply.error(Errno::ENOENT);
        }
    }

    fn readlink(&self, _req: &Request, ino: INodeNo, reply: ReplyData) {
        info!("[Implemented] readlink(ino: {ino:#x?})");
        let tree = match self.read_tree() {
            Ok(tree) => tree,
            Err(e) => return reply.error(e),
        };
        if let Some(node) = tree.get(ino.0) {
            if let FsContent::Symlink(target) = &node.content {
                reply.data(target.as_bytes());
            } else {
                reply.error(Errno::EINVAL);
            }
        } else {
            reply.error(Errno::ENOENT);
        }
    }

    fn open(&self, _req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        info!("[Implemented] open(ino: {ino:#x?}, flags: {flags:#x?})");
        if self.write_strategy.is_none() && flags.acc_mode() != OpenAccMode::O_RDONLY {
            reply.error(Errno::EROFS);
            return;
        }
        // bump_open is atomic so a read lock suffices on the FUSE hot path.
        let tree = match self.read_tree() {
            Ok(tree) => tree,
            Err(e) => return reply.error(e),
        };
        if let Err(e) = tree.bump_open(ino.0) {
            reply.error(e);
            return;
        }
        reply.opened(FileHandle(0), FopenFlags::empty());
    }

    fn read(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyData,
    ) {
        info!("[Implemented] read(ino: {ino:#x?}, offset: {offset}, size: {size})");
        let tree = match self.read_tree() {
            Ok(tree) => tree,
            Err(e) => return reply.error(e),
        };
        let node = match tree.get(ino.0) {
            Some(n) => n,
            None => {
                reply.error(Errno::ENOENT);
                return;
            }
        };
        let data = match &node.content {
            FsContent::File(fd) => fd.data(),
            FsContent::Directory(_) => {
                reply.error(Errno::EISDIR);
                return;
            }
            FsContent::Symlink(_) => {
                reply.error(Errno::EINVAL);
                return;
            }
            FsContent::Special(_) => {
                // The kernel handles fifo/socket/device read paths itself
                // when getattr reports the right file_type, so we should
                // never get here. Return ENXIO defensively.
                reply.error(Errno::ENXIO);
                return;
            }
        };
        let offset = offset as usize;
        let size = size as usize;
        reply.data(&data[data.len().min(offset)..data.len().min(offset.saturating_add(size))]);
    }

    fn write(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: WriteFlags,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyWrite,
    ) {
        info!(
            "[Implemented] write(ino: {ino:#x?}, offset: {offset}, data.len(): {})",
            data.len()
        );
        if let Err(e) = self.require_writable() {
            reply.error(e);
            return;
        }
        let mut tree = match self.write_tree() {
            Ok(tree) => tree,
            Err(e) => return reply.error(e),
        };
        match tree.write_file(ino.0, offset, data) {
            Ok(written) => reply.written(written as u32),
            Err(e) => reply.error(e),
        }
    }

    fn flush(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        lock_owner: LockOwner,
        reply: ReplyEmpty,
    ) {
        info!("[Implemented] flush(ino: {ino:#x?}, fh: {fh:?}, lock_owner: {lock_owner:?})");
        let tree = match self.read_tree() {
            Ok(tree) => tree,
            Err(e) => return reply.error(e),
        };
        if tree.get(ino.0).is_some() {
            reply.ok();
        } else {
            reply.error(Errno::ENOENT);
        }
    }

    fn release(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        info!("[Implemented] release(ino: {ino:#x?})");
        // Decrement the fd counter under a read lock; only escalate to a
        // write lock when this drop turns an orphan into a candidate for
        // freeing (or when the configured write strategy demands a save).
        let needs_free = {
            let tree = match self.read_tree() {
                Ok(tree) => tree,
                Err(e) => return reply.error(e),
            };
            tree.release_open(ino.0)
        };
        let immediate = self.write_strategy == Some(WriteStrategy::Immediate);
        if needs_free || immediate {
            let mut tree = match self.write_tree() {
                Ok(tree) => tree,
                Err(e) => return reply.error(e),
            };
            if needs_free {
                tree.try_free_orphan(ino.0);
            }
            if immediate && let Err(e) = Self::save_if_dirty(&mut tree) {
                log::error!("Failed to save on release: {e}");
                reply.error(Errno::EIO);
                return;
            }
        }
        reply.ok();
    }

    fn fsync(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        info!("[Implemented] fsync(ino: {_ino:#x?})");
        if self.write_strategy.is_some() {
            let mut tree = match self.write_tree() {
                Ok(tree) => tree,
                Err(e) => return reply.error(e),
            };
            if let Err(e) = Self::save_if_dirty(&mut tree) {
                log::error!("Failed to save on fsync: {e}");
                reply.error(Errno::EIO);
                return;
            }
        }
        reply.ok();
    }

    fn fallocate(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        length: u64,
        mode: i32,
        reply: ReplyEmpty,
    ) {
        info!(
            "[Implemented] fallocate(ino: {ino:#x?}, offset: {offset}, length: {length}, mode: {mode:#x})"
        );
        if let Err(e) = self.require_writable() {
            reply.error(e);
            return;
        }
        let mut tree = match self.write_tree() {
            Ok(tree) => tree,
            Err(e) => return reply.error(e),
        };
        match tree.fallocate(ino.0, offset, length, mode) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    fn copy_file_range(
        &self,
        _req: &Request,
        ino_in: INodeNo,
        _fh_in: FileHandle,
        offset_in: u64,
        ino_out: INodeNo,
        _fh_out: FileHandle,
        offset_out: u64,
        len: u64,
        _flags: fuser::CopyFileRangeFlags,
        reply: ReplyWrite,
    ) {
        info!(
            "[Implemented] copy_file_range(ino_in: {ino_in:#x?}, offset_in: {offset_in}, ino_out: {ino_out:#x?}, offset_out: {offset_out}, len: {len})"
        );
        if let Err(e) = self.require_writable() {
            reply.error(e);
            return;
        }
        let mut tree = match self.write_tree() {
            Ok(tree) => tree,
            Err(e) => return reply.error(e),
        };
        match tree.copy_file_range(ino_in.0, offset_in, ino_out.0, offset_out, len) {
            Ok(written) => reply.written(written as u32),
            Err(e) => reply.error(e),
        }
    }

    fn setattr(
        &self,
        req: &Request,
        ino: INodeNo,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        info!("[Implemented] setattr(ino: {ino:#x?}, mode: {mode:?}, size: {size:?})");
        if let Err(e) = self.require_writable() {
            reply.error(e);
            return;
        }
        // POSIX chown/chmod privilege gates. The kernel's
        // `default_permissions` covers basic rwx checks but not the
        // chown-specific rule that only root may change a file's uid
        // (and only the owner — or root — may change gid). Without these
        // checks an --allow-other mount would let any user re-own files.
        if uid.is_some() || gid.is_some() || mode.is_some() {
            let tree = match self.read_tree() {
                Ok(tree) => tree,
                Err(e) => return reply.error(e),
            };
            let node = match tree.get(ino.0) {
                Some(n) => n,
                None => {
                    reply.error(Errno::ENOENT);
                    return;
                }
            };
            let caller_uid = req.uid();
            let owner_uid = node.attr.uid;
            // chown of uid: caller must be root.
            if let Some(new_uid) = uid
                && new_uid != owner_uid
                && caller_uid != 0
            {
                reply.error(Errno::EPERM);
                return;
            }
            // chgrp / mode: caller must be root, or the file's owner.
            if (gid.is_some() || mode.is_some()) && caller_uid != 0 && caller_uid != owner_uid {
                reply.error(Errno::EPERM);
                return;
            }
        }
        let mut tree = match self.write_tree() {
            Ok(tree) => tree,
            Err(e) => return reply.error(e),
        };
        if let Some(new_size) = size
            && let Err(e) = tree.set_size(ino.0, new_size)
        {
            reply.error(e);
            return;
        }
        if (atime.is_some() || mtime.is_some())
            && let Err(e) = tree.set_times(ino.0, atime, mtime)
        {
            reply.error(e);
            return;
        }
        if (mode.is_some() || uid.is_some() || gid.is_some())
            && let Err(e) = tree.set_attr_full(ino.0, mode, uid, gid)
        {
            reply.error(e);
            return;
        }
        let ttl = Duration::from_secs(1);
        match tree.get(ino.0) {
            Some(node) => reply.attr(&ttl, &node.attr),
            None => reply.error(Errno::ENOENT),
        }
    }

    fn create(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        info!("[Implemented] create(parent: {parent:#x?}, name: {name:?}, mode: {mode})");
        if let Err(e) = self.require_writable() {
            reply.error(e);
            return;
        }
        if let Err(e) = Self::check_name(name) {
            reply.error(e);
            return;
        }
        let mut tree = match self.write_tree() {
            Ok(tree) => tree,
            Err(e) => return reply.error(e),
        };
        let existing = tree.lookup_child(parent.0, name).map(|n| n.attr.ino.0);
        let result_ino = if let Some(ino) = existing {
            if (flags & libc::O_EXCL) != 0 {
                reply.error(Errno::EEXIST);
                return;
            }
            if (flags & libc::O_TRUNC) != 0
                && let Err(e) = tree.set_size(ino, 0)
            {
                reply.error(e);
                return;
            }
            ino
        } else {
            match tree.create_file(
                parent.0,
                name,
                mode & !umask,
                Owner::new(req.uid(), req.gid()),
            ) {
                Ok(node) => node.attr.ino.0,
                Err(e) => {
                    reply.error(e);
                    return;
                }
            }
        };
        // create() is open()-equivalent: track the implicit open so the
        // matching release() can drain it.
        if let Err(e) = tree.bump_open(result_ino) {
            reply.error(e);
            return;
        }
        // The inode existed when we lifted the lock above; another thread
        // can't have freed it because we hold the write lock for this
        // whole handler. Still, prefer EIO over a panic if the invariant
        // is ever broken.
        let attr = match tree.get(result_ino) {
            Some(node) => node.attr,
            None => {
                reply.error(Errno::EIO);
                return;
            }
        };
        reply.created(
            &Duration::from_secs(1),
            &attr,
            Generation(0),
            FileHandle(0),
            FopenFlags::empty(),
        );
    }

    fn mknod(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        rdev: u32,
        reply: ReplyEntry,
    ) {
        info!(
            "[Implemented] mknod(parent: {parent:#x?}, name: {name:?}, mode: {mode:#o}, rdev: {rdev})"
        );
        if let Err(e) = self.require_writable() {
            reply.error(e);
            return;
        }
        if let Err(e) = Self::check_name(name) {
            reply.error(e);
            return;
        }
        let kind = match NodeKind::from_mode(mode) {
            Ok(k) => k,
            Err(e) => {
                reply.error(e);
                return;
            }
        };
        let perm = (mode & !umask) as u16;
        let owner = Owner::new(req.uid(), req.gid());
        let mut tree = match self.write_tree() {
            Ok(tree) => tree,
            Err(e) => return reply.error(e),
        };
        let result = match kind {
            // mknod(S_IFREG, ...) is `create()` without the returned fd.
            NodeKind::Regular => tree
                .create_file(parent.0, name, perm as u32, owner)
                .map(|n| n.attr),
            NodeKind::Special(sk) => tree
                .create_special(parent.0, name, sk, perm, rdev, owner)
                .map(|n| n.attr),
        };
        match result {
            Ok(attr) => reply.entry(&Duration::from_secs(1), &attr, Generation(0)),
            Err(e) => reply.error(e),
        }
    }

    fn mkdir(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        info!("[Implemented] mkdir(parent: {parent:#x?}, name: {name:?}, mode: {mode})");
        if let Err(e) = self.require_writable() {
            reply.error(e);
            return;
        }
        if let Err(e) = Self::check_name(name) {
            reply.error(e);
            return;
        }
        let mut tree = match self.write_tree() {
            Ok(tree) => tree,
            Err(e) => return reply.error(e),
        };
        match tree.make_dir(
            parent.0,
            name,
            mode,
            umask,
            Owner::new(req.uid(), req.gid()),
        ) {
            Ok(node) => {
                let attr = node.attr;
                reply.entry(&Duration::from_secs(1), &attr, Generation(0));
            }
            Err(e) => reply.error(e),
        }
    }

    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        info!("[Implemented] unlink(parent: {parent:#x?}, name: {name:?})");
        if let Err(e) = self.require_writable() {
            reply.error(e);
            return;
        }
        if let Err(e) = Self::check_name(name) {
            reply.error(e);
            return;
        }
        let mut tree = match self.write_tree() {
            Ok(tree) => tree,
            Err(e) => return reply.error(e),
        };
        match tree.unlink(parent.0, name) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        info!("[Implemented] rmdir(parent: {parent:#x?}, name: {name:?})");
        if let Err(e) = self.require_writable() {
            reply.error(e);
            return;
        }
        if let Err(e) = Self::check_name(name) {
            reply.error(e);
            return;
        }
        let mut tree = match self.write_tree() {
            Ok(tree) => tree,
            Err(e) => return reply.error(e),
        };
        match tree.rmdir(parent.0, name) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    fn link(
        &self,
        _req: &Request,
        ino: INodeNo,
        newparent: INodeNo,
        newname: &OsStr,
        reply: ReplyEntry,
    ) {
        info!(
            "[Implemented] link(ino: {ino:#x?}, newparent: {newparent:#x?}, newname: {newname:?})"
        );
        if let Err(e) = self.require_writable() {
            reply.error(e);
            return;
        }
        if let Err(e) = Self::check_name(newname) {
            reply.error(e);
            return;
        }
        let mut tree = match self.write_tree() {
            Ok(tree) => tree,
            Err(e) => return reply.error(e),
        };
        match tree.create_hardlink(newparent.0, newname, ino.0) {
            Ok(node) => {
                let attr = node.attr;
                reply.entry(&Duration::from_secs(1), &attr, Generation(0));
            }
            Err(e) => reply.error(e),
        }
    }

    fn symlink(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        link: &std::path::Path,
        reply: ReplyEntry,
    ) {
        info!("[Implemented] symlink(parent: {parent:#x?}, name: {name:?}, link: {link:?})");
        if let Err(e) = self.require_writable() {
            reply.error(e);
            return;
        }
        if let Err(e) = Self::check_name(name) {
            reply.error(e);
            return;
        }
        let mut tree = match self.write_tree() {
            Ok(tree) => tree,
            Err(e) => return reply.error(e),
        };
        match tree.create_symlink(parent.0, name, link, Owner::new(req.uid(), req.gid())) {
            Ok(node) => {
                let attr = node.attr;
                reply.entry(&Duration::from_secs(1), &attr, Generation(0));
            }
            Err(e) => reply.error(e),
        }
    }

    fn rename(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        flags: RenameFlags,
        reply: ReplyEmpty,
    ) {
        info!(
            "[Implemented] rename(parent: {parent:#x?}, name: {name:?}, newparent: {newparent:#x?}, newname: {newname:?})"
        );
        if let Err(e) = self.require_writable() {
            reply.error(e);
            return;
        }
        if let Err(e) = Self::check_name(name) {
            reply.error(e);
            return;
        }
        if let Err(e) = Self::check_name(newname) {
            reply.error(e);
            return;
        }
        let mut tree = match self.write_tree() {
            Ok(tree) => tree,
            Err(e) => return reply.error(e),
        };
        match tree.rename(parent.0, name, newparent.0, newname, flags) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        info!("[Implemented] readdir(ino: {ino:#x?}, fh: {fh:?}, offset: {offset})");
        let tree = match self.read_tree() {
            Ok(tree) => tree,
            Err(e) => return reply.error(e),
        };
        let node = match tree.get(ino.0) {
            Some(n) => n,
            None => {
                reply.error(Errno::ENOENT);
                return;
            }
        };
        if !matches!(node.content, FsContent::Directory(_)) {
            reply.error(Errno::ENOTDIR);
            return;
        }
        let mut current_offset = offset + 1;
        for (name, node) in tree.children(ino.0).unwrap().skip(offset as usize) {
            let is_full = reply.add(node.attr.ino, current_offset, node.attr.kind, name);
            if is_full {
                break;
            } else {
                current_offset += 1;
            }
        }
        reply.ok();
    }

    fn getxattr(&self, _req: &Request, ino: INodeNo, name: &OsStr, size: u32, reply: ReplyXattr) {
        info!("[Implemented] getxattr(ino: {ino:#x?}, name: {name:?}, size: {size})");
        let tree = match self.read_tree() {
            Ok(tree) => tree,
            Err(e) => return reply.error(e),
        };
        let Some(node) = tree.get(ino.0) else {
            reply.error(Errno::ENOENT);
            return;
        };
        // PNA xattr names are UTF-8 strings, so non-UTF-8 lookups can't
        // resolve to anything we stored.
        let Some(value) = name.to_str().and_then(|n| node.xattrs.get(n)) else {
            reply.error(Errno::ENODATA);
            return;
        };
        if size == 0 {
            reply.size(value.len() as u32);
        } else {
            reply.data(value);
        }
    }

    fn setxattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        name: &OsStr,
        value: &[u8],
        flags: i32,
        _position: u32,
        reply: ReplyEmpty,
    ) {
        info!(
            "[Implemented] setxattr(ino: {ino:#x?}, name: {name:?}, len: {}, flags: {flags:#x})",
            value.len()
        );
        if let Err(e) = self.require_writable() {
            reply.error(e);
            return;
        }
        // PNA's xattr name type is UTF-8; reject non-UTF-8 names with the
        // same errno Linux uses when a name violates fs constraints.
        let Some(name_str) = name.to_str() else {
            reply.error(Errno::EINVAL);
            return;
        };
        let mut tree = match self.write_tree() {
            Ok(tree) => tree,
            Err(e) => return reply.error(e),
        };
        match tree.setxattr(ino.0, name_str, value, flags) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    fn removexattr(&self, _req: &Request, ino: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        info!("[Implemented] removexattr(ino: {ino:#x?}, name: {name:?})");
        if let Err(e) = self.require_writable() {
            reply.error(e);
            return;
        }
        let Some(name_str) = name.to_str() else {
            // A non-UTF-8 name can't possibly be set, so the right answer
            // is "not found" rather than EINVAL — `attr -r` and friends
            // expect ENODATA when the name was never there.
            reply.error(Errno::ENODATA);
            return;
        };
        let mut tree = match self.write_tree() {
            Ok(tree) => tree,
            Err(e) => return reply.error(e),
        };
        match tree.removexattr(ino.0, name_str) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    fn listxattr(&self, _req: &Request, ino: INodeNo, size: u32, reply: ReplyXattr) {
        info!("[Implemented] listxattr(ino: {ino:#x?}, size: {size})");
        let tree = match self.read_tree() {
            Ok(tree) => tree,
            Err(e) => return reply.error(e),
        };
        if let Some(node) = tree.get(ino.0) {
            let keys = node
                .xattrs
                .keys()
                .flat_map(|key| {
                    CString::new(key.as_bytes())
                        .unwrap_or_default()
                        .as_bytes_with_nul()
                        .to_vec()
                })
                .collect::<Vec<_>>();
            if size == 0 {
                reply.size(keys.len() as u32);
            } else {
                reply.data(&keys);
            }
        } else {
            reply.error(Errno::ENOENT);
        }
    }

    fn statfs(&self, _req: &Request, _ino: INodeNo, reply: ReplyStatfs) {
        info!("[Implemented] statfs()");
        let tree = match self.read_tree() {
            Ok(tree) => tree,
            Err(e) => return reply.error(e),
        };
        let (bytes, files) = Self::usage(&tree);
        let bsize = u64::from(STATFS_BSIZE);
        let blocks = bytes.div_ceil(bsize);
        // The archive is a fully-materialised in-memory tree with no notion
        // of free space: every reported block is in use, so bfree/bavail are
        // zero and total blocks == used blocks. ffree is likewise zero. This
        // is identical for read-only and --write mounts (a --write mount has
        // no separate capacity; it just flushes the same tree back).
        reply.statfs(blocks, 0, 0, files, 0, STATFS_BSIZE, 255, STATFS_BSIZE);
    }

    fn destroy(&mut self) {
        info!("[Implemented] destroy()");
        if self.write_strategy.is_some() {
            let tree = match self.tree.get_mut() {
                Ok(tree) => tree,
                Err(_poisoned) => {
                    // A handler panicked while holding the lock; the tree
                    // may be half-mutated. Overwriting the known-good
                    // archive with it would be worse than losing the
                    // unsaved delta, so keep the last consistent state.
                    log::error!(
                        "tree lock poisoned; NOT saving on unmount \
                         (archive keeps its last consistent state)"
                    );
                    eprintln!(
                        "pnafs: ERROR: an internal panic left the in-memory state \
                         possibly inconsistent; any unsaved changes since the last \
                         save were NOT persisted, to protect the on-disk archive"
                    );
                    return;
                }
            };
            if let Err(e) = Self::save_if_dirty(tree) {
                eprintln!("pnafs: CRITICAL: failed to save archive on unmount: {e}");
                log::error!("Failed to save archive on destroy: {e}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pna::{Archive, Metadata, WriteOptions};
    use std::io::Write as IoWrite;
    use std::path::PathBuf;
    use tempfile::TempDir;

    fn create_plain_archive(dir: &TempDir, filename: &str, files: &[(&str, &[u8])]) -> PathBuf {
        let path = dir.path().join(filename);
        let mut archive = Archive::write_header(std::fs::File::create(&path).unwrap()).unwrap();
        for (name, data) in files {
            let entry_name = pna::EntryName::from_lossy(name);
            let data = *data;
            archive
                .write_file(
                    entry_name,
                    Metadata::new(),
                    WriteOptions::builder().build(),
                    |w| {
                        w.write_all(data)?;
                        Ok(())
                    },
                )
                .unwrap();
        }
        archive.finalize().unwrap();
        path
    }

    #[test]
    fn usage_of_empty_archive_is_just_root() {
        let dir = TempDir::new().unwrap();
        let path = create_plain_archive(&dir, "empty.pna", &[]);
        let tree = archive_io::load(&path, None).unwrap();
        let (bytes, files) = PnaFS::usage(&tree);
        // Only the root directory node exists.
        assert_eq!(files, 1);
        assert_eq!(bytes, tree.get(ROOT_INODE).unwrap().attr.size);
    }

    #[test]
    fn usage_tracks_node_count_and_total_bytes() {
        let dir = TempDir::new().unwrap();
        let path = create_plain_archive(
            &dir,
            "data.pna",
            &[("a.txt", b"hello"), ("sub/b.bin", &[0u8; 4096])],
        );
        let tree = archive_io::load(&path, None).unwrap();
        let (bytes, files) = PnaFS::usage(&tree);

        // root + "sub" dir + a.txt + b.bin == 4 reachable inodes.
        assert_eq!(files, 4);

        // Total bytes is the sum of every reachable node's reported size,
        // so it must be at least the two file payloads (5 + 4096).
        let payload = 5 + 4096;
        assert!(
            bytes >= payload,
            "usage bytes {bytes} should cover the {payload}-byte payload"
        );

        // Block count reported by statfs rounds the byte total up to the
        // 512-byte block size; it must be non-zero for a non-empty tree.
        let blocks = bytes.div_ceil(u64::from(STATFS_BSIZE));
        assert!(blocks > 0);
    }

    #[test]
    fn usage_grows_when_more_data_is_present() {
        let dir = TempDir::new().unwrap();
        let small = create_plain_archive(&dir, "small.pna", &[("f", b"x")]);
        let large = create_plain_archive(&dir, "large.pna", &[("f", &[0u8; 65536])]);

        let (small_bytes, small_files) = PnaFS::usage(&archive_io::load(&small, None).unwrap());
        let (large_bytes, large_files) = PnaFS::usage(&archive_io::load(&large, None).unwrap());

        // Same node layout (root + one file) but a much larger payload, so
        // reported usage must grow with the tree contents.
        assert_eq!(small_files, large_files);
        assert!(
            large_bytes > small_bytes,
            "larger payload ({large_bytes}) should report more usage than smaller ({small_bytes})"
        );
    }

    /// Poison the tree lock the same way a real failure would: a thread
    /// panicking while holding the write guard.
    fn poison_tree_lock(fs: &PnaFS) {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = fs.tree.write().unwrap();
            panic!("poison the tree lock");
        }));
        assert!(result.is_err());
        assert!(fs.tree.is_poisoned());
    }

    #[test]
    fn poisoned_lock_fails_with_eio_instead_of_panicking() {
        let dir = TempDir::new().unwrap();
        let path = create_plain_archive(&dir, "a.pna", &[("f", b"x")]);
        let fs = PnaFS::new(path, None, None).unwrap();
        poison_tree_lock(&fs);
        let read_err = fs.read_tree().map(|_| ()).unwrap_err();
        assert_eq!(read_err.code(), Errno::EIO.code());
        let write_err = fs.write_tree().map(|_| ()).unwrap_err();
        assert_eq!(write_err.code(), Errno::EIO.code());
    }

    #[test]
    fn healthy_lock_hands_out_guards() {
        let dir = TempDir::new().unwrap();
        let path = create_plain_archive(&dir, "a.pna", &[("f", b"x")]);
        let fs = PnaFS::new(path, None, None).unwrap();
        assert!(fs.read_tree().is_ok());
        assert!(fs.write_tree().is_ok());
    }

    #[test]
    fn destroy_saves_dirty_tree() {
        let dir = TempDir::new().unwrap();
        let path = create_plain_archive(&dir, "a.pna", &[("f", b"x")]);
        let mut fs = PnaFS::new(path.clone(), None, Some(WriteStrategy::Lazy)).unwrap();
        {
            let mut tree = fs.tree.write().unwrap();
            tree.create_file(
                ROOT_INODE,
                std::ffi::OsStr::new("created"),
                0o644,
                Owner::new(0, 0),
            )
            .unwrap();
        }
        fs.destroy();
        let reloaded = archive_io::load(&path, None).unwrap();
        assert!(
            reloaded
                .lookup_child(ROOT_INODE, std::ffi::OsStr::new("created"))
                .is_some(),
            "a clean destroy must persist dirty data"
        );
    }

    #[test]
    fn destroy_with_poisoned_lock_keeps_archive_bytes_intact() {
        let dir = TempDir::new().unwrap();
        let path = create_plain_archive(&dir, "a.pna", &[("f", b"x")]);
        let before = std::fs::read(&path).unwrap();
        let mut fs = PnaFS::new(path.clone(), None, Some(WriteStrategy::Lazy)).unwrap();
        // Dirty the tree so a save would normally rewrite the archive,
        // then poison the lock: destroy must refuse to persist a
        // possibly half-mutated tree over the known-good archive.
        {
            let mut tree = fs.tree.write().unwrap();
            tree.create_file(
                ROOT_INODE,
                std::ffi::OsStr::new("doomed"),
                0o644,
                Owner::new(0, 0),
            )
            .unwrap();
        }
        poison_tree_lock(&fs);
        fs.destroy();
        let after = std::fs::read(&path).unwrap();
        assert_eq!(
            before, after,
            "poisoned destroy must not rewrite the archive"
        );
    }
}
