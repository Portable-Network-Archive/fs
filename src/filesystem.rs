use crate::archive_io;
use crate::file_tree::{FileTree, FsContent};
use fuser::{
    BsdFileFlags, Errno, FileHandle, Filesystem, FopenFlags, Generation, INodeNo, LockOwner,
    OpenAccMode, OpenFlags, RenameFlags, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory,
    ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite, ReplyXattr, Request, TimeOrNow, WriteFlags,
};
use log::info;
use std::ffi::{CString, OsStr};
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;
use std::sync::RwLock;
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

    /// Save the archive and mark the tree clean. Returns `Ok(())` even when
    /// there is nothing to save.
    fn save_if_dirty(tree: &mut FileTree) -> io::Result<()> {
        if tree.is_dirty() {
            archive_io::save(tree)?;
            tree.mark_clean();
        }
        Ok(())
    }
}

impl Filesystem for PnaFS {
    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        info!("[Implemented] lookup(parent: {parent:#x?}, name {name:?})");
        let tree = self.tree.read().unwrap();
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
        let tree = self.tree.read().unwrap();
        if let Some(node) = tree.get(ino.0) {
            reply.attr(&ttl, &node.attr);
        } else {
            reply.error(Errno::ENOENT);
        }
    }

    fn readlink(&self, _req: &Request, ino: INodeNo, reply: ReplyData) {
        info!("[Implemented] readlink(ino: {ino:#x?})");
        let tree = self.tree.read().unwrap();
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
        let tree = self.tree.read().unwrap();
        if tree.get(ino.0).is_none() {
            reply.error(Errno::ENOENT);
            return;
        }
        drop(tree);
        if self.write_strategy.is_none() && flags.acc_mode() != OpenAccMode::O_RDONLY {
            reply.error(Errno::EROFS);
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
        let tree = self.tree.read().unwrap();
        let node = match tree.get(ino.0) {
            Some(n) => n,
            None => {
                reply.error(Errno::ENOENT);
                return;
            }
        };
        let data = match &node.content {
            FsContent::File(fd) => fd.data(),
            FsContent::Directory(_) | FsContent::Symlink(_) => {
                reply.error(Errno::EISDIR);
                return;
            }
        };
        let offset = offset as usize;
        let size = size as usize;
        reply.data(&data[data.len().min(offset)..data.len().min(offset + size)]);
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
        if self.write_strategy.is_none() {
            reply.error(Errno::EROFS);
            return;
        }
        let mut tree = self.tree.write().unwrap();
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
        let tree = self.tree.read().unwrap();
        if tree.get(ino.0).is_some() {
            reply.ok();
        } else {
            reply.error(Errno::ENOENT);
        }
    }

    fn release(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        info!("[Implemented] release(ino: {_ino:#x?})");
        if self.write_strategy == Some(WriteStrategy::Immediate) {
            let mut tree = self.tree.write().unwrap();
            if let Err(e) = Self::save_if_dirty(&mut tree) {
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
            let mut tree = self.tree.write().unwrap();
            if let Err(e) = Self::save_if_dirty(&mut tree) {
                log::error!("Failed to save on fsync: {e}");
                reply.error(Errno::EIO);
                return;
            }
        }
        reply.ok();
    }

    fn setattr(
        &self,
        _req: &Request,
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
        if self.write_strategy.is_none() {
            reply.error(Errno::EROFS);
            return;
        }
        let mut tree = self.tree.write().unwrap();
        // Handle truncate (size change).
        if let Some(new_size) = size {
            if let Err(e) = tree.set_size(ino.0, new_size) {
                reply.error(e);
                return;
            }
        }
        // Handle time changes.
        if atime.is_some() || mtime.is_some() {
            if let Err(e) = tree.set_times(ino.0, atime, mtime) {
                reply.error(e);
                return;
            }
        }
        // Handle mode/uid/gid.
        if mode.is_some() || uid.is_some() || gid.is_some() {
            if let Err(e) = tree.set_attr_full(ino.0, mode, uid, gid) {
                reply.error(e);
                return;
            }
        }
        let ttl = Duration::from_secs(1);
        match tree.get(ino.0) {
            Some(node) => reply.attr(&ttl, &node.attr),
            None => reply.error(Errno::ENOENT),
        }
    }

    fn create(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        info!("[Implemented] create(parent: {parent:#x?}, name: {name:?}, mode: {mode})");
        if self.write_strategy.is_none() {
            reply.error(Errno::EROFS);
            return;
        }
        let mut tree = self.tree.write().unwrap();
        // Check parent
        match tree.get(parent.0) {
            None => {
                reply.error(Errno::ENOENT);
                return;
            }
            Some(n) if !matches!(n.content, FsContent::Directory(_)) => {
                reply.error(Errno::ENOTDIR);
                return;
            }
            _ => {}
        }
        let existing = tree.lookup_child(parent.0, name).map(|n| n.attr.ino.0);
        if let Some(ino) = existing {
            if (flags & libc::O_EXCL) != 0 {
                reply.error(Errno::EEXIST);
                return;
            }
            if (flags & libc::O_TRUNC) != 0 {
                if let Err(e) = tree.set_size(ino, 0) {
                    reply.error(e);
                    return;
                }
            }
            let node = tree.get(ino).unwrap();
            reply.created(
                &Duration::from_secs(1),
                &node.attr,
                Generation(0),
                FileHandle(0),
                FopenFlags::empty(),
            );
        } else {
            match tree.create_file(parent.0, name, mode & !umask) {
                Ok(node) => {
                    let attr = node.attr;
                    reply.created(
                        &Duration::from_secs(1),
                        &attr,
                        Generation(0),
                        FileHandle(0),
                        FopenFlags::empty(),
                    );
                }
                Err(e) => reply.error(e),
            }
        }
    }

    fn mkdir(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        info!("[Implemented] mkdir(parent: {parent:#x?}, name: {name:?}, mode: {mode})");
        if self.write_strategy.is_none() {
            reply.error(Errno::EROFS);
            return;
        }
        let mut tree = self.tree.write().unwrap();
        match tree.make_dir(parent.0, name, mode, umask) {
            Ok(node) => {
                let attr = node.attr;
                reply.entry(&Duration::from_secs(1), &attr, Generation(0));
            }
            Err(e) => reply.error(e),
        }
    }

    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        info!("[Implemented] unlink(parent: {parent:#x?}, name: {name:?})");
        if self.write_strategy.is_none() {
            reply.error(Errno::EROFS);
            return;
        }
        let mut tree = self.tree.write().unwrap();
        match tree.unlink(parent.0, name) {
            Ok(()) => reply.ok(),
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
        if self.write_strategy.is_none() {
            reply.error(Errno::EROFS);
            return;
        }
        let mut tree = self.tree.write().unwrap();
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
        let tree = self.tree.read().unwrap();
        let children: Vec<_> = match tree.children(ino.0) {
            Some(iter) => iter.collect(),
            None => Vec::new(),
        };

        let mut current_offset = offset + 1;
        for entry in children.into_iter().skip(offset as usize) {
            let is_full = reply.add(entry.attr.ino, current_offset, entry.attr.kind, &entry.name);
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
        let tree = self.tree.read().unwrap();
        if let Some(node) = tree.get(ino.0) {
            if let Some(value) = node.xattrs.get(name) {
                if size == 0 {
                    reply.size(value.len() as u32);
                } else {
                    reply.data(value);
                }
            } else {
                reply.error(Errno::ENOENT);
            }
        } else {
            reply.error(Errno::ENOENT);
        }
    }

    fn listxattr(&self, _req: &Request, ino: INodeNo, size: u32, reply: ReplyXattr) {
        info!("[Implemented] listxattr(ino: {ino:#x?}, size: {size})");
        let tree = self.tree.read().unwrap();
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

    fn destroy(&mut self) {
        info!("[Implemented] destroy()");
        if self.write_strategy.is_some() {
            let tree = self.tree.get_mut().unwrap();
            if let Err(e) = Self::save_if_dirty(tree) {
                eprintln!("pnafs: CRITICAL: failed to save archive on unmount: {e}");
                log::error!("Failed to save archive on destroy: {e}");
            }
        }
    }
}
