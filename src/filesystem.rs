use crate::file_manager::FileManager;
use fuser::{
    Errno, FileHandle, Filesystem, Generation, INodeNo, LockOwner, OpenFlags, ReplyAttr, ReplyData,
    ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyXattr, Request,
};
use log::info;
use std::ffi::{CString, OsStr};
use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;

pub(crate) struct PnaFS {
    manager: Mutex<FileManager>,
}

impl PnaFS {
    pub(crate) fn new(archive: PathBuf, password: Option<String>) -> Self {
        Self {
            manager: Mutex::new(FileManager::new(archive, password)),
        }
    }
}

impl Filesystem for PnaFS {
    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        info!("[Implemented] lookup(parent: {parent:#x?}, name {name:?})");
        let manager = self.manager.lock().unwrap();
        let children = manager.get_children(parent.0).unwrap();
        let entry = children.iter().find(|it| it.name == name);
        if let Some(entry) = entry {
            let ttl = Duration::from_secs(1);
            reply.entry(&ttl, &entry.attr, Generation(0));
        } else {
            reply.error(Errno::ENOENT);
        }
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, fh: Option<FileHandle>, reply: ReplyAttr) {
        info!("[Implemented] getattr(ino: {ino:#x?}, fh: {fh:#x?})");
        let ttl = Duration::from_secs(1);
        let manager = self.manager.lock().unwrap();
        let file = manager.get_file(ino.0).unwrap();
        reply.attr(&ttl, &file.attr);
    }

    fn read(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        size: u32,
        flags: OpenFlags,
        lock_owner: Option<LockOwner>,
        reply: ReplyData,
    ) {
        info!(
            "[Implemented] read(ino: {ino:#x?}, fh: {fh:?}, offset: {offset}, size: {size}, \
            flags: {flags:#x?}, lock_owner: {lock_owner:?})"
        );
        let mut manager = self.manager.lock().unwrap();
        if let Some(file) = manager.get_file_mut(ino.0) {
            let offset = offset as usize;
            let size = size as usize;
            let data = file.data.as_slice();
            reply.data(&data[data.len().min(offset)..data.len().min(offset + size)])
        } else {
            reply.error(Errno::ENOENT)
        };
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
        let manager = self.manager.lock().unwrap();
        if manager.get_file(ino.0).is_some() {
            reply.ok();
        } else {
            reply.error(Errno::ENOENT);
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
        let manager = self.manager.lock().unwrap();
        let children = manager.get_children(ino.0).unwrap();

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
        let mut manager = self.manager.lock().unwrap();
        if let Some(file) = manager.get_file_mut(ino.0) {
            if let Some(value) = file.data.xattrs().get(name) {
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
        let mut manager = self.manager.lock().unwrap();
        if let Some(file) = manager.get_file_mut(ino.0) {
            let keys = file
                .data
                .xattrs()
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
}
