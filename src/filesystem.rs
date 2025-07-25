use crate::file_manager::FileManager;
use fuser::{
    Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyXattr, Request,
};
use libc::ENOENT;
use log::info;
use std::ffi::{CString, OsStr};
use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;
use std::time::Duration;

pub(crate) struct PnaFS {
    manager: FileManager,
}

impl PnaFS {
    pub(crate) fn new(archive: PathBuf, password: Option<String>) -> Self {
        Self {
            manager: FileManager::new(archive, password),
        }
    }
}

impl Filesystem for PnaFS {
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        info!("[Implemented] lookup(parent: {parent:#x?}, name {name:?})");
        let children = self.manager.get_children(parent).unwrap();
        let entry = children.iter().find(|it| it.name == name);
        if let Some(entry) = entry {
            let ttl = Duration::from_secs(1);
            reply.entry(&ttl, &entry.attr, 0);
        } else {
            reply.error(ENOENT);
        }
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, fh: Option<u64>, reply: ReplyAttr) {
        info!("[Implemented] getattr(ino: {ino:#x?}, fh: {fh:#x?})");
        let ttl = Duration::from_secs(1);
        let file = self.manager.get_file(ino).unwrap();
        reply.attr(&ttl, &file.attr);
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        flags: i32,
        lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        info!(
            "[Implemented] read(ino: {ino:#x?}, fh: {fh}, offset: {offset}, size: {size}, \
            flags: {flags:#x?}, lock_owner: {lock_owner:?})"
        );
        if let Some(file) = self.manager.get_file_mut(ino) {
            let offset = offset as usize;
            let size = size as usize;
            let data = file.data.as_slice();
            reply.data(&data[data.len().min(offset)..data.len().min(offset + size)])
        } else {
            reply.error(ENOENT)
        };
    }

    fn flush(&mut self, _req: &Request<'_>, ino: u64, fh: u64, lock_owner: u64, reply: ReplyEmpty) {
        info!("[Implemented] flush(ino: {ino:#x?}, fh: {fh}, lock_owner: {lock_owner:?})");
        if self.manager.get_file(ino).is_some() {
            reply.ok();
        } else {
            reply.error(ENOENT);
        }
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        info!("[Implemented] readdir(ino: {ino:#x?}, fh: {fh}, offset: {offset})");
        let children = self.manager.get_children(ino).unwrap();

        let mut current_offset = offset + 1;
        for entry in children.into_iter().skip(offset as usize) {
            let is_full = reply.add(
                current_offset as u64,
                current_offset,
                entry.attr.kind,
                &entry.name,
            );
            if is_full {
                break;
            } else {
                current_offset += 1;
            }
        }
        reply.ok();
    }

    fn getxattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        name: &OsStr,
        size: u32,
        reply: ReplyXattr,
    ) {
        info!("[Implemented] getxattr(ino: {ino:#x?}, name: {name:?}, size: {size})");
        if let Some(file) = self.manager.get_file_mut(ino) {
            if let Some(value) = file.data.xattrs().get(name) {
                if size == 0 {
                    reply.size(value.len() as u32);
                } else {
                    reply.data(value);
                }
            } else {
                reply.error(ENOENT);
            }
        } else {
            reply.error(ENOENT);
        }
    }

    fn listxattr(&mut self, _req: &Request<'_>, ino: u64, size: u32, reply: ReplyXattr) {
        info!("[Implemented] listxattr(ino: {ino:#x?}, size: {size})");
        if let Some(file) = self.manager.get_file_mut(ino) {
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
            reply.error(ENOENT);
        }
    }
}
