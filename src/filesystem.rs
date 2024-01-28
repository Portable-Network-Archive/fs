use crate::file_manager::FileManager;
use fuser::{Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, Request};
use libc::ENOENT;
use log::info;
use std::ffi::OsStr;
use std::path::PathBuf;
use std::time::Duration;

const TTL: Duration = Duration::from_secs(1);

pub(crate) struct PnaFS {
    manager: FileManager,
}

impl PnaFS {
    pub(crate) fn new(archive: PathBuf) -> Self {
        Self {
            manager: FileManager::new(archive),
        }
    }
}

impl Filesystem for PnaFS {
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        info!(
            "[Implemented] lookup(parent: {:#x?}, name {:?})",
            parent, name
        );
        let children = self.manager.get_children(parent).unwrap();
        let entry = children.iter().find(|it| it.name == name);
        if let Some(entry) = entry {
            reply.entry(&TTL, &entry.attr, 0);
        } else {
            reply.error(ENOENT);
        }
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyAttr) {
        info!("[Implemented] getattr(ino: {:#x?})", ino);
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
            "[Implemented] read(ino: {:#x?}, fh: {}, offset: {}, size: {}, \
            flags: {:#x?}, lock_owner: {:?})",
            ino, fh, offset, size, flags, lock_owner
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
        info!(
            "[Implemented] flush(ino: {:#x?}, fh: {}, lock_owner: {:?})",
            ino, fh, lock_owner
        );
        if let Some(_) = self.manager.get_file(ino) {
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
        info!(
            "[Implemented] readdir(ino: {:#x?}, fh: {}, offset: {})",
            ino, fh, offset
        );
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
}
