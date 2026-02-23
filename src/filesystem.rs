use crate::file_manager::FileManager;
use fuser::{
    Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyXattr, Request,
};
use libc::ENOENT;
use log::info;
use std::collections::HashMap;
use std::ffi::{CString, OsStr};
use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Default)]
pub(crate) struct PendingChanges {
    pub created: HashMap<u64, crate::file_manager::File>,
    pub written: HashMap<u64, Vec<u8>>, // ino -> 新しいデータ
    pub deleted: Vec<u64>,
}

pub(crate) struct PnaFS {
    manager: FileManager,
    pending: PendingChanges, // 差分管理用フィールドを追加
}

impl PnaFS {
    pub(crate) fn new(archive: PathBuf, password: Option<String>) -> Self {
        Self {
            manager: FileManager::new(archive, password),
            pending: PendingChanges::default(),
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

    // --- ここから書き込み系メソッドのダミー実装 ---
    fn create(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        flags: u32,
        _umask: i32,
        reply: fuser::ReplyCreate,
    ) {
        log::info!(
            "[Experimental] create(parent: {:#x?}, name: {:?}, mode: {:#o}, flags: {:#x?})",
            parent,
            name,
            mode,
            flags
        );
        let ino = self
            .manager
            .get_file(parent)
            .map(|_| self.manager.get_file(parent).unwrap().attr.ino + 10000)
            .unwrap_or(99999);
        let now = std::time::SystemTime::now();
        let file = crate::file_manager::File {
            name: name.to_os_string(),
            data: crate::file_manager::Entry::Loaded(crate::file_manager::LoadedEntry::empty()),
            attr: fuser::FileAttr {
                ino,
                size: 0,
                blocks: 1,
                atime: now,
                mtime: now,
                ctime: now,
                crtime: now,
                kind: fuser::FileType::RegularFile,
                perm: mode as u16,
                nlink: 1,
                uid: 0,
                gid: 0,
                rdev: 0,
                blksize: 512,
                flags: 0,
            },
        };
        self.pending.created.insert(ino, file);
        let ttl = Duration::from_secs(1);
        reply.created(&ttl, &self.pending.created[&ino].attr, 0, 0, 0);
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: fuser::ReplyWrite,
    ) {
        log::info!(
            "[Experimental] write(ino: {:#x?}, offset: {}, size: {})",
            ino,
            offset,
            data.len()
        );
        let entry = self.pending.written.entry(ino).or_default();
        if offset as usize > entry.len() {
            entry.resize(offset as usize, 0);
        }
        if offset as usize + data.len() > entry.len() {
            entry.resize(offset as usize + data.len(), 0);
        }
        entry[offset as usize..offset as usize + data.len()].copy_from_slice(data);
        reply.written(data.len() as u32);
    }

    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        log::info!(
            "[Experimental] unlink(parent: {:#x?}, name: {:?})",
            parent,
            name
        );
        // 仮実装: 親ディレクトリの子から名前一致のinodeを探して削除リストに追加
        if let Some(children) = self.manager.get_children(parent) {
            if let Some(file) = children.iter().find(|f| f.name == name) {
                self.pending.deleted.push(file.attr.ino);
                reply.ok();
                return;
            }
        }
        reply.error(ENOENT);
    }
}
