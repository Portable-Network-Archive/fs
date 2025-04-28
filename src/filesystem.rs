use crate::file_manager::FileManager;
use fuser::{
    FileType, Filesystem, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyIoctl, ReplyWrite, ReplyXattr, Request, TimeOrNow,
};
use libc::{ENOENT, ENOSYS};
use log::{error, info};
use std::ffi::{CString, OsStr};
use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

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
    fn destroy(&mut self) {
        info!("[Implemented] destroy() called — saving archive.");
        if let Err(e) = self.manager.save_to_archive() {
            error!("Failed to save archive during destroy: {:?}", e);
        } else {
            info!("Archive successfully saved on unmount.");
        }
    }

    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        info!(
            "[Implemented] lookup(parent: {:#x?}, name {:?})",
            parent, name
        );
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
        info!("[Implemented] getattr(ino: {:#x?}, fh: {:#x?})", ino, fh);
        let ttl = Duration::from_secs(1);
        let file = self.manager.get_file(ino).unwrap();
        reply.attr(&ttl, &file.attr);
    }

    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        info!(
            "[Implemented] unlink(parent: {:#x?}, name: {:?})",
            parent, name
        );

        if let Some(children) = self.manager.get_children(parent) {
            if let Some(target) = children.iter().find(|f| f.name == name) {
                let removed = self.manager.remove_file(target.attr.ino);
                if removed {
                    reply.ok();
                } else {
                    reply.error(ENOENT);
                }
            } else {
                reply.error(ENOENT);
            }
        } else {
            reply.error(ENOENT);
        }
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

    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        write_flags: u32,
        flags: i32,
        lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        info!(
            "[Implemented] write(ino: {:#x?}, offset: {}, size: {})",
            ino,
            offset,
            data.len()
        );

        if let Some(file) = self.manager.get_file_mut(ino) {
            let offset = offset as usize;
            let target = file.data.as_mut_slice();
            let required_len = offset + data.len();

            if target.len() < required_len {
                target.resize(required_len, 0);
            }

            target[offset..offset + data.len()].copy_from_slice(data);
            file.attr.size = target.len() as u64;
            reply.written(data.len() as u32);
        } else {
            reply.error(ENOENT);
        }
    }

    fn flush(&mut self, _req: &Request<'_>, ino: u64, fh: u64, lock_owner: u64, reply: ReplyEmpty) {
        info!(
            "[Implemented] flush(ino: {:#x?}, fh: {}, lock_owner: {:?})",
            ino, fh, lock_owner
        );
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

    fn getxattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        name: &OsStr,
        size: u32,
        reply: ReplyXattr,
    ) {
        info!(
            "[Implemented] getxattr(ino: {:#x?}, name: {:?}, size: {})",
            ino, name, size
        );
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
        info!("[Implemented] listxattr(ino: {:#x?}, size: {})", ino, size);
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

    fn create(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        info!(
            "[Implemented] create(parent: {:#x?}, name: {:?})",
            parent, name
        );

        let name = name.to_os_string();

        if let Ok(attr) = self.manager.create_file(parent, name) {
            let ttl = Duration::from_secs(1);
            reply.created(&ttl, &attr, 0, 0, 0);
        } else {
            reply.error(ENOENT);
        }
    }

    fn mkdir(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        info!(
            "[Implementing] mkdir(parent: {:#x?}, name: {:?}, mode: {:#o}, umask: {:#o})",
            parent, name, mode, umask
        );

        let name = name.to_os_string();
        // 権限とumaskを適用
        let perm = mode & !umask;

        // 親ディレクトリを確認
        if let Some(children) = self.manager.get_children(parent) {
            // 既に同名のファイルが存在するかチェック
            if children.iter().any(|f| f.name == name) {
                reply.error(libc::EEXIST);
                return;
            }

            if let Ok(attr) = self.manager.make_dir(parent, name) {
                let ttl = Duration::from_secs(1);
                reply.entry(&ttl, &attr, 0);
            } else {
                reply.error(libc::EIO);
            }
        } else {
            reply.error(ENOENT);
        }
    }

    fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        info!(
            "[Implementing] rmdir(parent: {:#x?}, name: {:?})",
            parent, name
        );

        // 親ディレクトリからファイルを検索
        if let Some(children) = self.manager.get_children(parent) {
            if let Some(target) = children.iter().find(|f| f.name == name) {
                // ディレクトリであることを確認
                if target.attr.kind != FileType::Directory {
                    reply.error(libc::ENOTDIR);
                    return;
                }

                // ディレクトリが空か確認
                if let Some(dir_children) = self.manager.get_children(target.attr.ino) {
                    if !dir_children.is_empty() {
                        reply.error(libc::ENOTEMPTY);
                        return;
                    }
                }

                // ディレクトリを削除
                let removed = self.manager.remove_file(target.attr.ino);
                if removed {
                    reply.ok();
                } else {
                    reply.error(ENOENT);
                }
            } else {
                reply.error(ENOENT);
            }
        } else {
            reply.error(ENOENT);
        }
    }

    fn rename(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        flags: u32,
        reply: ReplyEmpty,
    ) {
        info!(
            "[Implemented] rename(parent: {:#x?}, name: {:?}, newparent: {:#x?}, newname: {:?}, flags: {:#x?})",
            parent, name, newparent, newname, flags
        );

        // 現在のファイルを見つける
        if let Some(children) = self.manager.get_children(parent) {
            if let Some(target) = children.iter().find(|f| f.name == name) {
                let ino = target.attr.ino;
                let new_name = newname.to_os_string();

                // 新しい親ディレクトリの下に同名のファイルがあるか確認
                if let Some(new_children) = self.manager.get_children(newparent) {
                    if let Some(existing) = new_children.iter().find(|f| f.name == newname) {
                        // 同名のファイルが存在する場合、削除
                        self.manager.remove_file(existing.attr.ino);
                    }
                }

                // 親が同じでファイル名だけ変更する場合
                if parent == newparent {
                    // ファイルの名前を変更
                    if let Some(file) = self.manager.get_file_mut(ino) {
                        file.name = new_name;
                        reply.ok();
                    } else {
                        reply.error(ENOENT);
                    }
                } else {
                    // 異なる親ディレクトリに移動する場合
                    match self.manager.move_file(ino, newparent, new_name) {
                        Ok(_) => reply.ok(),
                        Err(e) => reply.error(e.raw_os_error().unwrap_or(libc::EIO)),
                    }
                }
            } else {
                reply.error(ENOENT);
            }
        } else {
            reply.error(ENOENT);
        }
    }

    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        ctime: Option<SystemTime>,
        fh: Option<u64>,
        crtime: Option<SystemTime>,
        chgtime: Option<SystemTime>,
        bkuptime: Option<SystemTime>,
        flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        info!(
            "[Implementing] setattr(ino: {:#x?}, mode: {:?}, uid: {:?}, gid: {:?}, size: {:?}, ...",
            ino, mode, uid, gid, size
        );

        if let Some(file) = self.manager.get_file_mut(ino) {
            // 属性を設定
            if let Some(mode) = mode {
                file.attr.perm = mode as u16;
            }
            if let Some(uid) = uid {
                file.attr.uid = uid;
            }
            if let Some(gid) = gid {
                file.attr.gid = gid;
            }
            if let Some(atime) = atime {
                file.attr.atime = match atime {
                    TimeOrNow::SpecificTime(time) => time,
                    TimeOrNow::Now => SystemTime::now(),
                };
            }
            if let Some(mtime) = mtime {
                file.attr.mtime = match mtime {
                    TimeOrNow::SpecificTime(time) => time,
                    TimeOrNow::Now => SystemTime::now(),
                };
            }
            if let Some(ctime) = ctime {
                file.attr.ctime = ctime;
            }
            if let Some(crtime) = crtime {
                file.attr.crtime = crtime;
            }
            if let Some(flags) = flags {
                file.attr.flags = flags;
            }

            // ファイルサイズの変更（切り詰め操作）
            if let Some(size) = size {
                let data = file.data.as_mut_slice();
                if size < data.len() as u64 {
                    // ファイルを短くする
                    data.truncate(size as usize);
                } else if size > data.len() as u64 {
                    // ファイルを大きくする（ゼロ埋め）
                    data.resize(size as usize, 0);
                }
                file.attr.size = size;
            }

            let ttl = Duration::from_secs(1);
            reply.attr(&ttl, &file.attr);
        } else {
            reply.error(ENOENT);
        }
    }

    fn statfs(&mut self, _req: &Request<'_>, _ino: u64, reply: fuser::ReplyStatfs) {
        info!("[Implementing] statfs(ino: {:#x?})", _ino);

        // 基本的なファイルシステム情報を報告
        // これらの値は単なる例であり、実際のアプリケーションに合わせて調整する必要があります
        reply.statfs(
            1000, // ブロックの総数
            500,  // 空きブロック
            500,  // 利用可能なブロック
            100,  // iノードの総数
            50,   // 空きiノード
            512,  // ブロックサイズ
            255,  // 最大ファイル名長
            0,    // フラグメントサイズ
        );
    }

    fn ioctl(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        flags: u32,
        cmd: u32,
        in_data: &[u8],
        out_size: u32,
        reply: ReplyIoctl,
    ) {
        info!(
            "[Not Implemented] ioctl(ino: {:#x?}, fh: {}, flags: {}, cmd: {}, \
            in_data.len(): {}, out_size: {})",
            ino,
            fh,
            flags,
            cmd,
            in_data.len(),
            out_size,
        );
        // ioctlは特殊なファイルシステム固有の操作のためのインターフェースですが、
        // この実装では対応しないためENOSYS（実装されていない機能）エラーを返します
        reply.error(ENOSYS);
    }
}
