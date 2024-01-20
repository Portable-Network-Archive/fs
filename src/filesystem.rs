use crate::file_manager::FileManager;
use fuser::{Filesystem, ReplyAttr, ReplyDirectory, Request};
use log::info;
use std::path::PathBuf;
use std::time::Duration;

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
    fn getattr(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyAttr) {
        info!("[Implemented] getattr(ino: {:#x?})", ino);
        let ttl = Duration::from_secs(1);
        let file = self.manager.get_file(ino).unwrap();
        reply.attr(&ttl, &file.attr);
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
        let mut children = self.manager.get_children(ino).unwrap();

        let mut current_offset = offset + 1;
        for entry in children.into_iter().skip(offset as usize) {
            let is_full = reply.add(
                current_offset as u64,
                current_offset,
                entry.attr.kind,
                entry.name.as_str(),
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
