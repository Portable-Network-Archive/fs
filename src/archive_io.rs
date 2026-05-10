use crate::file_tree::{
    CipherConfig, DirContent, FileData, FileTree, FsContent, FsNode, ROOT_INODE, get_gid, get_uid,
    make_dir_node,
};
use fuser::{FileAttr, FileType, INodeNo};
use pna::{
    Archive, DataKind, EntryBuilder, EntryName, EntryReference, ExtendedAttribute, HashAlgorithm,
    NormalEntry, Permission, ReadEntry, ReadOptions, WriteOptions,
};
use std::collections::HashMap;
use std::io::{Read, Write as IoWrite};
use std::path::Path;
use std::process;
use std::sync::atomic::AtomicU32;
use std::time::{Duration, SystemTime};
use std::{fs, io};

/// Remove any leftover `.{stem}.tmp.{pid}` files next to `archive_path`.
/// Threshold beyond which a `.archive.tmp.<pid>` file is considered
/// abandoned regardless of whether some process still holds that PID.
/// Linux recycles PIDs, so the original kill-zero check has a long-tail
/// false-positive that would keep stale tmp files indefinitely. Anything
/// older than this almost certainly belongs to a previous mount that's
/// since died.
const STALE_TMP_AGE: Duration = Duration::from_secs(60 * 60); // 1h

pub(crate) fn cleanup_stale_tmp(archive_path: &Path) {
    let dir = archive_path.parent().unwrap_or(Path::new("."));
    let stem = match archive_path.file_name() {
        Some(s) => s.to_string_lossy().into_owned(),
        None => return,
    };
    let prefix = format!(".{stem}.tmp.");
    let rd = match dir.read_dir() {
        Ok(rd) => rd,
        Err(e) => {
            log::warn!("cleanup_stale_tmp: read_dir({:?}) failed: {e}", dir);
            return;
        }
    };
    for entry in rd.flatten() {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        let Some(pid_str) = name_str.strip_prefix(&prefix) else {
            continue;
        };
        let Ok(pid) = pid_str.parse::<u32>() else {
            continue;
        };

        let mut alive = false;
        #[cfg(unix)]
        {
            let ret = unsafe { libc::kill(pid as i32, 0) };
            if ret == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM) {
                alive = true;
            }
        }
        if alive {
            // PID could be recycled — fall through to age-based eviction
            // before declaring the file safe to keep.
            let too_old = entry
                .metadata()
                .and_then(|m| m.modified())
                .map(|m| m.elapsed().unwrap_or_default() > STALE_TMP_AGE)
                .unwrap_or(false);
            if !too_old {
                continue;
            }
            log::warn!(
                "cleanup_stale_tmp: removing {:?} (PID {pid} alive but file > {:?})",
                entry.path(),
                STALE_TMP_AGE
            );
        }
        if let Err(e) = fs::remove_file(entry.path()) {
            log::warn!("cleanup_stale_tmp: remove {:?} failed: {e}", entry.path());
        }
    }
}

/// Load a PNA archive from `archive_path`, optionally decrypting with `password`.
pub(crate) fn load(archive_path: &Path, password: Option<String>) -> io::Result<FileTree> {
    cleanup_stale_tmp(archive_path);

    let data = fs::read(archive_path)?;

    // Derive password bytes before moving `password` into the tree.
    let password_bytes: Option<Vec<u8>> = password.as_deref().map(|s| s.as_bytes().to_vec());

    let mut archive = Archive::read_header_from_slice(&data)?;

    let mut tree = FileTree::new(archive_path.to_path_buf(), password);

    let root = make_dir_node(ROOT_INODE, ".".into());
    tree.insert_node(root, None)?;

    let pw = password_bytes.as_deref();

    // Pass 1 stores File / Dir / Symlink directly; HardLink entries are
    // deferred because their source path may appear later in the archive
    // (or itself be another hardlink).
    let mut pending_hardlinks: Vec<PendingHardlink> = Vec::new();

    for entry in archive.entries_slice() {
        let entry = entry?;
        match entry {
            ReadEntry::Normal(e) => {
                let owned: NormalEntry<Vec<u8>> = e.into();
                if let Some(p) = add_normal_entry(&mut tree, owned, pw)? {
                    pending_hardlinks.push(p);
                }
            }
            ReadEntry::Solid(solid) => {
                for e in solid.entries(pw)? {
                    if let Some(p) = add_normal_entry(&mut tree, e?, pw)? {
                        pending_hardlinks.push(p);
                    }
                }
            }
        }
    }

    // Pass 2 retries until no further progress, so chains
    // hardlink → hardlink → file resolve regardless of archive order.
    while !pending_hardlinks.is_empty() {
        let before = pending_hardlinks.len();
        let mut still_pending = Vec::with_capacity(pending_hardlinks.len());
        for pending in pending_hardlinks.drain(..) {
            if !resolve_hardlink(&mut tree, &pending)? {
                still_pending.push(pending);
            }
        }
        pending_hardlinks = still_pending;
        if pending_hardlinks.len() == before {
            for p in &pending_hardlinks {
                log::warn!(
                    "load: dropping hardlink '{}' -> '{}': source not found in archive",
                    p.link_path.display(),
                    p.source_path.display()
                );
            }
            break;
        }
    }

    tree.recompute_directory_nlinks();
    Ok(tree)
}

/// A `DataKind::HardLink` entry deferred for resolution after pass 1.
struct PendingHardlink {
    link_path: std::path::PathBuf,
    source_path: std::path::PathBuf,
    /// Used to bump the source inode's `ctime` (POSIX: link updates it).
    mtime: SystemTime,
}

fn resolve_hardlink(tree: &mut FileTree, p: &PendingHardlink) -> io::Result<bool> {
    let source_ino = match tree.resolve_path(&p.source_path) {
        Some(ino) => ino,
        None => return Ok(false),
    };
    // POSIX forbids hardlinks to directories. A malformed or hostile
    // archive whose hardlink entry points at a dir path would otherwise
    // create a directory cycle on load. Drop and warn — caller treats
    // this as a non-fatal corruption signal.
    if matches!(
        tree.get(source_ino).map(|n| &n.content),
        Some(FsContent::Directory(_))
    ) {
        log::warn!(
            "load: refusing hardlink '{}' -> '{}': source is a directory",
            p.link_path.display(),
            p.source_path.display()
        );
        eprintln!(
            "pnafs: WARNING: dropping hardlink '{}' -> '{}' (source is a directory)",
            p.link_path.display(),
            p.source_path.display()
        );
        return Ok(true);
    }
    let (parent_ino, name) = match split_parent(tree, &p.link_path)? {
        Some(v) => v,
        None => return Ok(true),
    };
    if tree.lookup_child(parent_ino, &name).is_some() {
        return Ok(true);
    }
    if let Some(parent) = tree.get_mut(parent_ino)
        && let FsContent::Directory(dir) = &mut parent.content
    {
        dir.insert(name.clone(), source_ino);
    }
    if let Some(src) = tree.get_mut(source_ino) {
        src.attr.nlink += 1;
        src.attr.ctime = p.mtime.max(src.attr.ctime);
    }
    Ok(true)
}

fn split_parent(tree: &mut FileTree, path: &Path) -> io::Result<Option<(u64, std::ffi::OsString)>> {
    let parent_ino = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => tree.make_dir_all(p, ROOT_INODE)?,
        _ => ROOT_INODE,
    };
    let name = match path.components().next_back() {
        Some(c) => c.as_os_str().to_owned(),
        None => return Ok(None),
    };
    Ok(Some((parent_ino, name)))
}

fn add_normal_entry(
    tree: &mut FileTree,
    entry: NormalEntry<Vec<u8>>,
    password: Option<&[u8]>,
) -> io::Result<Option<PendingHardlink>> {
    let now = SystemTime::now();
    let header = entry.header();
    let metadata = entry.metadata();
    let entry_path = header.path().as_path().to_path_buf();

    if header.data_kind() == DataKind::HardLink {
        let opts = ReadOptions::with_password(password);
        let mut buf = Vec::new();
        entry.reader(&opts)?.read_to_end(&mut buf)?;
        let source_str = String::from_utf8_lossy(&buf).into_owned();
        let mtime = metadata
            .modified()
            .map_or(now, |d| SystemTime::UNIX_EPOCH + d);
        return Ok(Some(PendingHardlink {
            link_path: entry_path,
            source_path: std::path::PathBuf::from(source_str),
            mtime,
        }));
    }

    let parent_ino = match entry_path.parent() {
        Some(p) if !p.as_os_str().is_empty() => tree.make_dir_all(p, ROOT_INODE)?,
        _ => ROOT_INODE,
    };
    let name = match entry_path.components().next_back() {
        Some(c) => c.as_os_str().to_owned(),
        None => return Ok(None),
    };

    let ino = tree.next_inode();

    let mut attr = FileAttr {
        ino: INodeNo(ino),
        size: 0, // Corrected below after decoding entry data
        blocks: 1,
        atime: metadata
            .modified()
            .map_or(now, |d| SystemTime::UNIX_EPOCH + d),
        mtime: metadata
            .modified()
            .map_or(now, |d| SystemTime::UNIX_EPOCH + d),
        ctime: metadata
            .modified()
            .map_or(now, |d| SystemTime::UNIX_EPOCH + d),
        crtime: metadata
            .created()
            .map_or(now, |d| SystemTime::UNIX_EPOCH + d),
        kind: match header.data_kind() {
            DataKind::File => FileType::RegularFile,
            DataKind::Directory => FileType::Directory,
            DataKind::SymbolicLink => FileType::Symlink,
            // HardLink is handled above and never reaches this match.
            DataKind::HardLink => unreachable!("hardlinks are deferred to pass 2"),
        },
        perm: metadata.permission().map_or(0o775, |p| p.permissions()),
        nlink: 1,
        uid: get_uid(metadata.permission()),
        gid: get_gid(metadata.permission()),
        rdev: 0,
        blksize: 512,
        flags: 0,
    };

    let cipher = CipherConfig::from_entry_header(header);

    let opts = ReadOptions::with_password(password);

    let content = match header.data_kind() {
        DataKind::Directory => FsContent::Directory(crate::file_tree::DirContent::new()),
        DataKind::SymbolicLink => {
            let mut buf = Vec::new();
            entry.reader(&opts)?.read_to_end(&mut buf)?;
            FsContent::Symlink(std::ffi::OsString::from(
                String::from_utf8_lossy(&buf).into_owned(),
            ))
        }
        DataKind::File => {
            // Always decode: fSIZ is only a hint and must not be trusted for
            // attr.size or any load-strategy decisions.  The only reliable
            // source of the true file size is the decoded data itself.
            let mut buf = Vec::new();
            entry.reader(&opts)?.read_to_end(&mut buf)?;
            attr.size = buf.len() as u64;
            FsContent::File(FileData::Clean { data: buf, cipher })
        }
        DataKind::HardLink => unreachable!("hardlinks are deferred to pass 2"),
    };

    let xattrs = entry
        .xattrs()
        .iter()
        .map(|x| (x.name().to_owned(), x.value().to_vec()))
        .collect();

    let node = FsNode {
        name,
        parent: None,
        attr,
        content,
        xattrs,
        open_count: AtomicU32::new(0),
    };

    // If a node with this name already exists under parent (e.g. incremental
    // archives), update it in-place reusing the existing inode.
    let existing_ino = tree
        .lookup_child(parent_ino, &node.name)
        .map(|n| n.attr.ino.0);

    if let Some(existing) = existing_ino {
        if let Some(existing_node) = tree.get_mut(existing) {
            // When replacing an existing directory with another directory entry,
            // preserve the children map — it may already contain nodes inserted
            // by earlier file entries (e.g., "dir/file.txt" before "dir").
            let preserve_children = matches!(existing_node.content, FsContent::Directory(_))
                && matches!(node.content, FsContent::Directory(_));
            let preserved = if preserve_children {
                match std::mem::replace(
                    &mut existing_node.content,
                    FsContent::Directory(DirContent::new()),
                ) {
                    old @ FsContent::Directory(_) => Some(old),
                    _ => None,
                }
            } else {
                None
            };
            let parent = existing_node.parent;
            let preserved_open = existing_node
                .open_count
                .load(std::sync::atomic::Ordering::Relaxed);
            *existing_node = FsNode {
                name: node.name,
                parent,
                attr: FileAttr {
                    ino: INodeNo(existing),
                    ..node.attr
                },
                content: preserved.unwrap_or(node.content),
                xattrs: node.xattrs,
                open_count: AtomicU32::new(preserved_open),
            };
        }
    } else {
        tree.insert_node(node, Some(parent_ino))?;
    }

    Ok(None)
}

/// Save the in-memory `FileTree` back to disk atomically.
///
/// Writes to a temporary file `.{stem}.tmp.{pid}`, finalizes, calls `sync_all()`,
/// then renames the temporary file over the original archive path.
pub(crate) fn save(tree: &FileTree) -> io::Result<()> {
    let archive_path = tree.archive_path();

    // Password guard via collect_dfs
    let nodes = tree.collect_dfs();
    for (_, node, _) in &nodes {
        if let FsContent::File(
            FileData::Clean {
                cipher: Some(_), ..
            }
            | FileData::Dirty {
                cipher: Some(_), ..
            },
        ) = &node.content
            && tree.password().is_none()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot re-encrypt: archive requires password but none was provided",
            ));
        }
    }

    cleanup_stale_tmp(archive_path);

    let dir = archive_path.parent().unwrap_or(Path::new("."));
    let stem = archive_path
        .file_name()
        .unwrap_or(archive_path.as_os_str())
        .to_string_lossy();
    let tmp_path = dir.join(format!(".{}.tmp.{}", stem, process::id()));

    let tmp_file = fs::File::create(&tmp_path)?;

    let result = (|| -> io::Result<()> {
        let mut archive = Archive::write_header(tmp_file)?;

        // Track which inodes have already been written as their primary
        // (File / Symlink) entry. Subsequent occurrences of the same inode
        // are written as HardLink entries that reference the primary path.
        let mut primary_path: HashMap<crate::file_tree::Inode, String> = HashMap::new();

        for (ino, node, archive_path_str) in &nodes {
            let entry_name = EntryName::from_lossy(archive_path_str);

            match &node.content {
                FsContent::Directory(_) => {
                    finalize_primary_entry(&mut archive, EntryBuilder::new_dir(entry_name), node)?;
                }
                FsContent::Symlink(target) => {
                    if let Some(original) = primary_path.get(ino) {
                        // A second directory entry for an inode we already
                        // wrote — write a hardlink pointing at the primary.
                        write_hardlink_entry(
                            &mut archive,
                            entry_name,
                            original,
                            node.attr.mtime,
                            node.attr.crtime,
                        )?;
                    } else {
                        primary_path.insert(*ino, archive_path_str.clone());
                        let target_path = std::path::PathBuf::from(target);
                        let reference = EntryReference::from_lossy(target_path);
                        let builder = EntryBuilder::new_symlink(entry_name, reference)?;
                        finalize_primary_entry(&mut archive, builder, node)?;
                    }
                }
                FsContent::File(fc) => {
                    if let Some(original) = primary_path.get(ino) {
                        write_hardlink_entry(
                            &mut archive,
                            entry_name,
                            original,
                            node.attr.mtime,
                            node.attr.crtime,
                        )?;
                    } else {
                        primary_path.insert(*ino, archive_path_str.clone());
                        let write_opts = build_write_options(fc, tree.password())?;
                        let mut builder = EntryBuilder::new_file(entry_name, write_opts)?;
                        builder.write_all(fc.data())?;
                        finalize_primary_entry(&mut archive, builder, node)?;
                    }
                }
                FsContent::Special(sf) => {
                    // PNA's on-disk format does not yet have a DataKind for
                    // block / char / fifo / socket, so the node only lives
                    // for the lifetime of the mount. Surface the drop on
                    // both `log::warn!` (for any installed logger) and
                    // stderr (so users without a logger configured still
                    // see the data-loss warning).
                    //
                    // Forward-compatibility: when PNA gains a special-file
                    // datakind, replace this branch with a builder dispatch
                    // mirroring the Symlink arm above; the matching
                    // FsContent::Special arms in file_tree.rs / filesystem.rs
                    // (set_size, set_times, write_file, read) should be
                    // revisited at the same time.
                    log::warn!(
                        "save: dropping {:?} entry '{}' (rdev: {}); the PNA \
                         format does not yet represent special-file nodes",
                        sf.kind,
                        archive_path_str,
                        sf.rdev,
                    );
                    eprintln!(
                        "pnafs: WARNING: dropping {:?} entry '{}' (rdev: {}); \
                         PNA cannot represent special-file nodes",
                        sf.kind, archive_path_str, sf.rdev
                    );
                }
            }
        }

        // Finalize returns the inner writer so we can sync before rename.
        let inner = archive.finalize()?;
        inner.sync_all()?;
        drop(inner);

        fs::rename(&tmp_path, archive_path)?;
        // The parent-dir fsync is what makes the rename durable across a
        // crash. Failing to open or sync it leaves the rename only in
        // page-cache, which can disappear on power loss — log loudly but
        // don't abort the save (the bytes are already in the kernel's
        // queue and the mount may be on a filesystem that doesn't expose
        // a directory fd, e.g. some FUSE backends).
        if let Some(parent_dir) = archive_path.parent() {
            match fs::File::open(parent_dir) {
                Ok(dir_file) => {
                    if let Err(e) = dir_file.sync_all() {
                        log::error!("save: parent-dir sync_all failed for {:?}: {e}", parent_dir);
                    }
                }
                Err(e) => log::error!("save: cannot open parent dir {:?}: {e}", parent_dir),
            }
        }
        Ok(())
    })();

    if result.is_err() {
        let _ = fs::remove_file(&tmp_path);
    }

    result
}

/// Write a `DataKind::HardLink` entry referencing an existing primary path.
fn write_hardlink_entry<W: IoWrite>(
    archive: &mut Archive<W>,
    entry_name: EntryName,
    primary_path: &str,
    mtime: SystemTime,
    crtime: SystemTime,
) -> io::Result<()> {
    let reference = EntryReference::from_lossy(std::path::PathBuf::from(primary_path));
    let mut builder = EntryBuilder::new_hard_link(entry_name, reference)?;
    builder.modified(system_time_to_pna(mtime));
    builder.created(system_time_to_pna(crtime));
    let entry = builder.build()?;
    archive.add_entry(entry)?;
    Ok(())
}

/// Build `WriteOptions` for `fc`, given the mount-level password.
///
/// The mount-password default (`CipherConfig::default_for_password`) only
/// applies to `FileData::New` — files that were created during this
/// mount and have no on-disk cipher state of their own. For pre-existing
/// entries (`Clean` / `Dirty`) the `cipher` field is authoritative: a
/// plaintext file stays plaintext even if the user passed `--password`,
/// otherwise mounting an unencrypted archive with a password and saving
/// would silently re-encrypt every entry.
fn build_write_options(fc: &FileData, password: Option<&str>) -> io::Result<WriteOptions> {
    let effective = fc.cipher().copied().or_else(|| match fc {
        FileData::New(_) => password.map(|_| CipherConfig::default_for_password()),
        FileData::Clean { .. } | FileData::Dirty { .. } => None,
    });
    match effective {
        Some(cfg) => {
            let pwd = password.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "cipher config requires a password",
                )
            })?;
            Ok(WriteOptions::builder()
                .encryption(cfg.encryption)
                .cipher_mode(cfg.cipher_mode)
                .hash_algorithm(HashAlgorithm::argon2id())
                .password(Some(pwd.as_bytes()))
                .build())
        }
        None => Ok(WriteOptions::builder().build()),
    }
}

fn system_time_to_pna(t: SystemTime) -> Option<pna::Duration> {
    t.duration_since(std::time::UNIX_EPOCH)
        .ok()
        .map(|d| pna::Duration::seconds(d.as_secs() as i64))
}

/// Stamp `node`'s mtime / crtime / permission / xattrs onto `builder`,
/// build the entry, and append it to `archive`. Centralised so that all
/// primary-entry paths (file, dir, symlink) round-trip the same metadata.
fn finalize_primary_entry<W: IoWrite>(
    archive: &mut Archive<W>,
    mut builder: EntryBuilder,
    node: &FsNode,
) -> io::Result<()> {
    builder.modified(system_time_to_pna(node.attr.mtime));
    builder.created(system_time_to_pna(node.attr.crtime));
    builder.permission(Some(build_permission(node)));
    for (name, value) in &node.xattrs {
        builder.add_xattr(ExtendedAttribute::new(name.clone(), value.clone()));
    }
    archive.add_entry(builder.build()?)?;
    Ok(())
}

#[cfg(unix)]
fn build_permission(node: &FsNode) -> Permission {
    let uname = nix::unistd::User::from_uid(nix::unistd::Uid::from_raw(node.attr.uid))
        .ok()
        .flatten()
        .map(|u| u.name)
        .unwrap_or_default();
    let gname = nix::unistd::Group::from_gid(nix::unistd::Gid::from_raw(node.attr.gid))
        .ok()
        .flatten()
        .map(|g| g.name)
        .unwrap_or_default();
    Permission::new(
        node.attr.uid as u64,
        uname,
        node.attr.gid as u64,
        gname,
        node.attr.perm,
    )
}

#[cfg(not(unix))]
fn build_permission(node: &FsNode) -> Permission {
    Permission::new(
        node.attr.uid as u64,
        String::new(),
        node.attr.gid as u64,
        String::new(),
        node.attr.perm,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_tree::{Owner, ROOT_INODE};
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
    fn load_empty_archive() {
        let dir = TempDir::new().unwrap();
        let path = create_plain_archive(&dir, "empty.pna", &[]);
        let tree = load(&path, None).unwrap();
        assert!(tree.get(ROOT_INODE).is_some());
        assert_eq!(tree.children(ROOT_INODE).unwrap().count(), 0);
        assert!(!tree.is_dirty());
    }

    #[test]
    fn load_single_file() {
        let dir = TempDir::new().unwrap();
        let path = create_plain_archive(&dir, "single.pna", &[("hello.txt", b"world")]);
        let tree = load(&path, None).unwrap();
        let children: Vec<_> = tree.children(ROOT_INODE).unwrap().collect();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].0, std::ffi::OsStr::new("hello.txt"));
        assert!(matches!(
            children[0].1.content,
            FsContent::File(FileData::Clean { .. })
        ));
        assert_eq!(children[0].1.attr.size, 5); // b"world"
    }

    #[test]
    fn load_cleans_stale_tmp() {
        let dir = TempDir::new().unwrap();
        let path = create_plain_archive(&dir, "test.pna", &[]);
        // create a stale tmp file
        let stale = dir.path().join(".test.pna.tmp.99999");
        std::fs::write(&stale, b"stale").unwrap();
        let _tree = load(&path, None).unwrap();
        assert!(!stale.exists(), "stale tmp should be deleted");
    }

    #[test]
    fn load_nested_dirs() {
        let dir = TempDir::new().unwrap();
        let path = create_plain_archive(&dir, "nested.pna", &[("a/b/c.txt", b"x")]);
        let tree = load(&path, None).unwrap();

        // Root should have exactly one child: "a" (a directory).
        let root_children: Vec<_> = tree.children(ROOT_INODE).unwrap().collect();
        assert_eq!(root_children.len(), 1);
        assert_eq!(root_children[0].0.to_str().unwrap(), "a");

        // "a" should have exactly one child: "b" (a directory).
        let a_ino = root_children[0].1.attr.ino.0;
        let a_children: Vec<_> = tree.children(a_ino).unwrap().collect();
        assert_eq!(a_children.len(), 1);
        assert_eq!(a_children[0].0.to_str().unwrap(), "b");

        // "b" should have exactly one child: "c.txt".
        let b_ino = a_children[0].1.attr.ino.0;
        let b_children: Vec<_> = tree.children(b_ino).unwrap().collect();
        assert_eq!(b_children.len(), 1);
        assert_eq!(b_children[0].0.to_str().unwrap(), "c.txt");
    }

    #[test]
    fn load_solid_entry_force_loaded() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("solid.pna");
        let writer = std::fs::File::create(&path).unwrap();
        let mut solid =
            Archive::write_solid_header(writer, WriteOptions::builder().build()).unwrap();
        solid
            .write_file(
                pna::EntryName::from_lossy("solid_file.txt"),
                Metadata::new(),
                |w| {
                    w.write_all(b"hello")?;
                    Ok(())
                },
            )
            .unwrap();
        solid.finalize().unwrap();

        let tree = load(&path, None).unwrap();
        let children: Vec<_> = tree.children(ROOT_INODE).unwrap().collect();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].0, std::ffi::OsStr::new("solid_file.txt"));
        assert!(matches!(
            children[0].1.content,
            FsContent::File(FileData::Clean { .. })
        ));
    }

    #[test]
    fn load_encrypted_normal_entry_loaded() {
        use pna::{CipherMode, Encryption};

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("encrypted.pna");
        let mut archive = Archive::write_header(std::fs::File::create(&path).unwrap()).unwrap();
        // write_file() doesn't write fSIZ, so the entry will be force-loaded.
        archive
            .write_file(
                pna::EntryName::from_lossy("secret.txt"),
                Metadata::new(),
                WriteOptions::builder()
                    .encryption(Encryption::Aes)
                    .cipher_mode(CipherMode::CTR)
                    .password(Some(b"testpass"))
                    .build(),
                |w| {
                    w.write_all(b"secret data")?;
                    Ok(())
                },
            )
            .unwrap();
        archive.finalize().unwrap();

        let tree = load(&path, Some("testpass".to_string())).unwrap();
        let children: Vec<_> = tree.children(ROOT_INODE).unwrap().collect();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].0, std::ffi::OsStr::new("secret.txt"));
        assert!(matches!(
            children[0].1.content,
            FsContent::File(FileData::Clean { .. })
        ));
    }

    /// POSIX: a directory's `nlink` is `2 + #subdirectories` (`.` plus one
    /// `..` from each child directory). A flat `nlink: 1` for every loaded
    /// directory would make `stat()` lie to userspace.
    #[test]
    fn load_directories_have_posix_nlink() {
        let dir = TempDir::new().unwrap();
        let path = create_plain_archive(
            &dir,
            "nlink.pna",
            &[
                ("a/file.txt", b"x"),
                ("a/b/c/leaf.txt", b"y"),
                ("a/b/d/leaf.txt", b"z"),
            ],
        );
        let tree = load(&path, None).unwrap();

        let root_children: Vec<_> = tree.children(ROOT_INODE).unwrap().collect();
        assert_eq!(tree.get(ROOT_INODE).unwrap().attr.nlink, 3); // self + ..parent + a
        let a_ino = root_children[0].1.attr.ino.0;
        assert_eq!(tree.get(a_ino).unwrap().attr.nlink, 3); // self + b
        let b_ino = tree
            .children(a_ino)
            .unwrap()
            .find(|(n, _)| n.to_str() == Some("b"))
            .unwrap()
            .1
            .attr
            .ino
            .0;
        assert_eq!(tree.get(b_ino).unwrap().attr.nlink, 4); // self + c + d
        let c_ino = tree
            .children(b_ino)
            .unwrap()
            .find(|(n, _)| n.to_str() == Some("c"))
            .unwrap()
            .1
            .attr
            .ino
            .0;
        assert_eq!(tree.get(c_ino).unwrap().attr.nlink, 2); // self only
    }

    /// xattrs should round-trip through load → save → load. libpna 0.33 has
    /// a per-entry `xattrs` API; before this fix both directions silently
    /// dropped them.
    #[test]
    fn xattrs_round_trip_through_save_and_load() {
        use pna::ExtendedAttribute;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("xattrs.pna");
        let mut archive = Archive::write_header(std::fs::File::create(&path).unwrap()).unwrap();
        let mut builder = pna::EntryBuilder::new_file(
            pna::EntryName::from_lossy("doc.txt"),
            WriteOptions::builder().build(),
        )
        .unwrap();
        builder.add_xattr(ExtendedAttribute::new("user.tag".into(), b"red".to_vec()));
        builder.add_xattr(ExtendedAttribute::new(
            "user.note".into(),
            b"hello".to_vec(),
        ));
        let entry = builder.build().unwrap();
        archive.add_entry(entry).unwrap();
        archive.finalize().unwrap();

        // Load: xattrs should populate FsNode.xattrs.
        let tree = load(&path, None).unwrap();
        let (_, node) = tree
            .children(ROOT_INODE)
            .unwrap()
            .find(|(n, _)| n.to_str() == Some("doc.txt"))
            .unwrap();
        let ino = node.attr.ino.0;
        let xattrs = &tree.get(ino).unwrap().xattrs;
        assert_eq!(xattrs.len(), 2);
        assert_eq!(xattrs.get("user.tag").unwrap(), b"red");
        assert_eq!(xattrs.get("user.note").unwrap(), b"hello");

        // Mark dirty and save round-trip.
        let mut tree = tree;
        // Touch the file to flip Clean -> Dirty so save will rewrite it.
        tree.write_file(ino, 0, b"x").unwrap();
        save(&mut tree).unwrap();
        let reloaded = load(&path, None).unwrap();
        let (_, node) = reloaded
            .children(ROOT_INODE)
            .unwrap()
            .find(|(n, _)| n.to_str() == Some("doc.txt"))
            .unwrap();
        let xa = &reloaded.get(node.attr.ino.0).unwrap().xattrs;
        assert_eq!(xa.len(), 2);
        assert_eq!(xa.get("user.tag").unwrap(), b"red");
    }

    /// Mounting a plaintext archive with `--password` and saving it must
    /// not silently encrypt pre-existing plaintext entries. Only newly
    /// created (`FileData::New`) files should pick up the password's
    /// default cipher.
    #[test]
    fn save_does_not_re_encrypt_existing_plaintext_with_mount_password() {
        let dir = TempDir::new().unwrap();
        let path = create_plain_archive(&dir, "plain.pna", &[("doc.txt", b"plaintext-bytes")]);

        // Mount with a password, but the archive itself is plaintext.
        let mut tree = load(&path, Some("ignored-pw".to_string())).unwrap();
        let (_, node) = tree
            .children(ROOT_INODE)
            .unwrap()
            .find(|(n, _)| n.to_str() == Some("doc.txt"))
            .unwrap();
        let ino = node.attr.ino.0;
        // Touch the file to flip Clean -> Dirty so save will rewrite it.
        tree.write_file(ino, 0, b"x").unwrap();
        save(&mut tree).unwrap();

        // Reload **without** the password — if save re-encrypted, this
        // would now require one and decoding `b"plaintext-bytes"` would
        // fail.
        let plain = load(&path, None).unwrap();
        let (_, node) = plain
            .children(ROOT_INODE)
            .unwrap()
            .find(|(n, _)| n.to_str() == Some("doc.txt"))
            .unwrap();
        assert_eq!(plain.get(node.attr.ino.0).unwrap().attr.size, 15);
    }

    // -----------------------------------------------------------------------
    // save() tests
    // -----------------------------------------------------------------------

    /// Helper: read all data for a node by inode.
    fn read_node_data(tree: &FileTree, ino: u64) -> Vec<u8> {
        let node = tree.get(ino).expect("node not found");
        match &node.content {
            FsContent::File(fd) => fd.data().to_vec(),
            _ => panic!("not a file"),
        }
    }

    /// Helper: read all data for the first child of `parent_ino` in a tree.
    fn read_first_child_data(tree: &FileTree, parent_ino: u64) -> Vec<u8> {
        let child_ino = tree
            .children(parent_ino)
            .unwrap()
            .next()
            .unwrap()
            .1
            .attr
            .ino
            .0;
        read_node_data(tree, child_ino)
    }

    /// Test 1: Clean entries are re-written with the same content.
    #[test]
    fn save_loaded_entries_rewritten() {
        let dir = TempDir::new().unwrap();
        let path = create_plain_archive(&dir, "t1.pna", &[("hello.txt", b"world")]);
        let tree = load(&path, None).unwrap();
        save(&tree).unwrap();

        let tree2 = load(&path, None).unwrap();
        let data = read_first_child_data(&tree2, ROOT_INODE);
        assert_eq!(data, b"world");
    }

    /// Test 2: Dirty data - new content appears in the archive.
    #[test]
    fn save_loaded_modified_data() {
        let dir = TempDir::new().unwrap();
        let path = create_plain_archive(&dir, "t2.pna", &[("file.txt", b"original")]);
        let mut tree = load(&path, None).unwrap();
        let child_ino = tree
            .children(ROOT_INODE)
            .unwrap()
            .next()
            .unwrap()
            .1
            .attr
            .ino
            .0;
        // Write new content (this transitions Clean -> Dirty via write_file).
        tree.write_file(child_ino, 0, b"modified").unwrap();
        save(&tree).unwrap();

        let tree2 = load(&path, None).unwrap();
        let data = read_first_child_data(&tree2, ROOT_INODE);
        assert_eq!(data, b"modified");
    }

    /// Test 3: Dirty entries persist the modified data.
    #[test]
    fn save_modified_entries_persisted() {
        let dir = TempDir::new().unwrap();
        let path = create_plain_archive(&dir, "t3.pna", &[("a.txt", b"aaa")]);
        let mut tree = load(&path, None).unwrap();
        let child_ino = tree
            .children(ROOT_INODE)
            .unwrap()
            .next()
            .unwrap()
            .1
            .attr
            .ino
            .0;
        tree.write_file(child_ino, 0, b"bbb").unwrap();
        save(&tree).unwrap();

        let tree2 = load(&path, None).unwrap();
        let data = read_first_child_data(&tree2, ROOT_INODE);
        assert_eq!(data, b"bbb");
    }

    /// Test 4: Created entries appear in the archive.
    #[test]
    fn save_created_entries_appear() {
        let dir = TempDir::new().unwrap();
        let path = create_plain_archive(&dir, "t4.pna", &[]);
        let mut tree = load(&path, None).unwrap();
        tree.create_file(
            ROOT_INODE,
            std::ffi::OsStr::new("new.txt"),
            0o644,
            Owner::new(0, 0),
        )
        .unwrap();
        let child_ino = tree
            .children(ROOT_INODE)
            .unwrap()
            .next()
            .unwrap()
            .1
            .attr
            .ino
            .0;
        tree.write_file(child_ino, 0, b"new content").unwrap();
        save(&tree).unwrap();

        let tree2 = load(&path, None).unwrap();
        let children: Vec<_> = tree2.children(ROOT_INODE).unwrap().collect();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].0, std::ffi::OsStr::new("new.txt"));
        let data = read_first_child_data(&tree2, ROOT_INODE);
        assert_eq!(data, b"new content");
    }

    /// Test 5: Created entries are encrypted when a password is set.
    #[test]
    fn save_created_entries_encrypted() {
        let dir = TempDir::new().unwrap();
        let path = create_plain_archive(&dir, "t5.pna", &[]);
        let mut tree = load(&path, Some("secretpwd".to_string())).unwrap();
        tree.create_file(
            ROOT_INODE,
            std::ffi::OsStr::new("enc.txt"),
            0o644,
            Owner::new(0, 0),
        )
        .unwrap();
        let child_ino = tree
            .children(ROOT_INODE)
            .unwrap()
            .next()
            .unwrap()
            .1
            .attr
            .ino
            .0;
        tree.write_file(child_ino, 0, b"secret").unwrap();
        save(&tree).unwrap();

        // Must be loadable with password.
        let tree2 = load(&path, Some("secretpwd".to_string())).unwrap();
        let children: Vec<_> = tree2.children(ROOT_INODE).unwrap().collect();
        assert_eq!(children.len(), 1);
        let data = read_first_child_data(&tree2, ROOT_INODE);
        assert_eq!(data, b"secret");

        // The reloaded entry should be Clean and carry a cipher config.
        let child_ino2 = tree2
            .children(ROOT_INODE)
            .unwrap()
            .next()
            .unwrap()
            .1
            .attr
            .ino
            .0;
        let node = tree2.get(child_ino2).unwrap();
        if let FsContent::File(FileData::Clean {
            cipher: Some(c), ..
        }) = &node.content
        {
            assert!(c.encryption != pna::Encryption::No);
        } else {
            panic!("expected Clean with cipher");
        }

        // Verify that loading without password doesn't yield plaintext
        let tree_nopass = load(&path, None);
        // Either load fails entirely, or the data is not readable as plaintext
        if let Ok(s) = tree_nopass {
            let children: Vec<_> = s.children(ROOT_INODE).unwrap().collect();
            if !children.is_empty() {
                let data = read_first_child_data(&s, ROOT_INODE);
                assert_ne!(data, b"secret", "data should be encrypted, not plaintext");
            }
        }
    }

    /// Test 6: File with cipher config but no password returns Err(InvalidInput).
    #[test]
    fn save_cipher_no_password_errors() {
        use pna::{CipherMode, Encryption};

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("enc.pna");
        let mut archive = Archive::write_header(std::fs::File::create(&path).unwrap()).unwrap();
        archive
            .write_file(
                pna::EntryName::from_lossy("s.txt"),
                Metadata::new(),
                WriteOptions::builder()
                    .encryption(Encryption::Aes)
                    .cipher_mode(CipherMode::CTR)
                    .password(Some(b"pwd"))
                    .build(),
                |w| {
                    w.write_all(b"data")?;
                    Ok(())
                },
            )
            .unwrap();
        archive.finalize().unwrap();

        // Load with password (stores cipher config), then try to save without password.
        let mut tree = load(&path, Some("pwd".to_string())).unwrap();
        // The node is already Clean with cipher (all entries decoded at load).
        // Write to transition to Dirty (which preserves cipher).
        let child_ino = tree
            .children(ROOT_INODE)
            .unwrap()
            .next()
            .unwrap()
            .1
            .attr
            .ino
            .0;
        tree.write_file(child_ino, 0, b"data").unwrap();
        // Now clear the password — the node is now Dirty with cipher config.
        tree.clear_password();

        let result = save(&tree);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidInput);
    }

    /// Test 7: Round-trip load -> save -> load gives same tree and content.
    #[test]
    fn save_roundtrip_same_content() {
        let dir = TempDir::new().unwrap();
        let path = create_plain_archive(
            &dir,
            "rt.pna",
            &[("a/b/c.txt", b"nested"), ("top.txt", b"top")],
        );
        let tree1 = load(&path, None).unwrap();
        save(&tree1).unwrap();

        let tree2 = load(&path, None).unwrap();

        // Check top.txt
        let top = tree2.lookup_child(ROOT_INODE, std::ffi::OsStr::new("top.txt"));
        assert!(top.is_some(), "top.txt not found");
        let top_ino = top.unwrap().attr.ino.0;
        let buf = read_node_data(&tree2, top_ino);
        assert_eq!(buf, b"top");

        // Check a/b/c.txt
        let a = tree2
            .lookup_child(ROOT_INODE, std::ffi::OsStr::new("a"))
            .unwrap();
        let a_ino = a.attr.ino.0;
        let b = tree2.children(a_ino).unwrap().next().unwrap();
        let b_ino = b.1.attr.ino.0;
        let c = tree2.children(b_ino).unwrap().next().unwrap();
        let c_ino = c.1.attr.ino.0;
        let buf2 = read_node_data(&tree2, c_ino);
        assert_eq!(buf2, b"nested");
    }

    /// Test 8: Round-trip load -> write -> save -> load preserves written data.
    #[test]
    fn save_roundtrip_write_preserved() {
        let dir = TempDir::new().unwrap();
        let path = create_plain_archive(&dir, "rt2.pna", &[("file.txt", b"old")]);
        let mut tree = load(&path, None).unwrap();
        let child_ino = tree
            .children(ROOT_INODE)
            .unwrap()
            .next()
            .unwrap()
            .1
            .attr
            .ino
            .0;
        tree.write_file(child_ino, 0, b"new").unwrap();
        save(&tree).unwrap();

        let tree2 = load(&path, None).unwrap();
        let data = read_first_child_data(&tree2, ROOT_INODE);
        assert_eq!(data, b"new");
    }

    /// Test 9: Encrypted round-trip -- load with password, save, reload, data matches.
    #[test]
    fn save_encrypted_roundtrip() {
        use pna::{CipherMode, Encryption};

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("enc_rt.pna");
        let mut archive = Archive::write_header(std::fs::File::create(&path).unwrap()).unwrap();
        archive
            .write_file(
                pna::EntryName::from_lossy("data.txt"),
                Metadata::new(),
                WriteOptions::builder()
                    .encryption(Encryption::Aes)
                    .cipher_mode(CipherMode::CTR)
                    .password(Some(b"mypwd"))
                    .build(),
                |w| {
                    w.write_all(b"encrypted content")?;
                    Ok(())
                },
            )
            .unwrap();
        archive.finalize().unwrap();

        // Load + save with password.
        let tree = load(&path, Some("mypwd".to_string())).unwrap();
        save(&tree).unwrap();

        // Reload and verify.
        let tree2 = load(&path, Some("mypwd".to_string())).unwrap();
        let data = read_first_child_data(&tree2, ROOT_INODE);
        assert_eq!(data, b"encrypted content");
    }

    /// Test 10: Stale tmp file before save is cleaned up, and new tmp is renamed.
    #[test]
    fn save_stale_tmp_cleaned() {
        let dir = TempDir::new().unwrap();
        let path = create_plain_archive(&dir, "stale.pna", &[("f.txt", b"data")]);

        // Plant a stale tmp file.
        let stale = dir.path().join(".stale.pna.tmp.99999");
        std::fs::write(&stale, b"leftover").unwrap();

        let tree = load(&path, None).unwrap();
        save(&tree).unwrap();

        // Stale file should be gone.
        assert!(!stale.exists(), "stale tmp should be cleaned up");
        // Archive should still be valid.
        let tree2 = load(&path, None).unwrap();
        let data = read_first_child_data(&tree2, ROOT_INODE);
        assert_eq!(data, b"data");
    }

    /// fSIZ is only a hint and must not be trusted, so even entries created
    /// via EntryBuilder (which writes fSIZ) are fully decoded on load.
    #[test]
    fn load_entry_with_fsiz_is_still_loaded() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("fsiz.pna");
        let mut archive = Archive::write_header(std::fs::File::create(&path).unwrap()).unwrap();

        // Use EntryBuilder which writes fSIZ chunk
        let mut builder = EntryBuilder::new_file(
            pna::EntryName::from_lossy("sized.txt"),
            WriteOptions::builder().build(),
        )
        .unwrap();
        builder.write_all(b"has known size").unwrap();
        archive.add_entry(builder.build().unwrap()).unwrap();
        archive.finalize().unwrap();

        let tree = load(&path, None).unwrap();
        let children: Vec<_> = tree.children(ROOT_INODE).unwrap().collect();
        assert_eq!(children.len(), 1);
        // fSIZ is not trusted -- entry is always fully decoded
        assert!(matches!(
            children[0].1.content,
            FsContent::File(FileData::Clean { .. })
        ));
        // And attr.size should be correct
        assert_eq!(children[0].1.attr.size, 14); // "has known size" = 14 bytes
    }

    /// Test: directory mtime survives save->load round-trip.
    #[test]
    fn save_roundtrip_preserves_directory_mtime() {
        let dir = TempDir::new().unwrap();
        let path = create_plain_archive(&dir, "dirs.pna", &[]);
        let mut tree = load(&path, None).unwrap();
        // Create a directory and set a specific mtime
        tree.make_dir(
            ROOT_INODE,
            std::ffi::OsStr::new("mydir"),
            0o755,
            0,
            Owner::new(0, 0),
        )
        .unwrap();
        let dir_ino = tree
            .lookup_child(ROOT_INODE, std::ffi::OsStr::new("mydir"))
            .unwrap()
            .attr
            .ino
            .0;
        let t = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1700000000);
        tree.set_times(dir_ino, None, Some(fuser::TimeOrNow::SpecificTime(t)))
            .unwrap();
        save(&tree).unwrap();
        let tree2 = load(&path, None).unwrap();
        let dir_node = tree2
            .lookup_child(ROOT_INODE, std::ffi::OsStr::new("mydir"))
            .unwrap();
        // Verify mtime survived (truncated to seconds)
        let mtime_secs = dir_node
            .attr
            .mtime
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        assert_eq!(mtime_secs, 1700000000);
    }

    /// Test: unlinking a file and saving removes it from the archive.
    #[test]
    fn save_after_unlink_removes_entry() {
        let dir = TempDir::new().unwrap();
        let path =
            create_plain_archive(&dir, "unlink.pna", &[("a.txt", b"aaa"), ("b.txt", b"bbb")]);
        let mut tree = load(&path, None).unwrap();
        tree.unlink(ROOT_INODE, std::ffi::OsStr::new("a.txt"))
            .unwrap();
        save(&tree).unwrap();
        let tree2 = load(&path, None).unwrap();
        let children: Vec<_> = tree2.children(ROOT_INODE).unwrap().collect();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].0, std::ffi::OsStr::new("b.txt"));
    }

    /// Regression test: when a PNA archive has file entries before their parent
    /// directory entry (e.g., "dir/file.txt" then "dir"), the directory
    /// replacement must preserve the children already inserted.
    #[test]
    fn load_dir_entry_after_child_preserves_children() {
        use std::io::Write as IoWrite;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("ordered.pna");
        let mut archive = Archive::write_header(std::fs::File::create(&path).unwrap()).unwrap();
        // Write file BEFORE its parent directory entry
        archive
            .write_file(
                pna::EntryName::from_lossy("mydir/child.txt"),
                Metadata::new(),
                WriteOptions::builder().build(),
                |w| {
                    w.write_all(b"hello")?;
                    Ok(())
                },
            )
            .unwrap();
        // Now write explicit directory entry for "mydir"
        let dir_entry = EntryBuilder::new_dir(pna::EntryName::from_lossy("mydir"))
            .build()
            .unwrap();
        archive.add_entry(dir_entry).unwrap();
        archive.finalize().unwrap();

        let tree = load(&path, None).unwrap();
        // "mydir" should exist as a directory under root
        let mydir = tree
            .lookup_child(ROOT_INODE, std::ffi::OsStr::new("mydir"))
            .expect("mydir not found");
        let mydir_ino = mydir.attr.ino.0;
        // "child.txt" should still be a child of "mydir" (not orphaned)
        let child = tree
            .lookup_child(mydir_ino, std::ffi::OsStr::new("child.txt"))
            .expect("child.txt was orphaned by directory replacement");
        let data = match &child.content {
            FsContent::File(fd) => fd.data(),
            _ => panic!("expected file"),
        };
        assert_eq!(data, b"hello");
    }

    #[test]
    fn save_then_load_roundtrips_hardlink() {
        let dir = TempDir::new().unwrap();
        let archive_path = dir.path().join("hl.pna");
        // Build a tree with a file and a hardlink to it.
        let mut tree = crate::file_tree::FileTree::new_for_test(archive_path.clone(), None);
        let original = tree
            .create_file(
                ROOT_INODE,
                std::ffi::OsStr::new("orig.txt"),
                0o644,
                Owner::new(0, 0),
            )
            .unwrap();
        let original_ino = original.attr.ino.0;
        tree.write_file(original_ino, 0, b"shared bytes").unwrap();
        tree.create_hardlink(ROOT_INODE, std::ffi::OsStr::new("alias.txt"), original_ino)
            .unwrap();

        save(&tree).unwrap();

        let reloaded = load(&archive_path, None).unwrap();
        let a = reloaded
            .lookup_child(ROOT_INODE, std::ffi::OsStr::new("orig.txt"))
            .unwrap();
        let b = reloaded
            .lookup_child(ROOT_INODE, std::ffi::OsStr::new("alias.txt"))
            .unwrap();
        assert_eq!(a.attr.ino.0, b.attr.ino.0, "hardlink should share inode");
        assert_eq!(a.attr.nlink, 2);
        // Both names see the same bytes.
        let bytes = match &a.content {
            FsContent::File(fd) => fd.data(),
            _ => panic!("expected regular file content"),
        };
        assert_eq!(bytes, b"shared bytes");
    }
}
