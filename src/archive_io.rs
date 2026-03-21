use crate::file_tree::{
    CipherConfig, DirContent, FileData, FileTree, FsContent, FsNode, ROOT_INODE, get_gid, get_uid,
    make_dir_node,
};
use fuser::{FileAttr, FileType, INodeNo};
use pna::{
    Archive, DataKind, EntryBuilder, EntryName, HashAlgorithm, Metadata, NormalEntry, ReadEntry,
    ReadOptions, WriteOptions,
};
use std::collections::HashMap;
use std::io::{Read, Write as IoWrite};
use std::path::Path;
use std::process;
use std::time::SystemTime;
use std::{fs, io};

/// Remove any leftover `.{stem}.tmp.{pid}` files next to `archive_path`.
pub(crate) fn cleanup_stale_tmp(archive_path: &Path) {
    let dir = archive_path.parent().unwrap_or(Path::new("."));
    let stem = match archive_path.file_name() {
        Some(s) => s.to_string_lossy().into_owned(),
        None => return,
    };
    let prefix = format!(".{stem}.tmp.");
    if let Ok(rd) = dir.read_dir() {
        for entry in rd.flatten() {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            // Extract PID from filename and skip if process is still alive.
            if let Some(pid_str) = name_str.strip_prefix(&prefix) {
                if let Ok(pid) = pid_str.parse::<u32>() {
                    #[cfg(unix)]
                    {
                        let ret = unsafe { libc::kill(pid as i32, 0) };
                        if ret == 0 {
                            continue;
                        }
                        let err = std::io::Error::last_os_error();
                        if err.raw_os_error() == Some(libc::EPERM) {
                            continue;
                        }
                    }
                    let _ = fs::remove_file(entry.path());
                }
            }
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

    // Insert root directory node (no parent).
    let root = make_dir_node(ROOT_INODE, ".".into());
    tree.insert_node(root, None)?;

    let pw = password_bytes.as_deref();

    for entry in archive.entries_slice() {
        let entry = entry?;
        match entry {
            ReadEntry::Normal(e) => {
                let owned: NormalEntry<Vec<u8>> = e.into();
                add_normal_entry(&mut tree, owned, pw)?;
            }
            ReadEntry::Solid(solid) => {
                for e in solid.entries(pw)? {
                    let e = e?;
                    add_normal_entry(&mut tree, e, pw)?;
                }
            }
        }
    }

    // No need to reset dirty — FileTree::new() starts with dirty=false
    // and insert_node() does not set dirty.

    Ok(tree)
}

fn add_normal_entry(
    tree: &mut FileTree,
    entry: NormalEntry<Vec<u8>>,
    password: Option<&[u8]>,
) -> io::Result<()> {
    let now = SystemTime::now();
    let header = entry.header();
    let metadata = entry.metadata();
    let entry_path = header.path().as_path().to_path_buf();

    // Determine parent inode.
    let parent_ino = match entry_path.parent() {
        Some(p) if !p.as_os_str().is_empty() => tree.make_dir_all(p, ROOT_INODE)?,
        _ => ROOT_INODE,
    };

    // Name = last path component.
    let name = match entry_path.components().next_back() {
        Some(c) => c.as_os_str().to_owned(),
        None => return Ok(()),
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
            DataKind::File | DataKind::HardLink => FileType::RegularFile,
            DataKind::Directory => FileType::Directory,
            DataKind::SymbolicLink => FileType::Symlink,
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
        _ => {
            // RegularFile / HardLink
            // Always decode: fSIZ is only a hint and must not be trusted for
            // attr.size or any load-strategy decisions.  The only reliable
            // source of the true file size is the decoded data itself.
            let mut buf = Vec::new();
            entry.reader(&opts)?.read_to_end(&mut buf)?;
            attr.size = buf.len() as u64;
            FsContent::File(FileData::Clean { data: buf, cipher })
        }
    };

    let node = FsNode {
        name,
        parent: None,
        attr,
        content,
        xattrs: HashMap::new(),
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
            *existing_node = FsNode {
                name: node.name,
                parent,
                attr: FileAttr {
                    ino: INodeNo(existing),
                    ..node.attr
                },
                content: preserved.unwrap_or(node.content),
                xattrs: node.xattrs,
            };
        }
    } else {
        tree.insert_node(node, Some(parent_ino))?;
    }

    Ok(())
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
        {
            if tree.password().is_none() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "cannot re-encrypt: archive requires password but none was provided",
                ));
            }
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

        for (_, node, archive_path_str) in &nodes {
            let entry_name = EntryName::from_lossy(archive_path_str);
            let metadata = build_metadata(node);

            match &node.content {
                FsContent::Directory(_) => {
                    // Write explicit directory entry with metadata (mtime, crtime).
                    let mut builder = EntryBuilder::new_dir(entry_name);
                    builder.modified(system_time_to_pna(node.attr.mtime));
                    builder.created(system_time_to_pna(node.attr.crtime));
                    let dir_entry = builder.build()?;
                    archive.add_entry(dir_entry)?;
                }
                FsContent::Symlink(_target) => {
                    log::warn!(
                        "save: skipping symlink entry '{}' (not yet supported)",
                        archive_path_str
                    );
                }
                FsContent::File(fc) => {
                    let write_opts = build_write_options(fc.cipher(), tree.password())?;
                    archive.write_file(entry_name, metadata, write_opts, |w| {
                        w.write_all(fc.data())?;
                        Ok(())
                    })?;
                }
            }
        }

        // Finalize returns the inner writer so we can sync before rename.
        let inner = archive.finalize()?;
        inner.sync_all()?;
        drop(inner);

        fs::rename(&tmp_path, archive_path)?;
        if let Some(parent_dir) = archive_path.parent() {
            if let Ok(dir_file) = fs::File::open(parent_dir) {
                let _ = dir_file.sync_all();
            }
        }
        Ok(())
    })();

    if result.is_err() {
        let _ = fs::remove_file(&tmp_path);
    }

    result
}

/// Build `WriteOptions` for a given optional cipher config and password.
fn build_write_options(
    cipher: Option<&CipherConfig>,
    password: Option<&str>,
) -> io::Result<WriteOptions> {
    let effective = cipher
        .copied()
        .or_else(|| password.map(|_| CipherConfig::default_for_password()));
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

/// Build pna `Metadata` from node attributes.
fn build_metadata(node: &FsNode) -> Metadata {
    let mtime = system_time_to_pna(node.attr.mtime);
    let crtime = system_time_to_pna(node.attr.crtime);
    Metadata::new().with_modified(mtime).with_created(crtime)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_tree::ROOT_INODE;
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
        assert_eq!(children[0].name, std::ffi::OsStr::new("hello.txt"));
        assert!(matches!(
            children[0].content,
            FsContent::File(FileData::Clean { .. })
        ));
        assert_eq!(children[0].attr.size, 5); // b"world"
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
        assert_eq!(root_children[0].name.to_str().unwrap(), "a");

        // "a" should have exactly one child: "b" (a directory).
        let a_ino = root_children[0].attr.ino.0;
        let a_children: Vec<_> = tree.children(a_ino).unwrap().collect();
        assert_eq!(a_children.len(), 1);
        assert_eq!(a_children[0].name.to_str().unwrap(), "b");

        // "b" should have exactly one child: "c.txt".
        let b_ino = a_children[0].attr.ino.0;
        let b_children: Vec<_> = tree.children(b_ino).unwrap().collect();
        assert_eq!(b_children.len(), 1);
        assert_eq!(b_children[0].name.to_str().unwrap(), "c.txt");
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
        assert_eq!(children[0].name, std::ffi::OsStr::new("solid_file.txt"));
        assert!(matches!(
            children[0].content,
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
        assert_eq!(children[0].name, std::ffi::OsStr::new("secret.txt"));
        assert!(matches!(
            children[0].content,
            FsContent::File(FileData::Clean { .. })
        ));
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
        tree.create_file(ROOT_INODE, std::ffi::OsStr::new("new.txt"), 0o644)
            .unwrap();
        let child_ino = tree
            .children(ROOT_INODE)
            .unwrap()
            .next()
            .unwrap()
            .attr
            .ino
            .0;
        tree.write_file(child_ino, 0, b"new content").unwrap();
        save(&tree).unwrap();

        let tree2 = load(&path, None).unwrap();
        let children: Vec<_> = tree2.children(ROOT_INODE).unwrap().collect();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].name, std::ffi::OsStr::new("new.txt"));
        let data = read_first_child_data(&tree2, ROOT_INODE);
        assert_eq!(data, b"new content");
    }

    /// Test 5: Created entries are encrypted when a password is set.
    #[test]
    fn save_created_entries_encrypted() {
        let dir = TempDir::new().unwrap();
        let path = create_plain_archive(&dir, "t5.pna", &[]);
        let mut tree = load(&path, Some("secretpwd".to_string())).unwrap();
        tree.create_file(ROOT_INODE, std::ffi::OsStr::new("enc.txt"), 0o644)
            .unwrap();
        let child_ino = tree
            .children(ROOT_INODE)
            .unwrap()
            .next()
            .unwrap()
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
        let b_ino = b.attr.ino.0;
        let c = tree2.children(b_ino).unwrap().next().unwrap();
        let c_ino = c.attr.ino.0;
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
            children[0].content,
            FsContent::File(FileData::Clean { .. })
        ));
        // And attr.size should be correct
        assert_eq!(children[0].attr.size, 14); // "has known size" = 14 bytes
    }

    /// Test: directory mtime survives save->load round-trip.
    #[test]
    fn save_roundtrip_preserves_directory_mtime() {
        let dir = TempDir::new().unwrap();
        let path = create_plain_archive(&dir, "dirs.pna", &[]);
        let mut tree = load(&path, None).unwrap();
        // Create a directory and set a specific mtime
        tree.make_dir(ROOT_INODE, std::ffi::OsStr::new("mydir"), 0o755, 0)
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
        assert_eq!(children[0].name, std::ffi::OsStr::new("b.txt"));
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
}
