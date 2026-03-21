use crate::archive_store::{
    ArchiveStore, CipherConfig, FileContent, Node, NodeContent, ROOT_INODE, get_gid, get_uid,
    make_dir_node,
};
use fuser::{FileAttr, FileType, INodeNo};
use id_tree::NodeId;
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
                    if unsafe { libc::kill(pid as i32, 0) } == 0 {
                        continue;
                    }
                }
                let _ = fs::remove_file(entry.path());
            }
        }
    }
}

/// Load a PNA archive from `archive_path`, optionally decrypting with `password`.
pub(crate) fn load(archive_path: &Path, password: Option<String>) -> io::Result<ArchiveStore> {
    cleanup_stale_tmp(archive_path);

    let file = fs::File::open(archive_path)?;
    let memmap = unsafe { memmap2::Mmap::map(&file) }?;

    // Derive password bytes before moving `password` into the store.
    let password_bytes: Option<Vec<u8>> = password.as_deref().map(|s| s.as_bytes().to_vec());

    let mut archive = Archive::read_header_from_slice(&memmap[..])?;

    let mut store = ArchiveStore::new(archive_path.to_path_buf(), password);

    // Insert root directory node (no parent).
    let root = make_dir_node(ROOT_INODE, ".".into());
    store.insert_node(root, None)?;

    let pw = password_bytes.as_deref();

    for entry in archive.entries_slice() {
        let entry = entry?;
        match entry {
            ReadEntry::Normal(e) => {
                let owned: NormalEntry<Vec<u8>> = e.into();
                add_normal_entry(&mut store, owned, pw)?;
            }
            ReadEntry::Solid(solid) => {
                for e in solid.entries(pw)? {
                    let e = e?;
                    add_normal_entry(&mut store, e, pw)?;
                }
            }
        }
    }

    // Loading from disk — store is clean.
    store.dirty = false;

    Ok(store)
}

fn add_normal_entry(
    store: &mut ArchiveStore,
    entry: NormalEntry<Vec<u8>>,
    password: Option<&[u8]>,
) -> io::Result<()> {
    let now = SystemTime::now();
    let header = entry.header();
    let metadata = entry.metadata();
    let entry_path = header.path().as_path().to_path_buf();

    // Determine parent inode.
    let parent_ino = match entry_path.parent() {
        Some(p) if !p.as_os_str().is_empty() => store.make_dir_all(p, ROOT_INODE)?,
        _ => ROOT_INODE,
    };

    // Name = last path component.
    let name = match entry_path.components().next_back() {
        Some(c) => c.as_os_str().to_owned(),
        None => return Ok(()),
    };

    let ino = store.next_inode();

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
        DataKind::Directory => NodeContent::Directory,
        DataKind::SymbolicLink => {
            let mut buf = Vec::new();
            entry.reader(&opts)?.read_to_end(&mut buf)?;
            NodeContent::Symlink(std::ffi::OsString::from(
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
            NodeContent::File(FileContent::Loaded { data: buf, cipher })
        }
    };

    let node = Node {
        name,
        attr,
        content,
        xattrs: HashMap::new(),
    };

    // If a node with this name already exists under parent (e.g. incremental
    // archives), update it in-place reusing the existing inode.
    let existing_ino = store.get_children(parent_ino).and_then(|children| {
        children
            .iter()
            .find(|n| n.name == node.name)
            .map(|n| n.attr.ino.0)
    });

    if let Some(existing) = existing_ino {
        if let Some(existing_node) = store.get_node_mut(existing) {
            *existing_node = Node {
                name: node.name,
                attr: FileAttr {
                    ino: INodeNo(existing),
                    ..node.attr
                },
                content: node.content,
                xattrs: node.xattrs,
            };
        }
    } else {
        store.insert_node(node, Some(parent_ino))?;
    }

    Ok(())
}

/// Save the in-memory `ArchiveStore` back to disk atomically.
///
/// Writes to a temporary file `.{stem}.tmp.{pid}`, finalizes, calls `sync_all()`,
/// then renames the temporary file over the original archive path.
pub(crate) fn save(store: &ArchiveStore) -> io::Result<()> {
    let archive_path = store.archive_path();

    // Password guard: any node that carries a cipher config requires a password.
    for node in store.nodes.values() {
        let needs_password = matches!(
            &node.content,
            NodeContent::File(FileContent::Loaded {
                cipher: Some(_),
                ..
            }) | NodeContent::File(FileContent::Modified {
                cipher: Some(_),
                ..
            })
        );
        if needs_password && store.password().is_none() {
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

        // Traverse tree in DFS order and write every non-root node.
        if let Some(root_tree_id) = store.tree.root_node_id() {
            write_subtree(store, root_tree_id, &mut archive, &mut Vec::new())?;
        }

        // Finalize returns the inner writer so we can sync before rename.
        let inner = archive.finalize()?;
        inner.sync_all()?;
        drop(inner);

        fs::rename(&tmp_path, archive_path)?;
        Ok(())
    })();

    if result.is_err() {
        let _ = fs::remove_file(&tmp_path);
    }

    result
}

/// Recursively write `node_id` and all its descendants into `archive`.
/// `path_components` accumulates the path segments from root down to the current node.
fn write_subtree(
    store: &ArchiveStore,
    node_id: &NodeId,
    archive: &mut Archive<fs::File>,
    path_components: &mut Vec<String>,
) -> io::Result<()> {
    let tree_node = store.tree.get(node_id).map_err(io::Error::other)?;
    let ino = *tree_node.data();

    // The root inode is synthetic — skip writing it but recurse into its children.
    if ino == ROOT_INODE {
        for child_id in store.tree.children_ids(node_id).map_err(io::Error::other)? {
            write_subtree(store, child_id, archive, path_components)?;
        }
        return Ok(());
    }

    let node = store
        .nodes
        .get(&ino)
        .ok_or_else(|| io::Error::other(format!("missing node for inode {ino}")))?;

    // Build the archive path for this node.
    let node_name = node.name.to_string_lossy().into_owned();
    path_components.push(node_name);
    let archive_path_str = path_components.join("/");
    let entry_name = EntryName::from_lossy(&archive_path_str);

    // Build metadata (mtime and crtime where available).
    let metadata = build_metadata(node);

    // Write this node.
    match &node.content {
        NodeContent::Directory => {
            // Write explicit directory entry with metadata (mtime, crtime).
            let mut builder = EntryBuilder::new_dir(entry_name);
            let mtime = node
                .attr
                .mtime
                .duration_since(std::time::UNIX_EPOCH)
                .ok()
                .map(|d| pna::Duration::seconds(d.as_secs() as i64));
            let crtime = node
                .attr
                .crtime
                .duration_since(std::time::UNIX_EPOCH)
                .ok()
                .map(|d| pna::Duration::seconds(d.as_secs() as i64));
            builder.modified(mtime);
            builder.created(crtime);
            let dir_entry = builder.build()?;
            archive.add_entry(dir_entry)?;
        }
        NodeContent::Symlink(_target) => {
            log::warn!(
                "save: skipping symlink entry '{}' (not yet supported)",
                archive_path_str
            );
        }
        NodeContent::File(fc) => {
            match fc {
                FileContent::Unloaded(entry, _opts) => {
                    // Re-add the original NormalEntry verbatim (clone is cheap — Vec<u8>).
                    archive.add_entry(entry.clone())?;
                }
                _ => {
                    let (data, cipher_ref): (&[u8], Option<&CipherConfig>) = match fc {
                        FileContent::Loaded { data, cipher }
                        | FileContent::Modified { data, cipher } => {
                            (data.as_slice(), cipher.as_ref())
                        }
                        FileContent::Created(data) => (data.as_slice(), None),
                        FileContent::Unloaded(..) => unreachable!(),
                    };
                    let write_opts = build_write_options(cipher_ref, store.password())?;
                    archive.write_file(entry_name, metadata, write_opts, |w| {
                        w.write_all(data)?;
                        Ok(())
                    })?;
                }
            }
        }
    }

    // Recurse into children.
    for child_id in store.tree.children_ids(node_id).map_err(io::Error::other)? {
        write_subtree(store, child_id, archive, path_components)?;
    }

    path_components.pop();

    Ok(())
}

/// Build `WriteOptions` for a given optional cipher config and password.
fn build_write_options(
    cipher: Option<&CipherConfig>,
    password: Option<&str>,
) -> io::Result<WriteOptions> {
    match cipher {
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
        None => {
            if let Some(pwd) = password {
                // Store has password; encrypt new/plain entries with AES-CTR.
                Ok(WriteOptions::builder()
                    .encryption(pna::Encryption::Aes)
                    .cipher_mode(pna::CipherMode::CTR)
                    .hash_algorithm(HashAlgorithm::argon2id())
                    .password(Some(pwd.as_bytes()))
                    .build())
            } else {
                Ok(WriteOptions::builder().build())
            }
        }
    }
}

/// Build pna `Metadata` from node attributes.
fn build_metadata(node: &Node) -> Metadata {
    use std::time::UNIX_EPOCH;
    let mtime = node
        .attr
        .mtime
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|d| pna::Duration::seconds(d.as_secs() as i64));
    let crtime = node
        .attr
        .crtime
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|d| pna::Duration::seconds(d.as_secs() as i64));
    Metadata::new().with_modified(mtime).with_created(crtime)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive_store::ROOT_INODE;
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
        let store = load(&path, None).unwrap();
        assert!(store.get_node(ROOT_INODE).is_some());
        assert!(store.get_children(ROOT_INODE).unwrap().is_empty());
        assert!(!store.is_dirty());
    }

    #[test]
    fn load_single_file() {
        let dir = TempDir::new().unwrap();
        let path = create_plain_archive(&dir, "single.pna", &[("hello.txt", b"world")]);
        let store = load(&path, None).unwrap();
        let children = store.get_children(ROOT_INODE).unwrap();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].name, std::ffi::OsStr::new("hello.txt"));
        // create_plain_archive uses write_file() which doesn't write fSIZ,
        // so the entry is force-loaded.
        assert!(matches!(
            children[0].content,
            NodeContent::File(FileContent::Loaded { .. })
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
        let _store = load(&path, None).unwrap();
        assert!(!stale.exists(), "stale tmp should be deleted");
    }

    #[test]
    fn load_nested_dirs() {
        let dir = TempDir::new().unwrap();
        let path = create_plain_archive(&dir, "nested.pna", &[("a/b/c.txt", b"x")]);
        let store = load(&path, None).unwrap();

        // Root should have exactly one child: "a" (a directory).
        let root_children = store.get_children(ROOT_INODE).unwrap();
        assert_eq!(root_children.len(), 1);
        assert_eq!(root_children[0].name.to_str().unwrap(), "a");

        // "a" should have exactly one child: "b" (a directory).
        let a_ino = root_children[0].attr.ino.0;
        let a_children = store.get_children(a_ino).unwrap();
        assert_eq!(a_children.len(), 1);
        assert_eq!(a_children[0].name.to_str().unwrap(), "b");

        // "b" should have exactly one child: "c.txt".
        let b_ino = a_children[0].attr.ino.0;
        let b_children = store.get_children(b_ino).unwrap();
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

        let store = load(&path, None).unwrap();
        let children = store.get_children(ROOT_INODE).unwrap();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].name, std::ffi::OsStr::new("solid_file.txt"));
        assert!(matches!(
            children[0].content,
            NodeContent::File(FileContent::Loaded { .. })
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

        let store = load(&path, Some("testpass".to_string())).unwrap();
        let children = store.get_children(ROOT_INODE).unwrap();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].name, std::ffi::OsStr::new("secret.txt"));
        assert!(matches!(
            children[0].content,
            NodeContent::File(FileContent::Loaded { .. })
        ));
    }

    // -----------------------------------------------------------------------
    // save() tests
    // -----------------------------------------------------------------------

    /// Helper: read all data for a node by inode (handles Unloaded, Loaded, Modified, Created).
    fn read_node_data(store: &ArchiveStore, ino: u64) -> Vec<u8> {
        let node = store.get_node(ino).expect("node not found");
        match &node.content {
            NodeContent::File(FileContent::Loaded { data, .. })
            | NodeContent::File(FileContent::Modified { data, .. })
            | NodeContent::File(FileContent::Created(data)) => data.clone(),
            NodeContent::File(FileContent::Unloaded(entry, opts)) => {
                let mut buf = Vec::new();
                entry.reader(opts).unwrap().read_to_end(&mut buf).unwrap();
                buf
            }
            _ => panic!("not a file"),
        }
    }

    /// Helper: read all data for the first child of `parent_ino` in a store.
    fn read_first_child_data(store: &ArchiveStore, parent_ino: u64) -> Vec<u8> {
        let child_ino = store
            .get_children(parent_ino)
            .unwrap()
            .first()
            .unwrap()
            .attr
            .ino
            .0;
        read_node_data(store, child_ino)
    }

    /// Test 1: Loaded entries are re-written with the same content.
    #[test]
    fn save_loaded_entries_rewritten() {
        let dir = TempDir::new().unwrap();
        let path = create_plain_archive(&dir, "t1.pna", &[("hello.txt", b"world")]);
        let store = load(&path, None).unwrap();
        save(&store).unwrap();

        let store2 = load(&path, None).unwrap();
        let data = read_first_child_data(&store2, ROOT_INODE);
        assert_eq!(data, b"world");
    }

    /// Test 2: Loaded{modified data} - new content appears in the archive.
    #[test]
    fn save_loaded_modified_data() {
        let dir = TempDir::new().unwrap();
        let path = create_plain_archive(&dir, "t2.pna", &[("file.txt", b"original")]);
        let mut store = load(&path, None).unwrap();
        // Force-load and modify the file.
        let child_ino = store
            .get_children(ROOT_INODE)
            .unwrap()
            .first()
            .unwrap()
            .attr
            .ino
            .0;
        // Write new content (this transitions Unloaded → Modified via write_file).
        store.write_file(child_ino, 0, b"modified").unwrap();
        save(&store).unwrap();

        let store2 = load(&path, None).unwrap();
        let data = read_first_child_data(&store2, ROOT_INODE);
        assert_eq!(data, b"modified");
    }

    /// Test 3: Modified entries persist the modified data.
    #[test]
    fn save_modified_entries_persisted() {
        let dir = TempDir::new().unwrap();
        let path = create_plain_archive(&dir, "t3.pna", &[("a.txt", b"aaa")]);
        let mut store = load(&path, None).unwrap();
        let child_ino = store
            .get_children(ROOT_INODE)
            .unwrap()
            .first()
            .unwrap()
            .attr
            .ino
            .0;
        store.write_file(child_ino, 0, b"bbb").unwrap();
        save(&store).unwrap();

        let store2 = load(&path, None).unwrap();
        let data = read_first_child_data(&store2, ROOT_INODE);
        assert_eq!(data, b"bbb");
    }

    /// Test 4: Created entries appear in the archive.
    #[test]
    fn save_created_entries_appear() {
        let dir = TempDir::new().unwrap();
        let path = create_plain_archive(&dir, "t4.pna", &[]);
        let mut store = load(&path, None).unwrap();
        store
            .create_file(ROOT_INODE, std::ffi::OsStr::new("new.txt"), 0o644)
            .unwrap();
        let child_ino = store
            .get_children(ROOT_INODE)
            .unwrap()
            .first()
            .unwrap()
            .attr
            .ino
            .0;
        store.write_file(child_ino, 0, b"new content").unwrap();
        save(&store).unwrap();

        let store2 = load(&path, None).unwrap();
        let children = store2.get_children(ROOT_INODE).unwrap();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].name, std::ffi::OsStr::new("new.txt"));
        let data = read_first_child_data(&store2, ROOT_INODE);
        assert_eq!(data, b"new content");
    }

    /// Test 5: Created entries are encrypted when a password is set.
    #[test]
    fn save_created_entries_encrypted() {
        let dir = TempDir::new().unwrap();
        let path = create_plain_archive(&dir, "t5.pna", &[]);
        let mut store = load(&path, Some("secretpwd".to_string())).unwrap();
        store
            .create_file(ROOT_INODE, std::ffi::OsStr::new("enc.txt"), 0o644)
            .unwrap();
        let child_ino = store
            .get_children(ROOT_INODE)
            .unwrap()
            .first()
            .unwrap()
            .attr
            .ino
            .0;
        store.write_file(child_ino, 0, b"secret").unwrap();
        save(&store).unwrap();

        // Must be loadable with password.
        let store2 = load(&path, Some("secretpwd".to_string())).unwrap();
        let children = store2.get_children(ROOT_INODE).unwrap();
        assert_eq!(children.len(), 1);
        let data = read_first_child_data(&store2, ROOT_INODE);
        assert_eq!(data, b"secret");

        // The reloaded entry should be Loaded (no fSIZ from write_file)
        // and carry a cipher config.
        let child_ino2 = store2
            .get_children(ROOT_INODE)
            .unwrap()
            .first()
            .unwrap()
            .attr
            .ino
            .0;
        let node = store2.get_node(child_ino2).unwrap();
        if let NodeContent::File(FileContent::Loaded {
            cipher: Some(c), ..
        }) = &node.content
        {
            assert!(c.encryption != pna::Encryption::No);
        } else {
            panic!("expected Loaded with cipher");
        }

        // Verify that loading without password doesn't yield plaintext
        let store_nopass = load(&path, None);
        // Either load fails entirely, or the data is not readable as plaintext
        if let Ok(s) = store_nopass {
            let children = s.get_children(ROOT_INODE).unwrap();
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
        let mut store = load(&path, Some("pwd".to_string())).unwrap();
        // Force-load so the node transitions from Unloaded to Loaded (which carries cipher).
        // We do this by triggering write_file which internally force-loads.
        let child_ino = store
            .get_children(ROOT_INODE)
            .unwrap()
            .first()
            .unwrap()
            .attr
            .ino
            .0;
        // Trigger force-load by writing (appending zero bytes doesn't change content).
        store.write_file(child_ino, 0, b"data").unwrap();
        // Now clear the password — the node is now Loaded/Modified with cipher config.
        store.password = None;

        let result = save(&store);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidInput);
    }

    /// Test 7: Round-trip load → save → load gives same tree and content.
    #[test]
    fn save_roundtrip_same_content() {
        let dir = TempDir::new().unwrap();
        let path = create_plain_archive(
            &dir,
            "rt.pna",
            &[("a/b/c.txt", b"nested"), ("top.txt", b"top")],
        );
        let store1 = load(&path, None).unwrap();
        save(&store1).unwrap();

        let store2 = load(&path, None).unwrap();

        // Check top.txt
        let root_children = store2.get_children(ROOT_INODE).unwrap();
        let top = root_children
            .iter()
            .find(|n| n.name == std::ffi::OsStr::new("top.txt"));
        assert!(top.is_some(), "top.txt not found");
        let top_ino = top.unwrap().attr.ino.0;
        let buf = read_node_data(&store2, top_ino);
        assert_eq!(buf, b"top");

        // Check a/b/c.txt
        let a = root_children
            .iter()
            .find(|n| n.name == std::ffi::OsStr::new("a"))
            .unwrap();
        let a_ino = a.attr.ino.0;
        let b_children = store2.get_children(a_ino).unwrap();
        let b = b_children.first().unwrap();
        let b_ino = b.attr.ino.0;
        let c_children = store2.get_children(b_ino).unwrap();
        let c = c_children.first().unwrap();
        let c_ino = c.attr.ino.0;
        let buf2 = read_node_data(&store2, c_ino);
        assert_eq!(buf2, b"nested");
    }

    /// Test 8: Round-trip load → write → save → load preserves written data.
    #[test]
    fn save_roundtrip_write_preserved() {
        let dir = TempDir::new().unwrap();
        let path = create_plain_archive(&dir, "rt2.pna", &[("file.txt", b"old")]);
        let mut store = load(&path, None).unwrap();
        let child_ino = store
            .get_children(ROOT_INODE)
            .unwrap()
            .first()
            .unwrap()
            .attr
            .ino
            .0;
        store.write_file(child_ino, 0, b"new").unwrap();
        save(&store).unwrap();

        let store2 = load(&path, None).unwrap();
        let data = read_first_child_data(&store2, ROOT_INODE);
        assert_eq!(data, b"new");
    }

    /// Test 9: Encrypted round-trip — load with password, save, reload, data matches.
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
        let store = load(&path, Some("mypwd".to_string())).unwrap();
        save(&store).unwrap();

        // Reload and verify.
        let store2 = load(&path, Some("mypwd".to_string())).unwrap();
        let data = read_first_child_data(&store2, ROOT_INODE);
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

        let store = load(&path, None).unwrap();
        save(&store).unwrap();

        // Stale file should be gone.
        assert!(!stale.exists(), "stale tmp should be cleaned up");
        // Archive should still be valid.
        let store2 = load(&path, None).unwrap();
        let data = read_first_child_data(&store2, ROOT_INODE);
        assert_eq!(data, b"data");
    }

    /// EntryBuilder writes fSIZ, so entries created with it should stay Unloaded
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

        let store = load(&path, None).unwrap();
        let children = store.get_children(ROOT_INODE).unwrap();
        assert_eq!(children.len(), 1);
        // fSIZ is not trusted — entry is always fully decoded
        assert!(matches!(
            children[0].content,
            NodeContent::File(FileContent::Loaded { .. })
        ));
        // And attr.size should be correct
        assert_eq!(children[0].attr.size, 14); // "has known size" = 14 bytes
    }

    /// Test: directory mtime survives save→load round-trip.
    #[test]
    fn save_roundtrip_preserves_directory_mtime() {
        let dir = TempDir::new().unwrap();
        let path = create_plain_archive(&dir, "dirs.pna", &[]);
        let mut store = load(&path, None).unwrap();
        // Create a directory and set a specific mtime
        store
            .make_dir(ROOT_INODE, std::ffi::OsStr::new("mydir"), 0o755, 0)
            .unwrap();
        let dir_ino = store
            .get_children(ROOT_INODE)
            .unwrap()
            .iter()
            .find(|n| n.name == std::ffi::OsStr::new("mydir"))
            .unwrap()
            .attr
            .ino
            .0;
        let t = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1700000000);
        store
            .set_times(dir_ino, None, Some(fuser::TimeOrNow::SpecificTime(t)))
            .unwrap();
        save(&store).unwrap();
        let store2 = load(&path, None).unwrap();
        let children = store2.get_children(ROOT_INODE).unwrap();
        let dir_node = children
            .iter()
            .find(|n| n.name == std::ffi::OsStr::new("mydir"))
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
        let mut store = load(&path, None).unwrap();
        store
            .unlink(ROOT_INODE, std::ffi::OsStr::new("a.txt"))
            .unwrap();
        save(&store).unwrap();
        let store2 = load(&path, None).unwrap();
        let children = store2.get_children(ROOT_INODE).unwrap();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].name, std::ffi::OsStr::new("b.txt"));
    }
}
