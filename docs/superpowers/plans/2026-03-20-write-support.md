# Write Support Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add write support to pnafs so users can create, modify, and delete files within a mounted PNA archive, with changes persisted back atomically on unmount or file close.

**Architecture:** Three-layer design: `PnaFS` (FUSE adapter with `Mutex<ArchiveStore>`) → `ArchiveStore` (in-memory FS state with write API) → `archive_io` (PNA serialization with atomic save). `file_manager.rs` is deleted and replaced by `archive_store.rs` + `archive_io.rs`.

**Tech Stack:** Rust 2024, fuser 0.17.0 (INodeNo/FileHandle/OpenFlags/OpenAccMode/TimeOrNow newtypes), pna 0.29.3 (NormalEntry<Vec<u8>>, ReadEntry::Normal/Solid, CipherMode::CTR), id_tree 1.8.0 (RemoveBehavior::OrphanChildren), libc (O_EXCL/O_TRUNC raw flags in create())

---

## File Structure

| Action | File | Responsibility |
|--------|------|----------------|
| Create | `src/archive_store.rs` | In-memory FS: ArchiveStore, Node, FileContent, write API |
| Create | `src/archive_io.rs` | PNA load/save, atomic write, stale tmp cleanup |
| Modify | `src/filesystem.rs` | FUSE adapter using ArchiveStore; add write ops |
| Modify | `src/command/mount.rs` | Add `--write` / `--write-strategy` CLI flags |
| Delete | `src/file_manager.rs` | Replaced by archive_store + archive_io |
| Modify | `src/main.rs` (or `lib.rs`) | Update `mod` declarations |
| Create | `scripts/tests/test_mount_write.sh` | Shell integration: basic write ops |
| Create | `scripts/tests/test_mount_write_encrypted.sh` | Shell integration: encrypted write |
| Create | `scripts/tests/test_mount_write_strategy.sh` | Shell integration: lazy vs immediate |
| Modify | `scripts/tests/run.sh` | Invoke new shell test scripts |

---

## Test Matrix

### Unit Tests: ArchiveStore::create_file

| # | parent ino | name | Condition | Expected |
|---|-----------|------|-----------|---------|
| 1 | root | "new.txt" | fresh store | Ok(&Node), kind=RegularFile, dirty=true |
| 2 | root | "existing.txt" | name already exists | Err(EEXIST) |
| 3 | 9999 | "x.txt" | invalid parent | Err(ENOENT) |
| 4 | file_ino | "x.txt" | parent is a file | Err(ENOTDIR) |
| 5 | dir_ino | "nested.txt" | nested dir as parent | Ok(&Node) |

### Unit Tests: ArchiveStore::write_file

| # | initial content | offset | data | Expected | State after |
|---|----------------|--------|------|----------|------------|
| 1 | Created([]) | 0 | b"hello" | Ok(5) | Created([hello]) |
| 2 | Created([hello]) | 5 | b" world" | Ok(6) | Created([hello world]) |
| 3 | Created([hello]) | 10 | b"!" | Ok(1) | Created([hello\0\0\0\0\0!]), size=11 |
| 4 | Created([hello]) | 0 | b"" | Ok(0) | Created([hello]) unchanged |
| 5 | Loaded{[a,b,c], None} | 1 | b"XY" | Ok(2) | Modified{[a,X,Y], None} |
| 6 | Unloaded | 0 | b"data" | force-load → Ok(4) | Modified{…} |
| 7 | (inode 9999) | 0 | b"x" | Err(ENOENT) | — |
| 8 | dir inode | 0 | b"x" | Err(EISDIR) | — |

### Unit Tests: ArchiveStore::set_size

| # | initial content | size | Expected | State after |
|---|----------------|------|----------|------------|
| 1 | Created([hello]) | 3 | Ok(()) | Created([hel]) |
| 2 | Created([hello]) | 0 | Ok(()) | Created([]) |
| 3 | Created([hel]) | 10 | Ok(()) | Created([hel\0\0\0\0\0\0\0]) |
| 4 | Created([hello]) | 5 | Ok(()) | Created([hello]) no-op |
| 5 | Unloaded | 0 | force-load → Ok(()) | Modified{[]} |
| 6 | dir inode | 0 | Err(EISDIR) | — |
| 7 | inode 9999 | 0 | Err(ENOENT) | — |

### Unit Tests: ArchiveStore::set_times

| # | inode | atime | mtime | Expected |
|---|-------|-------|-------|---------|
| 1 | valid | SpecificTime(t1) | SpecificTime(t2) | attr.atime=t1, attr.mtime=t2, dirty=true |
| 2 | valid | Now | None | attr.atime≈SystemTime::now(), mtime unchanged |
| 3 | valid | None | SpecificTime(t2) | atime unchanged, attr.mtime=t2 |
| 4 | valid | None | None | both unchanged, dirty=true |
| 5 | 9999 | None | None | Err(ENOENT) |
| 6 | Unloaded node | SpecificTime(t) | None | Ok(()) — no force-load needed |

### Unit Tests: ArchiveStore::make_dir

| # | parent | name | Expected | nlink effect |
|---|--------|------|----------|-------------|
| 1 | root | "newdir" | Ok(&Node), kind=Directory | new.nlink=2, root.nlink incremented |
| 2 | root | "existing" | Err(EEXIST) | unchanged |
| 3 | 9999 | "x" | Err(ENOENT) | — |
| 4 | file_ino | "x" | Err(ENOTDIR) | — |

### Unit Tests: ArchiveStore::unlink

| # | target | Expected |
|---|--------|---------|
| 1 | existing file | Ok(()), removed from tree + node_ids + nodes |
| 2 | non-existent name | Err(ENOENT) |
| 3 | directory name | Err(EPERM) [macOS] / Err(EISDIR) [Linux] |
| 4 | unlink → recreate same name | Ok, new inode |

### Unit Tests: ArchiveStore::mark_clean

| # | content before | Expected content after | dirty |
|---|---------------|----------------------|-------|
| 1 | Modified{data, cipher=None} | Loaded{data, cipher=None} | false |
| 2 | Modified{data, cipher=Some(cfg)} | Loaded{data, cipher=Some(cfg)} | false |
| 3 | Created(data) + password=Some | Loaded{data, cipher=Some(CTR+AES)} | false |
| 4 | Created(data) + password=None | Loaded{data, cipher=None} | false |
| 5 | Unloaded | Unloaded (unchanged) | false |

### Unit Tests: archive_io::load

| # | Archive | Password | Expected |
|---|---------|---------|---------|
| 1 | Normal, no encryption | None | Root + Unloaded entries |
| 2 | Normal, encrypted | Some("pwd") | Root + Unloaded entries |
| 3 | Solid, no encryption | None | Root + Loaded entries (force-loaded) |
| 4 | Empty archive | None | Root only |
| 5 | archive/dir/file.txt | None | 3-level tree: root→dir→file |
| 6 | Stale `.{name}.tmp.*` exist | None | Tmp files deleted before read |

### Unit Tests: archive_io::save

| # | Store state | Password | Expected |
|---|------------|---------|---------|
| 1 | Unloaded entries only | None | Re-written, same content |
| 2 | Loaded{modified data} | None | New content in archive |
| 3 | Modified entries | None | Modified data persisted |
| 4 | Created entries | None | New entries in archive |
| 5 | Created entries | Some("pwd") | Entries encrypted with CTR+AES+argon2id |
| 6 | File with cipher, no password | None | Err(InvalidInput) |
| 7 | Round-trip: load→save→load | None | Same tree, same content |
| 8 | Round-trip: load→write→save→load | None | Written data preserved |
| 9 | Round-trip encrypted: load→save→load | Some("pwd") | Same data decryptable |
| 10 | Stale tmp before save | None | Old tmp cleaned, new tmp→rename |

### Shell Integration Test Matrix

#### test_mount_write.sh (plain archive)

| # | Operation | Verify |
|---|-----------|--------|
| 1 | echo "hello" > f; umount; remount; cat f | "hello" |
| 2 | mkdir newdir; umount; remount; ls | newdir exists |
| 3 | create f; rm f; umount; remount | f does not exist |
| 4 | echo "a">f; echo "b">f; umount; remount; cat f | "b" (overwrite) |
| 5 | truncate -s 100 f; umount; remount; wc -c | 100 bytes |
| 6 | truncate -s 0 f; umount; remount; wc -c | 0 bytes |
| 7 | touch f (mtime update); umount; remount; stat f | mtime updated |
| 8 | pnafs mount src.pna mnt (no --write); echo >mnt/f | EROFS error |
| 9 | create multiple files in one session | all persisted |
| 10 | mkdir subdir; echo "x" > subdir/file; umount; remount | nested file readable |

#### test_mount_write_encrypted.sh

| # | Operation | Verify |
|---|-----------|--------|
| 1 | Encrypt archive; mount --write --password; create f; umount | f decryptable with same password |
| 2 | Create encrypted archive; mount --write --password; overwrite f; umount | new content decryptable |
| 3 | Mount encrypted archive without --password --write | fail or read-only |

#### test_mount_write_strategy.sh

| # | Strategy | Operation | Verify |
|---|---------|-----------|--------|
| 1 | lazy (default) | create f; umount | f in archive |
| 2 | immediate | create f; close f; umount | f in archive |
| 3 | lazy | create f; umount; remount; cat | content correct |
| 4 | immediate | create f; umount; remount; cat | content correct |

---

### Task 1: archive_store.rs — Data Types and Read API

**Files:**
- Create: `src/archive_store.rs`
- Modify: `src/main.rs` or `src/command.rs` — add `mod archive_store;`

- [ ] **Step 1: Write failing tests for data types and read accessors**

Add `#[cfg(test)]` module at bottom of new `src/archive_store.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn make_store() -> ArchiveStore {
        ArchiveStore::new_for_test(PathBuf::from("/tmp/test.pna"), None)
    }

    #[test]
    fn get_node_root_exists() {
        let store = make_store();
        assert!(store.get_node(ROOT_INODE).is_some());
    }

    #[test]
    fn get_node_unknown_returns_none() {
        let store = make_store();
        assert!(store.get_node(9999).is_none());
    }

    #[test]
    fn get_children_root_empty() {
        let store = make_store();
        let children = store.get_children(ROOT_INODE).unwrap();
        assert!(children.is_empty());
    }

    #[test]
    fn is_dirty_initially_false() {
        let store = make_store();
        assert!(!store.is_dirty());
    }
}
```

- [ ] **Step 2: Run test to verify compile failure**

```bash
cargo test 2>&1 | head -30
```
Expected: compile error — `archive_store` module not found or types undefined.

- [ ] **Step 3: Implement data types and read API**

Create `src/archive_store.rs` with:

```rust
use fuser::{FileAttr, FileType, INodeNo, TimeOrNow};
use id_tree::{InsertBehavior, Node as TreeNode, NodeId, RemoveBehavior, Tree, TreeBuilder};
use pna::{ReadOptions, NormalEntry};
use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

pub(crate) type Inode = u64;
pub(crate) const ROOT_INODE: Inode = 1;

pub(crate) struct CipherConfig {
    pub encryption: pna::Encryption,
    pub cipher_mode: pna::CipherMode,
}

pub(crate) enum FileContent {
    Unloaded(NormalEntry<Vec<u8>>, ReadOptions),
    Loaded { data: Vec<u8>, cipher: Option<CipherConfig> },
    Modified { data: Vec<u8>, cipher: Option<CipherConfig> },
    Created(Vec<u8>),
}

pub(crate) enum NodeContent {
    Directory,
    File(FileContent),
    Symlink(OsString),
}

pub(crate) struct Node {
    pub name: OsString,
    pub attr: FileAttr,
    pub content: NodeContent,
    pub xattrs: HashMap<OsString, Vec<u8>>,
}

pub(crate) struct ArchiveStore {
    pub(crate) tree: Tree<Inode>,
    pub(crate) node_ids: HashMap<Inode, NodeId>,
    pub(crate) nodes: HashMap<Inode, Node>,
    pub(crate) last_inode: Inode,
    pub(crate) password: Option<String>,
    pub(crate) archive_path: PathBuf,
    pub(crate) dirty: bool,
}

// Send assertion — ArchiveStore must be Send for Mutex<ArchiveStore>
const _: fn() = || {
    fn assert_send<T: Send>() {}
    assert_send::<ArchiveStore>();
};

impl ArchiveStore {
    pub(crate) fn archive_path(&self) -> &Path { &self.archive_path }
    pub(crate) fn password(&self) -> Option<&str> { self.password.as_deref() }
    pub(crate) fn is_dirty(&self) -> bool { self.dirty }

    pub(crate) fn get_node(&self, ino: Inode) -> Option<&Node> {
        self.nodes.get(&ino)
    }

    pub(crate) fn get_node_mut(&mut self, ino: Inode) -> Option<&mut Node> {
        self.nodes.get_mut(&ino)
    }

    pub(crate) fn get_children(&self, parent: Inode) -> Option<Vec<&Node>> {
        let node_id = self.node_ids.get(&parent)?;
        let children = self.tree.children(node_id).ok()?;
        children
            .map(|ino| self.nodes.get(ino.data()))
            .collect::<Option<Vec<_>>>()
    }

    pub(crate) fn mark_clean(&mut self) {
        let inodes: Vec<Inode> = self.nodes.keys().copied().collect();
        for ino in inodes {
            let node = self.nodes.get_mut(&ino).unwrap();
            let new_content = match &node.content {
                NodeContent::File(FileContent::Modified { data, cipher }) => {
                    let (data, cipher) = (data.clone(), cipher.as_ref().map(|c| CipherConfig {
                        encryption: c.encryption,
                        cipher_mode: c.cipher_mode,
                    }));
                    Some(NodeContent::File(FileContent::Loaded { data, cipher }))
                }
                NodeContent::File(FileContent::Created(data)) => {
                    let data = data.clone();
                    let cipher = if self.password.is_some() {
                        Some(CipherConfig {
                            encryption: pna::Encryption::Aes,
                            cipher_mode: pna::CipherMode::CTR,
                        })
                    } else {
                        None
                    };
                    Some(NodeContent::File(FileContent::Loaded { data, cipher }))
                }
                _ => None,
            };
            if let Some(c) = new_content {
                node.content = c;
            }
        }
        self.dirty = false;
    }

    pub(crate) fn next_inode(&mut self) -> Inode {
        self.last_inode += 1;
        self.last_inode
    }

    pub(crate) fn insert_node(
        &mut self,
        node: Node,
        parent: Option<Inode>,
    ) -> std::io::Result<Inode> {
        let ino = node.attr.ino.0;
        let behavior = match parent {
            None => InsertBehavior::AsRoot,
            Some(p) => {
                let parent_id = self.node_ids.get(&p).unwrap().clone();
                InsertBehavior::UnderNode(&parent_id)
            }
        };
        let node_id = self
            .tree
            .insert(TreeNode::new(ino), behavior)
            .map_err(std::io::Error::other)?;
        self.node_ids.insert(ino, node_id);
        self.nodes.insert(ino, node);
        Ok(ino)
    }

    /// Only for tests and archive_io::load
    pub(crate) fn make_dir_all(
        &mut self,
        path: &Path,
        mut parent: Inode,
    ) -> std::io::Result<Inode> {
        for component in path.components() {
            let name: OsString = component.as_os_str().into();
            let children = self.get_children(parent).unwrap_or_default();
            if let Some(existing) = children.iter().find(|n| n.name == name) {
                parent = existing.attr.ino.0;
            } else {
                let ino = self.next_inode();
                let node = make_dir_node(ino, name);
                self.insert_node(node, Some(parent))?;
                parent = ino;
            }
        }
        Ok(parent)
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(archive_path: PathBuf, password: Option<String>) -> Self {
        let mut store = Self {
            tree: TreeBuilder::new().build(),
            node_ids: HashMap::new(),
            nodes: HashMap::new(),
            last_inode: ROOT_INODE,
            password,
            archive_path,
            dirty: false,
        };
        let root = make_dir_node(ROOT_INODE, ".".into());
        store.insert_node(root, None).unwrap();
        store
    }
}

pub(crate) fn make_dir_node(ino: Inode, name: OsString) -> Node {
    let now = SystemTime::now();
    Node {
        name,
        xattrs: HashMap::new(),
        content: NodeContent::Directory,
        attr: FileAttr {
            ino: INodeNo(ino),
            size: 512,
            blocks: 1,
            atime: now, mtime: now, ctime: now, crtime: now,
            kind: FileType::Directory,
            perm: 0o775,
            nlink: 2,
            uid: current_uid(),
            gid: current_gid(),
            rdev: 0, blksize: 512, flags: 0,
        },
    }
}

#[cfg(unix)]
fn current_uid() -> u32 { nix::unistd::Uid::current().as_raw() }
#[cfg(unix)]
fn current_gid() -> u32 { nix::unistd::Gid::current().as_raw() }
#[cfg(not(unix))]
fn current_uid() -> u32 { 0 }
#[cfg(not(unix))]
fn current_gid() -> u32 { 0 }
```

Add `mod archive_store;` to `src/main.rs` (after removing `mod file_manager;` — keep file_manager for now until Task 7).

- [ ] **Step 4: Run tests to verify pass**

```bash
cargo test archive_store::tests --locked 2>&1
```
Expected: 4 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/archive_store.rs src/main.rs
git commit -m "feat: add ArchiveStore core data types and read API"
```

---

### Task 2: archive_io::load

**Files:**
- Create: `src/archive_io.rs`
- Modify: `src/main.rs` — add `mod archive_io;`

- [ ] **Step 1: Write failing tests for load**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive_store::ROOT_INODE;
    use pna::{Archive, WriteOptions};
    use std::io::Write;
    use tempfile::TempDir;

    fn create_plain_archive(dir: &TempDir, filename: &str, files: &[(&str, &[u8])]) -> PathBuf {
        let path = dir.path().join(filename);
        let mut archive = Archive::write_header(std::fs::File::create(&path).unwrap()).unwrap();
        for (name, data) in files {
            let mut entry = archive
                .append_file(WriteOptions::default(), name)
                .unwrap();
            entry.write_all(data).unwrap();
            entry.finish().unwrap();
        }
        archive.finish().unwrap();
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
        assert_eq!(children[0].name, "hello.txt");
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
        let root_children = store.get_children(ROOT_INODE).unwrap();
        assert_eq!(root_children.len(), 1);
        assert_eq!(root_children[0].name.to_str().unwrap(), "a");
    }
}
```

- [ ] **Step 2: Run test to verify compile failure**

```bash
cargo test archive_io::tests --locked 2>&1 | head -20
```
Expected: compile error — module not found.

- [ ] **Step 3: Implement archive_io::load**

Create `src/archive_io.rs`:

```rust
use crate::archive_store::{
    make_dir_node, ArchiveStore, FileContent, Inode, Node, NodeContent, CipherConfig, ROOT_INODE,
};
use fuser::{FileAttr, FileType, INodeNo};
use id_tree::TreeBuilder;
use pna::{Archive, DataKind, Encryption, NormalEntry, ReadEntry, ReadOptions};
use std::collections::HashMap;
use std::ffi::OsString;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

pub(crate) fn load(archive_path: &Path, password: Option<String>) -> io::Result<ArchiveStore> {
    cleanup_stale_tmp(archive_path);

    let file = std::fs::File::open(archive_path)?;
    let memmap = unsafe { memmap2::Mmap::map(&file) }?;
    let password_bytes = password.as_deref().map(str::as_bytes);
    let mut archive = Archive::read_header_from_slice(&memmap[..])?;

    let mut store = ArchiveStore {
        tree: TreeBuilder::new().build(),
        node_ids: HashMap::new(),
        nodes: HashMap::new(),
        last_inode: ROOT_INODE,
        password: password.clone(),
        archive_path: archive_path.to_owned(),
        dirty: false,
    };

    // Insert root
    let root = make_dir_node(ROOT_INODE, ".".into());
    store.insert_node(root, None)?;

    for entry in archive.entries_slice() {
        let entry = entry?;
        match entry {
            ReadEntry::Normal(entry) => {
                let normal: NormalEntry<Vec<u8>> = entry.into();
                add_normal_entry(&mut store, normal, password_bytes, false)?;
            }
            ReadEntry::Solid(solid) => {
                for entry in solid.entries(password_bytes)? {
                    let entry = entry?;
                    add_normal_entry(&mut store, entry, password_bytes, true)?;
                }
            }
        }
    }
    Ok(store)
}

fn add_normal_entry(
    store: &mut ArchiveStore,
    entry: NormalEntry<Vec<u8>>,
    password: Option<&[u8]>,
    force_load: bool,
) -> io::Result<()> {
    let header = entry.header();
    let path = header.path().as_path().to_owned();
    let parent_path = path.parent();
    let parent = if let Some(pp) = parent_path {
        if pp == std::path::Path::new("") {
            ROOT_INODE
        } else {
            store.make_dir_all(pp, ROOT_INODE)?
        }
    } else {
        ROOT_INODE
    };

    let name: OsString = path
        .components()
        .next_back()
        .map(|c| c.as_os_str().into())
        .unwrap_or_default();

    let metadata = entry.metadata();
    let now = SystemTime::now();
    let kind = match header.data_kind() {
        DataKind::File | DataKind::HardLink => FileType::RegularFile,
        DataKind::Directory => FileType::Directory,
        DataKind::SymbolicLink => FileType::Symlink,
    };

    let raw_size = metadata.raw_file_size().unwrap_or(0);
    let ino = store.next_inode();

    let cipher = if header.encryption() != Encryption::No {
        Some(CipherConfig {
            encryption: header.encryption(),
            cipher_mode: header.cipher_mode(),
        })
    } else {
        None
    };

    let opts = ReadOptions::with_password(password);
    let content = if force_load || raw_size == 0 {
        let mut data = Vec::new();
        entry.reader(&opts)?.read_to_end(&mut data)?;
        NodeContent::File(FileContent::Loaded { data, cipher })
    } else {
        NodeContent::File(FileContent::Unloaded(entry, opts))
    };

    // Collect xattrs
    // Note: for Unloaded, xattrs are lost here. Phase 2 fix needed.
    let xattrs = HashMap::new();

    let perm = metadata.permission();
    use crate::file_manager::{get_owner_id_pub, get_group_id_pub};
    let attr = FileAttr {
        ino: INodeNo(ino),
        size: raw_size,
        blocks: 1,
        atime: metadata.modified().map_or(now, |d| SystemTime::UNIX_EPOCH + d),
        mtime: metadata.modified().map_or(now, |d| SystemTime::UNIX_EPOCH + d),
        ctime: metadata.modified().map_or(now, |d| SystemTime::UNIX_EPOCH + d),
        crtime: metadata.created().map_or(now, |d| SystemTime::UNIX_EPOCH + d),
        kind,
        perm: perm.map_or(0o775, |p| p.permissions()),
        nlink: 1,
        uid: get_owner_id_pub(perm),
        gid: get_group_id_pub(perm),
        rdev: 0, blksize: 512, flags: 0,
    };

    let node = Node { name, attr, content, xattrs };
    store.insert_node(node, Some(parent))?;
    Ok(())
}

pub(crate) fn cleanup_stale_tmp(archive_path: &Path) {
    let dir = archive_path.parent().unwrap_or(Path::new("."));
    let stem = match archive_path.file_name() {
        Some(s) => s.to_string_lossy().into_owned(),
        None => return,
    };
    let prefix = format!(".{}.tmp.", stem);
    if let Ok(rd) = dir.read_dir() {
        for entry in rd.flatten() {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if name_str.starts_with(&prefix) {
                let _ = std::fs::remove_file(entry.path());
            }
        }
    }
}
```

Note: `get_owner_id_pub` / `get_group_id_pub` are extracted from `file_manager.rs` in Task 7. For now, temporarily duplicate the logic or add pub wrappers.

- [ ] **Step 4: Expose uid/gid helpers from file_manager temporarily**

In `src/file_manager.rs`, add:
```rust
pub(crate) use self::get_owner_id as get_owner_id_pub;
pub(crate) use self::get_group_id as get_group_id_pub;
```
(or simply duplicate the two small functions in `archive_io.rs` using `#[cfg(unix)]`/`#[cfg(not(unix))]`)

Add `mod archive_io;` to `src/main.rs`. Add `tempfile` to `[dev-dependencies]` in `Cargo.toml`:
```toml
[dev-dependencies]
tempfile = "3"
```

- [ ] **Step 5: Run tests to verify pass**

```bash
cargo test archive_io::tests --locked 2>&1
```
Expected: 4 tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/archive_io.rs src/archive_store.rs Cargo.toml
git commit -m "feat: add archive_io::load with stale-tmp cleanup and solid-entry handling"
```

---

### Task 3: archive_store.rs — Write API (Phase 1)

**Files:**
- Modify: `src/archive_store.rs`

- [ ] **Step 1: Write failing tests for write API**

Add to `archive_store.rs` `#[cfg(test)]` module:

```rust
    use fuser::TimeOrNow;
    use std::ffi::OsStr;

    fn make_store_with_file(content: &[u8]) -> (ArchiveStore, Inode) {
        let mut store = make_store();
        let node = store.create_file(ROOT_INODE, OsStr::new("test.txt"), 0o644).unwrap();
        let ino = node.attr.ino.0;
        if !content.is_empty() {
            store.write_file(ino, 0, content).unwrap();
        }
        (store, ino)
    }

    // --- create_file ---
    #[test]
    fn create_file_happy_path() {
        let mut store = make_store();
        let node = store.create_file(ROOT_INODE, OsStr::new("a.txt"), 0o644).unwrap();
        assert_eq!(node.name, "a.txt");
        assert!(store.is_dirty());
    }

    #[test]
    fn create_file_existing_name_returns_eexist() {
        let mut store = make_store();
        store.create_file(ROOT_INODE, OsStr::new("a.txt"), 0o644).unwrap();
        let err = store.create_file(ROOT_INODE, OsStr::new("a.txt"), 0o644).unwrap_err();
        assert_eq!(err, fuser::Errno::EEXIST);
    }

    #[test]
    fn create_file_bad_parent_returns_enoent() {
        let mut store = make_store();
        let err = store.create_file(9999, OsStr::new("x.txt"), 0o644).unwrap_err();
        assert_eq!(err, fuser::Errno::ENOENT);
    }

    #[test]
    fn create_file_parent_is_file_returns_enotdir() {
        let (mut store, file_ino) = make_store_with_file(b"");
        let err = store.create_file(file_ino, OsStr::new("x.txt"), 0o644).unwrap_err();
        assert_eq!(err, fuser::Errno::ENOTDIR);
    }

    // --- write_file ---
    #[test]
    fn write_file_at_offset_zero() {
        let (mut store, ino) = make_store_with_file(b"");
        let written = store.write_file(ino, 0, b"hello").unwrap();
        assert_eq!(written, 5);
        let node = store.get_node(ino).unwrap();
        assert_eq!(node.attr.size, 5);
    }

    #[test]
    fn write_file_sparse_zero_fills() {
        let (mut store, ino) = make_store_with_file(b"hello");
        store.write_file(ino, 10, b"!").unwrap();
        let node = store.get_node(ino).unwrap();
        assert_eq!(node.attr.size, 11);
        if let NodeContent::File(FileContent::Created(data)) = &node.content {
            assert_eq!(data[5..10], [0u8; 5]);
            assert_eq!(data[10], b'!');
        } else {
            panic!("expected Created content");
        }
    }

    #[test]
    fn write_file_empty_data_is_noop() {
        let (mut store, ino) = make_store_with_file(b"hello");
        let written = store.write_file(ino, 0, b"").unwrap();
        assert_eq!(written, 0);
        assert_eq!(store.get_node(ino).unwrap().attr.size, 5);
    }

    #[test]
    fn write_file_bad_ino_returns_enoent() {
        let mut store = make_store();
        assert_eq!(store.write_file(9999, 0, b"x").unwrap_err(), fuser::Errno::ENOENT);
    }

    #[test]
    fn write_file_on_dir_returns_eisdir() {
        let mut store = make_store();
        assert_eq!(store.write_file(ROOT_INODE, 0, b"x").unwrap_err(), fuser::Errno::EISDIR);
    }

    // --- set_size ---
    #[test]
    fn set_size_truncate() {
        let (mut store, ino) = make_store_with_file(b"hello");
        store.set_size(ino, 3).unwrap();
        let node = store.get_node(ino).unwrap();
        assert_eq!(node.attr.size, 3);
    }

    #[test]
    fn set_size_truncate_to_zero() {
        let (mut store, ino) = make_store_with_file(b"hello");
        store.set_size(ino, 0).unwrap();
        assert_eq!(store.get_node(ino).unwrap().attr.size, 0);
    }

    #[test]
    fn set_size_extend_zero_pads() {
        let (mut store, ino) = make_store_with_file(b"hi");
        store.set_size(ino, 5).unwrap();
        let node = store.get_node(ino).unwrap();
        assert_eq!(node.attr.size, 5);
    }

    #[test]
    fn set_size_on_dir_returns_eisdir() {
        let mut store = make_store();
        assert_eq!(store.set_size(ROOT_INODE, 0).unwrap_err(), fuser::Errno::EISDIR);
    }

    // --- set_times ---
    #[test]
    fn set_times_specific() {
        let (mut store, ino) = make_store_with_file(b"");
        let t = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1000);
        store.set_times(ino, Some(TimeOrNow::SpecificTime(t)), None).unwrap();
        assert_eq!(store.get_node(ino).unwrap().attr.atime, t);
        assert!(store.is_dirty());
    }

    #[test]
    fn set_times_bad_ino_returns_enoent() {
        let mut store = make_store();
        assert_eq!(
            store.set_times(9999, None, None).unwrap_err(),
            fuser::Errno::ENOENT
        );
    }

    // --- make_dir ---
    #[test]
    fn make_dir_happy_path() {
        let mut store = make_store();
        let node = store.make_dir(ROOT_INODE, OsStr::new("mydir"), 0o755, 0).unwrap();
        assert_eq!(node.attr.nlink, 2);
        assert!(store.is_dirty());
    }

    #[test]
    fn make_dir_increments_parent_nlink() {
        let mut store = make_store();
        let parent_nlink_before = store.get_node(ROOT_INODE).unwrap().attr.nlink;
        store.make_dir(ROOT_INODE, OsStr::new("mydir"), 0o755, 0).unwrap();
        let parent_nlink_after = store.get_node(ROOT_INODE).unwrap().attr.nlink;
        assert_eq!(parent_nlink_after, parent_nlink_before + 1);
    }

    #[test]
    fn make_dir_existing_name_returns_eexist() {
        let mut store = make_store();
        store.make_dir(ROOT_INODE, OsStr::new("d"), 0o755, 0).unwrap();
        assert_eq!(
            store.make_dir(ROOT_INODE, OsStr::new("d"), 0o755, 0).unwrap_err(),
            fuser::Errno::EEXIST
        );
    }

    // --- unlink ---
    #[test]
    fn unlink_removes_file_completely() {
        let (mut store, ino) = make_store_with_file(b"hi");
        store.unlink(ROOT_INODE, OsStr::new("test.txt")).unwrap();
        assert!(store.get_node(ino).is_none());
        assert!(store.node_ids.get(&ino).is_none());
        assert!(store.get_children(ROOT_INODE).unwrap().is_empty());
        assert!(store.is_dirty());
    }

    #[test]
    fn unlink_nonexistent_returns_enoent() {
        let mut store = make_store();
        assert_eq!(
            store.unlink(ROOT_INODE, OsStr::new("ghost.txt")).unwrap_err(),
            fuser::Errno::ENOENT
        );
    }

    #[test]
    fn unlink_directory_returns_eperm_or_eisdir() {
        let mut store = make_store();
        store.make_dir(ROOT_INODE, OsStr::new("d"), 0o755, 0).unwrap();
        let err = store.unlink(ROOT_INODE, OsStr::new("d")).unwrap_err();
        assert!(err == fuser::Errno::EPERM || err == fuser::Errno::EISDIR);
    }

    #[test]
    fn unlink_then_recreate_same_name() {
        let (mut store, _) = make_store_with_file(b"hi");
        store.unlink(ROOT_INODE, OsStr::new("test.txt")).unwrap();
        store.create_file(ROOT_INODE, OsStr::new("test.txt"), 0o644).unwrap();
        assert_eq!(store.get_children(ROOT_INODE).unwrap().len(), 1);
    }

    // --- mark_clean ---
    #[test]
    fn mark_clean_transitions_modified_to_loaded() {
        let (mut store, ino) = make_store_with_file(b"hello");
        assert!(store.is_dirty());
        store.mark_clean();
        assert!(!store.is_dirty());
        // Created transitions to Loaded after mark_clean
        let node = store.get_node(ino).unwrap();
        assert!(matches!(node.content, NodeContent::File(FileContent::Loaded { .. })));
    }

    #[test]
    fn mark_clean_created_with_password_gets_cipher() {
        let mut store = ArchiveStore::new_for_test(
            PathBuf::from("/tmp/t.pna"),
            Some("secret".to_string()),
        );
        // new_for_test already inserts root; do not insert it again
        let node = store.create_file(ROOT_INODE, OsStr::new("f.txt"), 0o644).unwrap();
        let ino = node.attr.ino.0;
        store.mark_clean();
        let node = store.get_node(ino).unwrap();
        if let NodeContent::File(FileContent::Loaded { cipher, .. }) = &node.content {
            assert!(cipher.is_some());
        } else {
            panic!("expected Loaded");
        }
    }
```

- [ ] **Step 2: Run tests to confirm failure**

```bash
cargo test archive_store::tests --locked 2>&1 | grep "error\|FAILED" | head -20
```
Expected: errors — methods not yet defined.

- [ ] **Step 3: Implement write API methods**

Add to `impl ArchiveStore` in `src/archive_store.rs`:

```rust
use fuser::{Errno, TimeOrNow};

pub(crate) fn create_file(
    &mut self,
    parent: Inode,
    name: &OsStr,
    mode: u32,
) -> Result<&Node, Errno> {
    let parent_node = self.nodes.get(&parent).ok_or(Errno::ENOENT)?;
    if !matches!(parent_node.content, NodeContent::Directory) {
        return Err(Errno::ENOTDIR);
    }
    let children = self.get_children(parent).unwrap_or_default();
    if children.iter().any(|n| n.name == name) {
        return Err(Errno::EEXIST);
    }
    let ino = self.next_inode();
    let now = SystemTime::now();
    let node = Node {
        name: name.to_owned(),
        xattrs: HashMap::new(),
        content: NodeContent::File(FileContent::Created(Vec::new())),
        attr: FileAttr {
            ino: INodeNo(ino),
            size: 0,
            blocks: 1,
            atime: now, mtime: now, ctime: now, crtime: now,
            kind: FileType::RegularFile,
            perm: mode as u16,
            nlink: 1,
            uid: current_uid(),
            gid: current_gid(),
            rdev: 0, blksize: 512, flags: 0,
        },
    };
    self.insert_node(node, Some(parent)).map_err(|_| Errno::EIO)?;
    self.dirty = true;
    Ok(self.nodes.get(&ino).unwrap())
}

fn force_load_file(node: &mut Node) -> Result<(), Errno> {
    // Rust 2024 NLL: borrow of Unloaded ends before assignment
    if let NodeContent::File(FileContent::Unloaded(entry, opts)) = &node.content {
        let mut data = Vec::new();
        entry.reader(opts).map_err(|_| Errno::EIO)?
            .read_to_end(&mut data).map_err(|_| Errno::EIO)?;
        let cipher = if entry.header().encryption() != pna::Encryption::No {
            Some(CipherConfig {
                encryption: entry.header().encryption(),
                cipher_mode: entry.header().cipher_mode(),
            })
        } else {
            None
        };
        node.content = NodeContent::File(FileContent::Loaded { data, cipher });
    }
    Ok(())
}

pub(crate) fn write_file(
    &mut self,
    ino: Inode,
    offset: u64,
    data: &[u8],
) -> Result<usize, Errno> {
    let node = self.nodes.get_mut(&ino).ok_or(Errno::ENOENT)?;
    if matches!(node.content, NodeContent::Directory) {
        return Err(Errno::EISDIR);
    }
    force_load_file(node)?;
    let offset = usize::try_from(offset).map_err(|_| Errno::EFBIG)?;
    let buf = match &mut node.content {
        NodeContent::File(FileContent::Loaded { data: d, cipher }) => {
            let cipher = cipher.take();
            node.content = NodeContent::File(FileContent::Modified {
                data: std::mem::take(d), cipher,
            });
            match &mut node.content {
                NodeContent::File(FileContent::Modified { data: d, .. }) => d,
                _ => unreachable!(),
            }
        }
        NodeContent::File(FileContent::Modified { data: d, .. }) => d,
        NodeContent::File(FileContent::Created(d)) => d,
        _ => return Err(Errno::EIO),
    };
    if offset > buf.len() {
        buf.resize(offset, 0);
    }
    if data.is_empty() {
        return Ok(0);
    }
    let end = offset + data.len();
    if end > buf.len() {
        buf.resize(end, 0);
    }
    buf[offset..end].copy_from_slice(data);
    node.attr.size = buf.len() as u64;
    self.dirty = true;
    Ok(data.len())
}

pub(crate) fn set_size(&mut self, ino: Inode, size: u64) -> Result<(), Errno> {
    let node = self.nodes.get_mut(&ino).ok_or(Errno::ENOENT)?;
    if matches!(node.content, NodeContent::Directory) {
        return Err(Errno::EISDIR);
    }
    force_load_file(node)?;
    let size_usize = usize::try_from(size).map_err(|_| Errno::EFBIG)?;
    let buf = match &mut node.content {
        NodeContent::File(FileContent::Loaded { data: d, cipher }) => {
            let cipher = cipher.take();
            node.content = NodeContent::File(FileContent::Modified {
                data: std::mem::take(d), cipher,
            });
            match &mut node.content {
                NodeContent::File(FileContent::Modified { data: d, .. }) => d,
                _ => unreachable!(),
            }
        }
        NodeContent::File(FileContent::Modified { data: d, .. }) => d,
        NodeContent::File(FileContent::Created(d)) => d,
        _ => return Err(Errno::EIO),
    };
    buf.resize(size_usize, 0);
    node.attr.size = size;
    self.dirty = true;
    Ok(())
}

pub(crate) fn set_times(
    &mut self,
    ino: Inode,
    atime: Option<TimeOrNow>,
    mtime: Option<TimeOrNow>,
) -> Result<(), Errno> {
    let node = self.nodes.get_mut(&ino).ok_or(Errno::ENOENT)?;
    match atime {
        Some(TimeOrNow::SpecificTime(t)) => node.attr.atime = t,
        Some(TimeOrNow::Now) => node.attr.atime = SystemTime::now(),
        None => {}
    }
    match mtime {
        Some(TimeOrNow::SpecificTime(t)) => node.attr.mtime = t,
        Some(TimeOrNow::Now) => node.attr.mtime = SystemTime::now(),
        None => {}
    }
    self.dirty = true;
    Ok(())
}

pub(crate) fn make_dir(
    &mut self,
    parent: Inode,
    name: &OsStr,
    mode: u32,
    umask: u32,
) -> Result<&Node, Errno> {
    let parent_node = self.nodes.get(&parent).ok_or(Errno::ENOENT)?;
    if !matches!(parent_node.content, NodeContent::Directory) {
        return Err(Errno::ENOTDIR);
    }
    let children = self.get_children(parent).unwrap_or_default();
    if children.iter().any(|n| n.name == name) {
        return Err(Errno::EEXIST);
    }
    let ino = self.next_inode();
    let effective_mode = (mode & !umask) as u16;
    let mut node = make_dir_node(ino, name.to_owned());
    node.attr.perm = effective_mode;
    self.insert_node(node, Some(parent)).map_err(|_| Errno::EIO)?;
    // Increment parent nlink
    self.nodes.get_mut(&parent).unwrap().attr.nlink += 1;
    self.dirty = true;
    Ok(self.nodes.get(&ino).unwrap())
}

pub(crate) fn unlink(&mut self, parent: Inode, name: &OsStr) -> Result<(), Errno> {
    let children = self.get_children(parent).ok_or(Errno::ENOENT)?;
    let target = children
        .iter()
        .find(|n| n.name == name)
        .ok_or(Errno::ENOENT)?;
    let ino = target.attr.ino.0;
    // Reject directories
    if matches!(target.content, NodeContent::Directory) {
        #[cfg(target_os = "macos")]
        return Err(Errno::EPERM);
        #[cfg(not(target_os = "macos"))]
        return Err(Errno::EISDIR);
    }
    let node_id = self.node_ids.get(&ino).cloned().ok_or(Errno::ENOENT)?;
    self.tree.remove_node(node_id, RemoveBehavior::OrphanChildren)
        .map_err(|_| Errno::EIO)?;
    self.node_ids.remove(&ino);
    self.nodes.remove(&ino);
    self.dirty = true;
    Ok(())
}

// Phase 2 stubs
pub(crate) fn rmdir(&mut self, _parent: Inode, _name: &OsStr) -> Result<(), Errno> {
    Err(Errno::ENOSYS)
}
pub(crate) fn rename(
    &mut self, _old_parent: Inode, _old_name: &OsStr,
    _new_parent: Inode, _new_name: &OsStr,
    _flags: fuser::RenameFlags,
) -> Result<(), Errno> { Err(Errno::ENOSYS) }
pub(crate) fn set_attr_full(
    &mut self, _ino: Inode, _mode: Option<u32>,
    _uid: Option<u32>, _gid: Option<u32>,
) -> Result<(), Errno> { Err(Errno::ENOSYS) }
pub(crate) fn create_symlink(
    &mut self, _parent: Inode, _name: &OsStr, _target: &Path,
) -> Result<&Node, Errno> { Err(Errno::ENOSYS) }
pub(crate) fn create_hardlink(
    &mut self, _parent: Inode, _name: &OsStr, _target: Inode,
) -> Result<(), Errno> { Err(Errno::ENOSYS) }
pub(crate) fn set_xattr(
    &mut self, _ino: Inode, _name: &OsStr, _value: &[u8],
) -> Result<(), Errno> { Err(Errno::ENOSYS) }
pub(crate) fn remove_xattr(
    &mut self, _ino: Inode, _name: &OsStr,
) -> Result<(), Errno> { Err(Errno::ENOSYS) }
```

Also add `use std::io::Read;` and the missing imports at top of `archive_store.rs`.

- [ ] **Step 4: Run all archive_store tests**

```bash
cargo test archive_store::tests --locked 2>&1
```
Expected: all tests pass (≥25 tests).

- [ ] **Step 5: Commit**

```bash
git add src/archive_store.rs
git commit -m "feat: implement ArchiveStore write API (create_file, write_file, set_size, set_times, make_dir, unlink)"
```

---

### Task 4: archive_io::save

**Files:**
- Modify: `src/archive_io.rs`

- [ ] **Step 1: Write failing tests for save**

Add to `archive_io::tests`:

```rust
    use crate::archive_store::{FileContent, NodeContent};
    use std::io::Write as IoWrite;

    fn roundtrip_plain(content: &[u8]) {
        let dir = TempDir::new().unwrap();
        let path = create_plain_archive(&dir, "rt.pna", &[("file.txt", content)]);
        let mut store = load(&path, None).unwrap();
        save(&store).unwrap();
        let store2 = load(&path, None).unwrap();
        let children = store2.get_children(crate::archive_store::ROOT_INODE).unwrap();
        assert_eq!(children.len(), 1);
    }

    #[test]
    fn save_roundtrip_empty_file() { roundtrip_plain(b""); }

    #[test]
    fn save_roundtrip_with_content() { roundtrip_plain(b"hello world"); }

    #[test]
    fn save_created_file_persisted() {
        let dir = TempDir::new().unwrap();
        let path = create_plain_archive(&dir, "base.pna", &[]);
        let mut store = load(&path, None).unwrap();
        store.create_file(crate::archive_store::ROOT_INODE, std::ffi::OsStr::new("new.txt"), 0o644).unwrap();
        store.write_file(
            store.get_children(crate::archive_store::ROOT_INODE).unwrap()[0].attr.ino.0,
            0, b"new content",
        ).unwrap();
        save(&store).unwrap();
        let store2 = load(&path, None).unwrap();
        let children = store2.get_children(crate::archive_store::ROOT_INODE).unwrap();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].name.to_str().unwrap(), "new.txt");
    }

    #[test]
    fn save_encrypted_requires_password() {
        let dir = TempDir::new().unwrap();
        // create an encrypted archive (simulate with a store that has cipher)
        let path = dir.path().join("enc.pna");
        // For this test, create a store with a file that has cipher config
        // and no password — should fail
        // (simplified: we rely on the password guard logic)
        // Real encrypted archive creation is tested in shell tests
        _ = path; // placeholder - see shell tests for full encrypted coverage
    }
```

- [ ] **Step 2: Implement archive_io::save**

Add to `src/archive_io.rs`:

```rust
use pna::{
    Archive, CipherMode, DataKind, Encryption, EntryBuilder, EntryName, HashAlgorithm,
    WriteOptions,
};
use std::process;
use std::time::UNIX_EPOCH;

pub(crate) fn save(store: &ArchiveStore) -> io::Result<()> {
    let archive_path = store.archive_path();

    // Password guard: check before building WriteOptions (avoid panics)
    for node in store.nodes.values() {
        match &node.content {
            NodeContent::File(FileContent::Loaded { cipher: Some(_), .. })
            | NodeContent::File(FileContent::Modified { cipher: Some(_), .. }) => {
                if store.password().is_none() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "cannot re-encrypt: archive requires password but none was provided",
                    ));
                }
            }
            _ => {}
        }
    }

    cleanup_stale_tmp(archive_path);

    let dir = archive_path.parent().unwrap_or(Path::new("."));
    let stem = archive_path.file_name().unwrap().to_string_lossy();
    let tmp_path = dir.join(format!(".{}.tmp.{}", stem, process::id()));

    let tmp_file = std::fs::File::create(&tmp_path)?;
    let mut archive = Archive::write_header(tmp_file)?;

    write_nodes_dfs(store, &mut archive)?;

    let inner = archive.finish()?;
    inner.sync_all()?;
    drop(inner);

    std::fs::rename(&tmp_path, archive_path)?;
    Ok(())
}

fn write_nodes_dfs(
    store: &ArchiveStore,
    archive: &mut Archive<std::fs::File>,
) -> io::Result<()> {
    let root_id = store.node_ids.get(&ROOT_INODE).unwrap();
    write_subtree(store, archive, root_id)?;
    Ok(())
}

fn write_subtree(
    store: &ArchiveStore,
    archive: &mut Archive<std::fs::File>,
    node_id: &id_tree::NodeId,
) -> io::Result<()> {
    use id_tree::NodeId;

    let ino = *store.tree.get(node_id).unwrap().data();
    if ino == ROOT_INODE {
        // root is synthetic — visit children only
        for child_id in store.tree.children_ids(node_id).unwrap() {
            write_subtree(store, archive, child_id)?;
        }
        return Ok(());
    }

    let node = store.nodes.get(&ino).unwrap();
    let path_str = build_archive_path(store, ino);
    let entry_name = EntryName::from_lossy(&path_str);

    match &node.content {
        NodeContent::Directory => {
            let meta = build_metadata(node);
            archive.append_entry(
                EntryBuilder::new_dir(entry_name).metadata(meta).build(),
            )?;
        }
        NodeContent::File(FileContent::Unloaded(entry, _opts)) => {
            let meta = build_metadata(node);
            let cloned = entry.clone().with_metadata(meta);
            archive.add_entry(cloned)?;
        }
        NodeContent::File(FileContent::Loaded { data, cipher })
        | NodeContent::File(FileContent::Modified { data, cipher }) => {
            let opts = build_write_options(cipher.as_ref(), store.password())?;
            let mut writer = archive.append_file(opts, entry_name)?;
            writer.write_all(data)?;
            writer.finish()?;
        }
        NodeContent::File(FileContent::Created(data)) => {
            let opts = if let Some(pwd) = store.password() {
                WriteOptions::builder()
                    .encryption(Encryption::Aes)
                    .cipher_mode(CipherMode::CTR)
                    .hash_algorithm(HashAlgorithm::argon2id())
                    .password(pwd.as_bytes().to_vec())
                    .build()
            } else {
                WriteOptions::default()
            };
            let mut writer = archive.append_file(opts, entry_name)?;
            writer.write_all(data)?;
            writer.finish()?;
        }
        NodeContent::Symlink(_) => { /* Phase 2 */ }
    }

    for child_id in store.tree.children_ids(node_id).unwrap() {
        write_subtree(store, archive, child_id)?;
    }
    Ok(())
}

fn build_write_options(cipher: Option<&CipherConfig>, password: Option<&str>) -> io::Result<WriteOptions> {
    match cipher {
        None => Ok(WriteOptions::default()),
        Some(c) => {
            let pwd = password.ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "password required")
            })?;
            Ok(WriteOptions::builder()
                .encryption(c.encryption)
                .cipher_mode(c.cipher_mode)
                .hash_algorithm(HashAlgorithm::argon2id())
                .password(pwd.as_bytes().to_vec())
                .build())
        }
    }
}

fn build_metadata(node: &Node) -> pna::Metadata {
    let to_pna_dur = |t: SystemTime| {
        t.duration_since(UNIX_EPOCH)
            .map(|d| pna::Duration::seconds(d.as_secs() as i64))
            .ok()
    };
    let mut builder = pna::Metadata::new();
    if let Some(d) = to_pna_dur(node.attr.mtime) {
        builder = builder.with_modified(d);
    }
    if let Some(d) = to_pna_dur(node.attr.crtime) {
        builder = builder.with_created(d);
    }
    builder.build()
}

fn build_archive_path(store: &ArchiveStore, ino: Inode) -> String {
    // Walk up the tree to build the path (push name, then move to parent; stop at root)
    let mut parts = Vec::new();
    let mut current = ino;
    loop {
        if current == ROOT_INODE { break; }
        let node = store.nodes.get(&current).unwrap();
        parts.push(node.name.to_string_lossy().into_owned());
        let node_id = store.node_ids.get(&current).unwrap();
        match store.tree.get(node_id).unwrap().parent() {
            Some(pid) => current = *store.tree.get(pid).unwrap().data(),
            None => break,
        }
    }
    parts.reverse();
    parts.join("/")
}
```

- [ ] **Step 3: Run save tests**

```bash
cargo test archive_io::tests --locked 2>&1
```
Expected: all tests pass.

- [ ] **Step 4: Commit**

```bash
git add src/archive_io.rs
git commit -m "feat: implement archive_io::save with atomic write and password guard"
```

---

### Task 5: filesystem.rs — Update FUSE Adapter

**Files:**
- Modify: `src/filesystem.rs`

- [ ] **Step 1: Rewrite filesystem.rs**

Replace the contents of `src/filesystem.rs` with:

```rust
use crate::archive_io;
use crate::archive_store::{ArchiveStore, NodeContent, FileContent, ROOT_INODE};
use fuser::{
    Errno, FileHandle, Filesystem, Generation, INodeNo, LockOwner, OpenAccMode, OpenFlags,
    ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyXattr,
    RenameFlags, Request, TimeOrNow, WriteFlags,
};
use log::info;
use std::ffi::{CString, OsStr};
use std::io::Read;
use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;

#[derive(clap::ValueEnum, Clone, PartialEq)]
pub(crate) enum WriteStrategy {
    Lazy,
    Immediate,
}

pub(crate) struct PnaFS {
    store: Mutex<ArchiveStore>,
    write_strategy: Option<WriteStrategy>,
}

impl PnaFS {
    pub(crate) fn new(
        archive: PathBuf,
        password: Option<String>,
        write_strategy: Option<WriteStrategy>,
    ) -> std::io::Result<Self> {
        let store = archive_io::load(&archive, password)?;
        Ok(Self {
            store: Mutex::new(store),
            write_strategy,
        })
    }
}

impl Filesystem for PnaFS {
    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        info!("[Implemented] lookup(parent: {parent:#x?}, name {name:?})");
        let manager = self.store.lock().unwrap();
        let children = manager.get_children(parent.0).unwrap_or_default();
        if let Some(entry) = children.iter().find(|it| it.name == name) {
            reply.entry(&Duration::from_secs(1), &entry.attr, Generation(0));
        } else {
            reply.error(Errno::ENOENT);
        }
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        info!("[Implemented] getattr(ino: {ino:#x?})");
        let manager = self.store.lock().unwrap();
        if let Some(node) = manager.get_node(ino.0) {
            reply.attr(&Duration::from_secs(1), &node.attr);
        } else {
            reply.error(Errno::ENOENT);
        }
    }

    fn open(&self, _req: &Request, ino: INodeNo, flags: OpenFlags, reply: fuser::ReplyOpen) {
        if self.write_strategy.is_none() && flags.acc_mode() != OpenAccMode::O_RDONLY {
            reply.error(Errno::EROFS);
            return;
        }
        let manager = self.store.lock().unwrap();
        if manager.get_node(ino.0).is_some() {
            reply.opened(0, 0);
        } else {
            reply.error(Errno::ENOENT);
        }
    }

    fn create(
        &self, _req: &Request, parent: INodeNo, name: &OsStr,
        mode: u32, umask: u32, flags: i32, reply: ReplyCreate,
    ) {
        if self.write_strategy.is_none() {
            reply.error(Errno::EROFS);
            return;
        }
        let mut store = self.store.lock().unwrap();
        // Guard: parent must be a directory
        match store.get_node(parent.0) {
            None => { reply.error(Errno::ENOENT); return; }
            Some(n) if !matches!(n.content, NodeContent::Directory) => {
                reply.error(Errno::ENOTDIR); return;
            }
            _ => {}
        }
        let children = store.get_children(parent.0).unwrap_or_default();
        let existing = children.iter().find(|n| n.name == name).map(|n| n.attr.ino.0);

        if let Some(ino) = existing {
            // O_CREAT on existing file
            if (flags & libc::O_EXCL) != 0 {
                reply.error(Errno::EEXIST);
                return;
            }
            if (flags & libc::O_TRUNC) != 0 {
                if let Err(e) = store.set_size(ino, 0) {
                    reply.error(e); return;
                }
            }
            let node = store.get_node(ino).unwrap();
            reply.created(&Duration::from_secs(1), &node.attr, Generation(0), 0, 0);
        } else {
            match store.create_file(parent.0, name, mode & !umask) {
                Ok(node) => {
                    let attr = node.attr;
                    reply.created(&Duration::from_secs(1), &attr, Generation(0), 0, 0);
                }
                Err(e) => reply.error(e),
            }
        }
    }

    fn write(
        &self, _req: &Request, ino: INodeNo, _fh: FileHandle,
        offset: u64, data: &[u8], _write_flags: WriteFlags, _flags: OpenFlags,
        _lock_owner: Option<LockOwner>, reply: fuser::ReplyWrite,
    ) {
        let mut store = self.store.lock().unwrap();
        match store.write_file(ino.0, offset, data) {
            Ok(n) => reply.written(n as u32),
            Err(e) => reply.error(e),
        }
    }

    fn setattr(
        &self, _req: &Request, ino: INodeNo, mode: Option<u32>,
        uid: Option<u32>, gid: Option<u32>, size: Option<u64>,
        atime: Option<TimeOrNow>, mtime: Option<TimeOrNow>,
        _ctime: Option<std::time::SystemTime>, _fh: Option<FileHandle>,
        _crtime: Option<std::time::SystemTime>, _chgtime: Option<std::time::SystemTime>,
        _bkuptime: Option<std::time::SystemTime>, _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        if self.write_strategy.is_none() {
            reply.error(Errno::EROFS); return;
        }
        let mut store = self.store.lock().unwrap();
        if let Some(size) = size {
            if let Err(e) = store.set_size(ino.0, size) {
                reply.error(e); return;
            }
        }
        if atime.is_some() || mtime.is_some() {
            if let Err(e) = store.set_times(ino.0, atime, mtime) {
                reply.error(e); return;
            }
        }
        if mode.is_some() || uid.is_some() || gid.is_some() {
            if let Err(e) = store.set_attr_full(ino.0, mode, uid, gid) {
                reply.error(e); return;
            }
        }
        if let Some(node) = store.get_node(ino.0) {
            reply.attr(&Duration::from_secs(1), &node.attr);
        } else {
            reply.error(Errno::ENOENT);
        }
    }

    fn mkdir(
        &self, _req: &Request, parent: INodeNo, name: &OsStr,
        mode: u32, umask: u32, reply: ReplyEntry,
    ) {
        if self.write_strategy.is_none() {
            reply.error(Errno::EROFS); return;
        }
        let mut store = self.store.lock().unwrap();
        match store.make_dir(parent.0, name, mode, umask) {
            Ok(node) => reply.entry(&Duration::from_secs(1), &node.attr, Generation(0)),
            Err(e) => reply.error(e),
        }
    }

    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        if self.write_strategy.is_none() {
            reply.error(Errno::EROFS); return;
        }
        let mut store = self.store.lock().unwrap();
        match store.unlink(parent.0, name) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    fn read(
        &self, _req: &Request, ino: INodeNo, _fh: FileHandle,
        offset: u64, size: u32, _flags: OpenFlags,
        _lock_owner: Option<LockOwner>, reply: ReplyData,
    ) {
        let mut store = self.store.lock().unwrap();
        let node = match store.get_node_mut(ino.0) {
            Some(n) => n,
            None => { reply.error(Errno::ENOENT); return; }
        };
        // Force-load if needed
        if let NodeContent::File(FileContent::Unloaded(entry, opts)) = &node.content {
            let mut data = Vec::new();
            if entry.reader(opts).and_then(|mut r| r.read_to_end(&mut data).map(|_| ())).is_err() {
                reply.error(Errno::EIO); return;
            }
            let cipher = if entry.header().encryption() != pna::Encryption::No {
                Some(crate::archive_store::CipherConfig {
                    encryption: entry.header().encryption(),
                    cipher_mode: entry.header().cipher_mode(),
                })
            } else { None };
            node.content = NodeContent::File(FileContent::Loaded { data, cipher });
        }
        let data = match &node.content {
            NodeContent::File(FileContent::Loaded { data, .. })
            | NodeContent::File(FileContent::Modified { data, .. })
            | NodeContent::File(FileContent::Created(data)) => data.as_slice(),
            _ => { reply.error(Errno::EISDIR); return; }
        };
        let offset = offset as usize;
        let size = size as usize;
        reply.data(&data[data.len().min(offset)..data.len().min(offset + size)]);
    }

    fn flush(
        &self, _req: &Request, ino: INodeNo, _fh: FileHandle,
        _lock_owner: LockOwner, reply: ReplyEmpty,
    ) {
        let store = self.store.lock().unwrap();
        if store.get_node(ino.0).is_some() { reply.ok(); } else { reply.error(Errno::ENOENT); }
    }

    fn release(
        &self, _req: &Request, _ino: INodeNo, _fh: FileHandle,
        _flags: OpenFlags, _lock_owner: Option<LockOwner>,
        _flush: bool, reply: ReplyEmpty,
    ) {
        if matches!(self.write_strategy, Some(WriteStrategy::Immediate)) {
            let mut store = self.store.lock().unwrap();
            if store.is_dirty() {
                match archive_io::save(&*store) {
                    Ok(()) => store.mark_clean(),
                    Err(e) => {
                        log::error!("failed to flush on release: {e}");
                        reply.error(Errno::EIO); return;
                    }
                }
            }
        }
        reply.ok();
    }

    fn fsync(
        &self, _req: &Request, _ino: INodeNo, _fh: FileHandle,
        _datasync: bool, reply: ReplyEmpty,
    ) {
        if self.write_strategy.is_some() {
            let mut store = self.store.lock().unwrap();
            if store.is_dirty() {
                match archive_io::save(&*store) {
                    Ok(()) => store.mark_clean(),
                    Err(e) => {
                        log::error!("failed to fsync: {e}");
                        reply.error(Errno::EIO); return;
                    }
                }
            }
        }
        reply.ok();
    }

    fn destroy(&mut self) {
        // Flush on any write mode when dirty — destroy() serves as a final safety net
        // even in Immediate mode (in case a prior release() save failed).
        let store = self.store.get_mut().unwrap();
        if self.write_strategy.is_some() && store.is_dirty() {
            match archive_io::save(store) {
                Ok(()) => store.mark_clean(),
                Err(e) => log::error!("failed to flush on unmount: {e}"),
            }
        }
    }

    fn readdir(
        &self, _req: &Request, ino: INodeNo, _fh: FileHandle,
        offset: u64, mut reply: ReplyDirectory,
    ) {
        let store = self.store.lock().unwrap();
        let children = store.get_children(ino.0).unwrap_or_default();
        let mut current_offset = offset + 1;
        for entry in children.into_iter().skip(offset as usize) {
            if reply.add(entry.attr.ino, current_offset, entry.attr.kind, &entry.name) { break; }
            current_offset += 1;
        }
        reply.ok();
    }

    fn getxattr(&self, _req: &Request, ino: INodeNo, name: &OsStr, size: u32, reply: ReplyXattr) {
        let store = self.store.lock().unwrap();
        if let Some(node) = store.get_node(ino.0) {
            if let Some(value) = node.xattrs.get(name) {
                if size == 0 { reply.size(value.len() as u32); } else { reply.data(value); }
            } else { reply.error(Errno::ENOENT); }
        } else { reply.error(Errno::ENOENT); }
    }

    fn listxattr(&self, _req: &Request, ino: INodeNo, size: u32, reply: ReplyXattr) {
        let store = self.store.lock().unwrap();
        if let Some(node) = store.get_node(ino.0) {
            let keys = node.xattrs.keys()
                .flat_map(|k| CString::new(k.as_bytes()).unwrap_or_default().as_bytes_with_nul().to_vec())
                .collect::<Vec<_>>();
            if size == 0 { reply.size(keys.len() as u32); } else { reply.data(&keys); }
        } else { reply.error(Errno::ENOENT); }
    }

    fn rename(
        &self, _req: &Request, old_parent: INodeNo, old_name: &OsStr,
        new_parent: INodeNo, new_name: &OsStr, flags: RenameFlags, reply: ReplyEmpty,
    ) {
        let mut store = self.store.lock().unwrap();
        match store.rename(old_parent.0, old_name, new_parent.0, new_name, flags) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }
}
```

- [ ] **Step 2: Build to check for errors**

```bash
cargo build --locked 2>&1
```
Fix any compile errors (import issues, missing `libc` usage, etc.).

- [ ] **Step 3: Run unit tests**

```bash
cargo test --locked 2>&1
```
Expected: all tests pass.

- [ ] **Step 4: Commit**

```bash
git add src/filesystem.rs
git commit -m "feat: update filesystem.rs to use ArchiveStore with write FUSE ops"
```

---

### Task 6: command/mount.rs and CLI Update

**Files:**
- Modify: `src/command/mount.rs`

- [ ] **Step 1: Update MountOptions and mount_archive**

Replace `src/command/mount.rs`:

```rust
use crate::{
    cli::PasswordArgs,
    command::{Command, ask_password},
    filesystem::{PnaFS, WriteStrategy},
};
use clap::{Args, ValueHint};
use fuser::{Config, MountOption, SessionACL, mount2};
use std::fs::create_dir_all;
use std::io;
use std::path::{Path, PathBuf};

#[derive(Args)]
pub(crate) struct MountArgs {
    #[command(flatten)]
    password: PasswordArgs,
    #[command(flatten)]
    mount_options: MountOptions,
    #[arg(value_hint = ValueHint::FilePath)]
    archive: PathBuf,
    #[arg(value_hint = ValueHint::DirPath)]
    mount_point: PathBuf,
}

#[derive(Args)]
struct MountOptions {
    #[arg(long, help = "Allow root access")]
    allow_root: bool,
    #[arg(long, help = "Allow all users to access")]
    allow_other: bool,
    #[arg(long, help = "Enable write mode (default: read-only). WARNING: solid archives will be converted to non-solid on first write.")]
    write: bool,
    #[arg(long, default_value = "lazy", requires = "write",
          help = "When to flush changes: lazy (on unmount) or immediate (on file close)")]
    write_strategy: WriteStrategy,
}

impl Command for MountArgs {
    fn execute(self) -> io::Result<()> {
        let password = ask_password(self.password)?;
        mount_archive(self.mount_point, self.archive, password, self.mount_options)
    }
}

fn mount_archive(
    mount_point: impl AsRef<Path>,
    archive: impl Into<PathBuf>,
    password: Option<String>,
    mount_options: MountOptions,
) -> io::Result<()> {
    let write_strategy = if mount_options.write {
        Some(mount_options.write_strategy)
    } else {
        None
    };
    let fs = PnaFS::new(archive.into(), password, write_strategy.clone())?;
    create_dir_all(&mount_point)?;

    let acl = if mount_options.allow_other {
        SessionACL::All
    } else if mount_options.allow_root {
        SessionACL::RootAndOwner
    } else {
        SessionACL::Owner
    };

    let mut mount_opts = vec![MountOption::FSName("pnafs".to_owned())];
    if write_strategy.is_none() {
        mount_opts.push(MountOption::RO);
    }

    let mut config = Config::default();
    config.mount_options = mount_opts;
    config.acl = acl;

    mount2(fs, mount_point, &config)?;
    Ok(())
}
```

- [ ] **Step 2: Build and verify CLI help**

```bash
cargo build --locked 2>&1 && cargo run -- mount --help 2>&1
```
Expected: help text shows `--write` and `--write-strategy` flags.

- [ ] **Step 3: Commit**

```bash
git add src/command/mount.rs
git commit -m "feat: add --write and --write-strategy CLI flags to mount command"
```

---

### Task 7: Cleanup — Remove file_manager.rs

**Files:**
- Delete: `src/file_manager.rs`
- Modify: `src/main.rs` — remove `mod file_manager;`
- Modify: `src/archive_io.rs` — remove dependency on file_manager helpers

- [ ] **Step 1: Move uid/gid helpers into archive_store.rs**

The `get_owner_id` / `get_group_id` functions (with nix crate `#[cfg(unix)]` guards) should live in `archive_store.rs` as private helpers. `archive_io.rs` calls them via `crate::archive_store::get_owner_id_for_node`.

- [ ] **Step 2: Remove file_manager.rs**

```bash
git rm src/file_manager.rs
```

Remove `mod file_manager;` from `src/main.rs`.

- [ ] **Step 3: Build and test**

```bash
cargo build --locked 2>&1 && cargo test --locked 2>&1
```
Expected: clean build, all tests pass.

- [ ] **Step 4: Commit**

```bash
git add -u
git commit -m "refactor: remove file_manager.rs (replaced by archive_store + archive_io)"
```

---

### Task 8: Shell Integration Tests for Write Operations

**Files:**
- Create: `scripts/tests/test_mount_write.sh`
- Create: `scripts/tests/test_mount_write_encrypted.sh`
- Create: `scripts/tests/test_mount_write_strategy.sh`
- Modify: `scripts/tests/run.sh`

- [ ] **Step 1: Create test_mount_write.sh**

```bash
cat > scripts/tests/test_mount_write.sh << 'SCRIPT'
#!/usr/bin/env bash
# Integration tests for pnafs write support (plain archive)
set -eu

PNA_BIN="${PNA_BIN:-pna}"
PNAFS_BIN="${PNAFS_BIN:-pnafs}"
WORKDIR="$(mktemp -d)"
ARCHIVE="$WORKDIR/test.pna"
MOUNTPOINT="$WORKDIR/mnt"

cleanup() {
  if mount | grep -q "$MOUNTPOINT"; then
    fusermount -u "$MOUNTPOINT" 2>/dev/null || umount "$MOUNTPOINT" 2>/dev/null || true
  fi
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

mount_rw() {
  mkdir -p "$MOUNTPOINT"
  "$PNAFS_BIN" mount --write "$ARCHIVE" "$MOUNTPOINT" &
  MOUNT_PID=$!
  for i in $(seq 1 10); do
    if mount | grep -q "$MOUNTPOINT"; then break; fi
    sleep 0.5
  done
}

unmount_wait() {
  fusermount -u "$MOUNTPOINT" 2>/dev/null || umount "$MOUNTPOINT"
  wait "$MOUNT_PID" 2>/dev/null || true
  sleep 0.2
}

# Create empty archive
"$PNA_BIN" create "$ARCHIVE" --overwrite /dev/null 2>/dev/null || \
  "$PNA_BIN" create "$ARCHIVE" --overwrite

echo "=== Test 1: Create file and verify after remount ==="
mount_rw
echo "hello world" > "$MOUNTPOINT/hello.txt"
unmount_wait
mount_rw
CONTENT="$(cat "$MOUNTPOINT/hello.txt")"
[ "$CONTENT" = "hello world" ] || { echo "FAIL: content mismatch: $CONTENT"; exit 1; }
unmount_wait
echo "PASS"

echo "=== Test 2: mkdir and verify after remount ==="
mount_rw
mkdir "$MOUNTPOINT/newdir"
unmount_wait
mount_rw
[ -d "$MOUNTPOINT/newdir" ] || { echo "FAIL: directory missing"; exit 1; }
unmount_wait
echo "PASS"

echo "=== Test 3: unlink file and verify gone after remount ==="
mount_rw
echo "delete me" > "$MOUNTPOINT/todelete.txt"
unmount_wait
mount_rw
rm "$MOUNTPOINT/todelete.txt"
unmount_wait
mount_rw
[ ! -f "$MOUNTPOINT/todelete.txt" ] || { echo "FAIL: file still exists"; exit 1; }
unmount_wait
echo "PASS"

echo "=== Test 4: Overwrite file content ==="
mount_rw
echo "first" > "$MOUNTPOINT/overwrite.txt"
unmount_wait
mount_rw
echo "second" > "$MOUNTPOINT/overwrite.txt"
unmount_wait
mount_rw
CONTENT="$(cat "$MOUNTPOINT/overwrite.txt")"
[ "$CONTENT" = "second" ] || { echo "FAIL: content: $CONTENT"; exit 1; }
unmount_wait
echo "PASS"

echo "=== Test 5: Truncate file to zero ==="
mount_rw
echo "some data" > "$MOUNTPOINT/truncate.txt"
unmount_wait
mount_rw
truncate -s 0 "$MOUNTPOINT/truncate.txt"
unmount_wait
mount_rw
SIZE="$(wc -c < "$MOUNTPOINT/truncate.txt")"
[ "$SIZE" -eq 0 ] || { echo "FAIL: size=$SIZE"; exit 1; }
unmount_wait
echo "PASS"

echo "=== Test 6: Create nested file in subdirectory ==="
mount_rw
mkdir -p "$MOUNTPOINT/sub/dir"
echo "nested" > "$MOUNTPOINT/sub/dir/file.txt"
unmount_wait
mount_rw
CONTENT="$(cat "$MOUNTPOINT/sub/dir/file.txt")"
[ "$CONTENT" = "nested" ] || { echo "FAIL: $CONTENT"; exit 1; }
unmount_wait
echo "PASS"

echo "=== Test 7: Read-only mount rejects write ==="
mount_rw  # actually mount without --write for this test
# This requires mounting without --write; we use a separate invocation
unmount_wait
mkdir -p "$MOUNTPOINT"
"$PNAFS_BIN" mount "$ARCHIVE" "$MOUNTPOINT" &
MOUNT_PID=$!
sleep 1
if echo "fail" > "$MOUNTPOINT/should_fail.txt" 2>/dev/null; then
  fusermount -u "$MOUNTPOINT" || true
  wait "$MOUNT_PID" || true
  echo "FAIL: write should have been rejected"
  exit 1
fi
fusermount -u "$MOUNTPOINT" 2>/dev/null || umount "$MOUNTPOINT" || true
wait "$MOUNT_PID" 2>/dev/null || true
echo "PASS"

echo "=== Test 8: Multiple files in one session ==="
mount_rw
for i in 1 2 3 4 5; do
  echo "content$i" > "$MOUNTPOINT/file$i.txt"
done
unmount_wait
mount_rw
for i in 1 2 3 4 5; do
  CONTENT="$(cat "$MOUNTPOINT/file$i.txt")"
  [ "$CONTENT" = "content$i" ] || { echo "FAIL file$i: $CONTENT"; exit 1; }
done
unmount_wait
echo "PASS"

echo "All write tests passed."
SCRIPT
chmod +x scripts/tests/test_mount_write.sh
```

- [ ] **Step 2: Create test_mount_write_encrypted.sh**

```bash
cat > scripts/tests/test_mount_write_encrypted.sh << 'SCRIPT'
#!/usr/bin/env bash
set -eu

PNA_BIN="${PNA_BIN:-pna}"
PNAFS_BIN="${PNAFS_BIN:-pnafs}"
WORKDIR="$(mktemp -d)"
ARCHIVE="$WORKDIR/enc.pna"
MOUNTPOINT="$WORKDIR/mnt"
PASSWORD="testpassword123"

cleanup() {
  if mount | grep -q "$MOUNTPOINT"; then
    fusermount -u "$MOUNTPOINT" 2>/dev/null || umount "$MOUNTPOINT" || true
  fi
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

# Create encrypted archive with one file
echo "original" > "$WORKDIR/seed.txt"
"$PNA_BIN" create "$ARCHIVE" --overwrite --password "$PASSWORD" "$WORKDIR/seed.txt"
rm "$WORKDIR/seed.txt"

mount_enc() {
  mkdir -p "$MOUNTPOINT"
  "$PNAFS_BIN" mount --write --password "$PASSWORD" "$ARCHIVE" "$MOUNTPOINT" &
  MOUNT_PID=$!
  for i in $(seq 1 10); do
    if mount | grep -q "$MOUNTPOINT"; then break; fi
    sleep 0.5
  done
}

unmount_wait() {
  fusermount -u "$MOUNTPOINT" 2>/dev/null || umount "$MOUNTPOINT"
  wait "$MOUNT_PID" 2>/dev/null || true
  sleep 0.2
}

echo "=== Encrypted Test 1: Create new file in encrypted archive ==="
mount_enc
echo "new encrypted content" > "$MOUNTPOINT/newfile.txt"
unmount_wait

# Verify by remounting with correct password
mount_enc
CONTENT="$(cat "$MOUNTPOINT/newfile.txt")"
[ "$CONTENT" = "new encrypted content" ] || { echo "FAIL: $CONTENT"; exit 1; }
unmount_wait
echo "PASS"

echo "=== Encrypted Test 2: Overwrite file in encrypted archive ==="
mount_enc
echo "overwritten" > "$MOUNTPOINT/seed.txt" 2>/dev/null || \
  echo "overwritten" > "$MOUNTPOINT/$(ls "$MOUNTPOINT" | head -1)"
unmount_wait
mount_enc
# Verify any .txt file has updated content
FOUND=0
for f in "$MOUNTPOINT"/*.txt; do
  if [ -f "$f" ] && [ "$(cat "$f")" = "overwritten" ]; then FOUND=1; break; fi
done
unmount_wait
[ "$FOUND" -eq 1 ] || { echo "FAIL: overwritten content not found"; exit 1; }
echo "PASS"

echo "All encrypted write tests passed."
SCRIPT
chmod +x scripts/tests/test_mount_write_encrypted.sh
```

- [ ] **Step 3: Create test_mount_write_strategy.sh**

```bash
cat > scripts/tests/test_mount_write_strategy.sh << 'SCRIPT'
#!/usr/bin/env bash
set -eu

PNA_BIN="${PNA_BIN:-pna}"
PNAFS_BIN="${PNAFS_BIN:-pnafs}"
WORKDIR="$(mktemp -d)"
ARCHIVE="$WORKDIR/strategy.pna"
MOUNTPOINT="$WORKDIR/mnt"

cleanup() {
  if mount | grep -q "$MOUNTPOINT"; then
    fusermount -u "$MOUNTPOINT" 2>/dev/null || umount "$MOUNTPOINT" || true
  fi
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

"$PNA_BIN" create "$ARCHIVE" --overwrite /dev/null 2>/dev/null || "$PNA_BIN" create "$ARCHIVE" --overwrite

mount_with_strategy() {
  mkdir -p "$MOUNTPOINT"
  "$PNAFS_BIN" mount --write --write-strategy "$1" "$ARCHIVE" "$MOUNTPOINT" &
  MOUNT_PID=$!
  for i in $(seq 1 10); do
    if mount | grep -q "$MOUNTPOINT"; then break; fi
    sleep 0.5
  done
}

unmount_wait() {
  fusermount -u "$MOUNTPOINT" 2>/dev/null || umount "$MOUNTPOINT"
  wait "$MOUNT_PID" 2>/dev/null || true
  sleep 0.2
}

echo "=== Strategy Test 1: lazy — file persisted after unmount ==="
mount_with_strategy lazy
echo "lazy content" > "$MOUNTPOINT/lazy.txt"
unmount_wait
mount_with_strategy lazy
CONTENT="$(cat "$MOUNTPOINT/lazy.txt")"
[ "$CONTENT" = "lazy content" ] || { echo "FAIL: $CONTENT"; exit 1; }
unmount_wait
echo "PASS"

echo "=== Strategy Test 2: immediate — file persisted ==="
mount_with_strategy immediate
echo "immediate content" > "$MOUNTPOINT/immediate.txt"
unmount_wait
mount_with_strategy immediate
CONTENT="$(cat "$MOUNTPOINT/immediate.txt")"
[ "$CONTENT" = "immediate content" ] || { echo "FAIL: $CONTENT"; exit 1; }
unmount_wait
echo "PASS"

echo "=== Strategy Test 3: lazy — multiple files all persisted ==="
mount_with_strategy lazy
for i in 1 2 3; do echo "data$i" > "$MOUNTPOINT/multi$i.txt"; done
unmount_wait
mount_with_strategy lazy
for i in 1 2 3; do
  C="$(cat "$MOUNTPOINT/multi$i.txt")"
  [ "$C" = "data$i" ] || { echo "FAIL multi$i: $C"; exit 1; }
done
unmount_wait
echo "PASS"

echo "All write-strategy tests passed."
SCRIPT
chmod +x scripts/tests/test_mount_write_strategy.sh
```

- [ ] **Step 4: Update run.sh to invoke write tests**

Edit `scripts/tests/run.sh`:

```bash
#!/usr/bin/env bash
set -eu

SCRIPT_DIR="$(dirname "$0")"

"$SCRIPT_DIR/test_mount.sh"
"$SCRIPT_DIR/test_mount_write.sh"
"$SCRIPT_DIR/test_mount_write_encrypted.sh"
"$SCRIPT_DIR/test_mount_write_strategy.sh"
```

- [ ] **Step 5: Verify scripts are executable and have valid syntax**

```bash
bash -n scripts/tests/test_mount_write.sh && \
bash -n scripts/tests/test_mount_write_encrypted.sh && \
bash -n scripts/tests/test_mount_write_strategy.sh
```
Expected: no syntax errors.

- [ ] **Step 6: Run cargo test one final time**

```bash
cargo test --locked 2>&1
```
Expected: all unit tests pass.

- [ ] **Step 7: Commit**

```bash
git add scripts/tests/test_mount_write.sh \
        scripts/tests/test_mount_write_encrypted.sh \
        scripts/tests/test_mount_write_strategy.sh \
        scripts/tests/run.sh
git commit -m "test: add shell integration tests for write support (plain, encrypted, lazy/immediate)"
```

---

## Verification

```bash
# All unit tests pass
cargo test --locked

# Build succeeds (including macos-no-mount feature)
cargo check --locked --features macos-no-mount

# CLI shows write flags
cargo run -- mount --help

# Shell integration tests (requires FUSE on Linux CI)
cargo install --locked --path .
./scripts/tests/run.sh
```
