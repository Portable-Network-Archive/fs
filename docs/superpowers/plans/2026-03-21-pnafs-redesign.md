# pnafs Internal Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the three-way data structure (id_tree + node_ids + nodes), global Mutex, 4-state FileContent, and memmap2 with a single-HashMap FileTree, RwLock, 3-state FileData, and fs::read() — eliminating seven architectural issues while maintaining all 100 existing tests.

**Architecture:** Inside-Out rewrite: create new `file_tree.rs` alongside old `archive_store.rs`, migrate consumers one at a time, then delete old code. Each task produces a compiling, test-passing state.

**Tech Stack:** Rust 2024, fuser 0.17.0, pna 0.29.3, BTreeMap (stdlib), RwLock (stdlib). Removes: id_tree 1.8.0, memmap2 0.9.10.

**Spec:** `docs/superpowers/specs/2026-03-21-pnafs-redesign.md`

---

## File Structure

| Action | File | Responsibility |
|--------|------|----------------|
| Create | `src/file_tree.rs` | FileTree, FsNode, FsContent, DirContent, FileData, CipherConfig, write API, ENOSYS stubs, uid/gid helpers |
| Modify | `src/archive_io.rs` | Adapt load/save to FileTree API, replace memmap2 with fs::read() |
| Modify | `src/filesystem.rs` | Mutex→RwLock, ArchiveStore→FileTree, use lookup_child/children |
| Modify | `src/command/mount.rs` | Update PnaFS::new() call |
| Modify | `src/main.rs` | Replace `mod archive_store` with `mod file_tree` |
| Delete | `src/archive_store.rs` | Replaced by file_tree.rs |
| Modify | `Cargo.toml` | Remove id_tree, memmap2 |

---

### Task 1: Create file_tree.rs — Data Types

**Files:**
- Create: `src/file_tree.rs`
- Modify: `src/main.rs` — add `mod file_tree;`

- [ ] **Step 1: Write failing tests for core types**

Add `#[cfg(test)] mod tests` with basic tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn make_tree() -> FileTree {
        FileTree::new_for_test(PathBuf::from("/tmp/test.pna"), None)
    }

    #[test]
    fn get_root_exists() {
        let tree = make_tree();
        assert!(tree.get(ROOT_INODE).is_some());
    }

    #[test]
    fn get_unknown_returns_none() {
        let tree = make_tree();
        assert!(tree.get(9999).is_none());
    }

    #[test]
    fn children_root_empty() {
        let tree = make_tree();
        let children: Vec<_> = tree.children(ROOT_INODE).unwrap().collect();
        assert!(children.is_empty());
    }

    #[test]
    fn is_dirty_initially_false() {
        let tree = make_tree();
        assert!(!tree.is_dirty());
    }

    #[test]
    fn lookup_child_not_found() {
        let tree = make_tree();
        assert!(tree.lookup_child(ROOT_INODE, OsStr::new("nope")).is_none());
    }
}
```

- [ ] **Step 2: Run tests to verify compile failure**

```bash
cargo test file_tree::tests 2>&1 | head -20
```
Expected: compile error — module not found.

- [ ] **Step 3: Implement core types and read API**

Create `src/file_tree.rs` with all types from the spec (§4.2):

```rust
use fuser::{Errno, FileAttr, FileType, INodeNo, TimeOrNow};
#[cfg(unix)]
use nix::unistd::{Gid, Group, Uid, User};
use pna::Permission;
use std::collections::{BTreeMap, HashMap};
use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use std::io;

pub(crate) type Inode = u64;
pub(crate) const ROOT_INODE: Inode = 1;

// CipherConfig — same as archive_store.rs but moved here
#[derive(Copy, Clone, Debug)]
pub(crate) struct CipherConfig {
    pub encryption: pna::Encryption,
    pub cipher_mode: pna::CipherMode,
}

impl CipherConfig {
    pub(crate) fn from_entry_header(header: &pna::EntryHeader) -> Option<Self> {
        if header.encryption() != pna::Encryption::No {
            Some(Self {
                encryption: header.encryption(),
                cipher_mode: header.cipher_mode(),
            })
        } else {
            None
        }
    }
}

pub(crate) enum FileData {
    Clean { data: Vec<u8>, cipher: Option<CipherConfig> },
    Dirty { data: Vec<u8>, cipher: Option<CipherConfig> },
    New(Vec<u8>),
}

pub(crate) struct DirContent {
    pub(crate) children: BTreeMap<OsString, Inode>,
}

pub(crate) enum FsContent {
    Directory(DirContent),
    File(FileData),
    Symlink(OsString),
}

pub(crate) struct FsNode {
    pub name: OsString,
    pub parent: Option<Inode>,
    pub attr: FileAttr,
    pub content: FsContent,
    pub xattrs: HashMap<OsString, Vec<u8>>,
}

pub(crate) struct FileTree {
    inodes: HashMap<Inode, FsNode>,
    next_inode: Inode,
    password: Option<String>,
    archive_path: PathBuf,
    dirty: bool,
}

// Send assertion
const _: fn() = || {
    fn assert_send<T: Send>() {}
    assert_send::<FileTree>();
};

impl FileTree {
    pub(crate) fn new(archive_path: PathBuf, password: Option<String>) -> Self {
        Self {
            inodes: HashMap::new(),
            next_inode: ROOT_INODE,
            password,
            archive_path,
            dirty: false,
        }
    }

    pub(crate) fn archive_path(&self) -> &Path { &self.archive_path }
    pub(crate) fn password(&self) -> Option<&str> { self.password.as_deref() }
    pub(crate) fn is_dirty(&self) -> bool { self.dirty }

    pub(crate) fn get(&self, ino: Inode) -> Option<&FsNode> {
        self.inodes.get(&ino)
    }

    pub(crate) fn get_mut(&mut self, ino: Inode) -> Option<&mut FsNode> {
        self.inodes.get_mut(&ino)
    }

    pub(crate) fn lookup_child(&self, parent: Inode, name: &OsStr) -> Option<&FsNode> {
        let parent_node = self.inodes.get(&parent)?;
        match &parent_node.content {
            FsContent::Directory(dir) => {
                let &child_ino = dir.children.get(name)?;
                self.inodes.get(&child_ino)
            }
            _ => None,
        }
    }

    pub(crate) fn children(&self, parent: Inode) -> Option<impl Iterator<Item = &FsNode>> {
        let parent_node = self.inodes.get(&parent)?;
        match &parent_node.content {
            FsContent::Directory(dir) => {
                let inodes = &self.inodes;
                Some(dir.children.values().filter_map(move |&ino| inodes.get(&ino)))
            }
            _ => None,
        }
    }

    pub(crate) fn next_inode(&mut self) -> Inode {
        self.next_inode += 1;
        self.next_inode
    }

    pub(crate) fn insert_node(&mut self, node: FsNode, parent: Option<Inode>) -> io::Result<Inode> {
        let ino = node.attr.ino.0;
        if let Some(parent_ino) = parent {
            let parent_node = self.inodes.get_mut(&parent_ino)
                .ok_or_else(|| io::Error::other(format!("parent inode {parent_ino} not found")))?;
            match &mut parent_node.content {
                FsContent::Directory(ref mut dir) => {
                    dir.children.insert(node.name.clone(), ino);
                }
                _ => return Err(io::Error::other("parent is not a directory")),
            }
        }
        self.inodes.insert(ino, node);
        Ok(ino)
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(archive_path: PathBuf, password: Option<String>) -> Self {
        let mut tree = Self::new(archive_path, password);
        let root = make_dir_node(ROOT_INODE, ".".into());
        tree.insert_node(root, None).unwrap();
        tree
    }
}
```

Also add `make_dir_node`, `current_uid/gid`, `get_uid/gid`, `search_owner/group` — copy from `archive_store.rs` adapting `Node` → `FsNode` and `NodeContent::Directory` → `FsContent::Directory(DirContent { children: BTreeMap::new() })`.

Add `mod file_tree;` to `src/main.rs` alongside existing `mod archive_store;`.

- [ ] **Step 4: Run tests to verify pass**

```bash
cargo test file_tree::tests 2>&1
```
Expected: 5 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/file_tree.rs src/main.rs
git commit -m "feat: add FileTree core types and read API"
```

---

### Task 2: file_tree.rs — FileData State Machine + Write API

**Files:**
- Modify: `src/file_tree.rs`

- [ ] **Step 1: Write failing tests for FileData transitions and write API**

Add tests for: `promote_to_dirty`, `make_clean`, `data_mut`, `create_file` (5 cases), `write_file` (8 cases), `set_size` (7 cases), `set_times` (6 cases), `make_dir` (4 cases + umask), `unlink` (4 cases), `mark_clean` (5 cases).

Port all test cases from the existing `archive_store::tests` (53 tests), adapting types: `NodeContent` → `FsContent`, `FileContent` → `FileData`, `Node` → `FsNode`, `ArchiveStore` → `FileTree`, etc. Test helper `make_store()` → `make_tree()`, `make_store_with_file()` → `make_tree_with_file()`.

- [ ] **Step 2: Run tests to confirm failure**

```bash
cargo test file_tree::tests 2>&1 | grep "error\|FAILED" | head -20
```
Expected: compile errors — methods not yet defined.

- [ ] **Step 3: Implement FileData methods**

```rust
impl FileData {
    /// Clean → Dirty. No-op when already Dirty or New.
    pub(crate) fn promote_to_dirty(&mut self) {
        if let FileData::Clean { data, cipher } = self {
            let data = std::mem::take(data);
            let cipher = cipher.take();
            *self = FileData::Dirty { data, cipher };
        }
    }

    pub(crate) fn make_clean(&mut self, has_password: bool) {
        match self {
            FileData::Dirty { data, cipher } => {
                let data = std::mem::take(data);
                let cipher = cipher.take();
                *self = FileData::Clean { data, cipher };
            }
            FileData::New(data) => {
                let data = std::mem::take(data);
                let cipher = if has_password {
                    Some(CipherConfig {
                        encryption: pna::Encryption::Aes,
                        cipher_mode: pna::CipherMode::CTR,
                    })
                } else {
                    None
                };
                *self = FileData::Clean { data, cipher };
            }
            FileData::Clean { .. } => {}
        }
    }

    pub(crate) fn data_mut(&mut self) -> &mut Vec<u8> {
        match self {
            FileData::Clean { data, .. }
            | FileData::Dirty { data, .. }
            | FileData::New(data) => data,
        }
    }

    pub(crate) fn data(&self) -> &[u8] {
        match self {
            FileData::Clean { data, .. }
            | FileData::Dirty { data, .. }
            | FileData::New(data) => data,
        }
    }
}
```

- [ ] **Step 4: Implement write API on FileTree**

Implement `create_file`, `write_file`, `set_size`, `set_times`, `make_dir`, `unlink`, `mark_clean`, and ENOSYS stubs. Use the spec's API signatures (§4.3).

Key differences from old `ArchiveStore`:
- `write_file`/`set_size`: call `file_data.promote_to_dirty()` instead of sentinel pattern
- `set_times`: only set `dirty = true` when at least one time actually changed
- `unlink`: remove from parent's `DirContent.children` AND from `self.inodes`
- `mark_clean`: iterate `inodes.values_mut()`, call `file_data.make_clean()` on each file

- [ ] **Step 5: Run all file_tree tests**

```bash
cargo test file_tree::tests 2>&1
```
Expected: ~55+ tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/file_tree.rs
git commit -m "feat: implement FileTree write API and state machine"
```

---

### Task 3: file_tree.rs — collect_dfs + make_dir_all

**Files:**
- Modify: `src/file_tree.rs`

- [ ] **Step 1: Write failing tests for DFS traversal and path construction**

```rust
#[test]
fn collect_dfs_empty_tree() {
    let tree = make_tree();
    let result = tree.collect_dfs();
    assert!(result.is_empty()); // root is excluded
}

#[test]
fn collect_dfs_single_file() {
    let mut tree = make_tree();
    tree.create_file(ROOT_INODE, OsStr::new("a.txt"), 0o644).unwrap();
    let result = tree.collect_dfs();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].2, "a.txt");
}

#[test]
fn collect_dfs_nested_dirs() {
    let mut tree = make_tree();
    let dir = tree.make_dir(ROOT_INODE, OsStr::new("sub"), 0o755, 0).unwrap();
    let dir_ino = dir.attr.ino.0;
    tree.create_file(dir_ino, OsStr::new("file.txt"), 0o644).unwrap();
    let result = tree.collect_dfs();
    assert_eq!(result.len(), 2);
    // BTreeMap sorted order: "sub" then "sub/file.txt"
    assert_eq!(result[0].2, "sub");
    assert_eq!(result[1].2, "sub/file.txt");
}
```

Also test `make_dir_all` (used by `archive_io::load` for nested paths):

```rust
#[test]
fn make_dir_all_creates_nested() {
    let mut tree = make_tree();
    let ino = tree.make_dir_all(Path::new("a/b/c"), ROOT_INODE).unwrap();
    assert!(tree.get(ino).is_some());
    let a = tree.lookup_child(ROOT_INODE, OsStr::new("a")).unwrap();
    let b = tree.lookup_child(a.attr.ino.0, OsStr::new("b")).unwrap();
    let c = tree.lookup_child(b.attr.ino.0, OsStr::new("c")).unwrap();
    assert_eq!(c.attr.ino.0, ino);
}
```

- [ ] **Step 2: Implement collect_dfs**

```rust
impl FileTree {
    pub(crate) fn collect_dfs(&self) -> Vec<(Inode, &FsNode, String)> {
        let mut result = Vec::new();
        if let Some(root) = self.inodes.get(&ROOT_INODE) {
            if let FsContent::Directory(ref dir) = root.content {
                self.collect_dfs_recurse(dir, &mut result, &mut String::new());
            }
        }
        result
    }

    fn collect_dfs_recurse<'a>(
        &'a self,
        dir: &DirContent,
        result: &mut Vec<(Inode, &'a FsNode, String)>,
        prefix: &mut String,
    ) {
        for (&ref name, &ino) in &dir.children {
            let node = match self.inodes.get(&ino) {
                Some(n) => n,
                None => continue,
            };
            let path = if prefix.is_empty() {
                name.to_string_lossy().into_owned()
            } else {
                format!("{}/{}", prefix, name.to_string_lossy())
            };
            result.push((ino, node, path.clone()));
            if let FsContent::Directory(ref child_dir) = node.content {
                let mut child_prefix = path;
                self.collect_dfs_recurse(child_dir, result, &mut child_prefix);
            }
        }
    }

    pub(crate) fn make_dir_all(&mut self, path: &Path, mut parent: Inode) -> io::Result<Inode> {
        for component in path.components() {
            let name = component.as_os_str();
            if let Some(child) = self.lookup_child(parent, name) {
                parent = child.attr.ino.0;
            } else {
                let ino = self.next_inode();
                let dir_node = make_dir_node(ino, name.to_owned());
                self.insert_node(dir_node, Some(parent))?;
                parent = ino;
            }
        }
        Ok(parent)
    }
}
```

- [ ] **Step 3: Run tests**

```bash
cargo test file_tree::tests 2>&1
```
Expected: all tests pass.

- [ ] **Step 4: Commit**

```bash
git add src/file_tree.rs
git commit -m "feat: add collect_dfs and make_dir_all to FileTree"
```

---

### Task 4: Migrate archive_io.rs to FileTree + Remove archive_store.rs

**Files:**
- Modify: `src/archive_io.rs`
- Modify: `src/main.rs` — replace `mod archive_store` with `mod file_tree`
- Delete: `src/archive_store.rs`

**Note**: Tasks 4-6 are merged into a single atomic step because `archive_io::load()` changing its return type from `ArchiveStore` to `FileTree` would break `archive_store::tests` (which cross-reference `archive_io::load`). All consumers must be updated in the same commit.

- [ ] **Step 1: Update imports**

Replace:
```rust
use crate::archive_store::{
    ArchiveStore, CipherConfig, FileContent, Node, NodeContent, ROOT_INODE, get_gid, get_uid,
    make_dir_node,
};
```
With:
```rust
use crate::file_tree::{
    FileTree, CipherConfig, FileData, FsNode, FsContent, DirContent, ROOT_INODE, get_gid, get_uid,
    make_dir_node,
};
```

Remove `use id_tree::NodeId;`.

- [ ] **Step 2: Rewrite load() — replace memmap2 with fs::read()**

```rust
pub(crate) fn load(archive_path: &Path, password: Option<String>) -> io::Result<FileTree> {
    cleanup_stale_tmp(archive_path);

    let data = fs::read(archive_path)?;   // replaces memmap2
    let password_bytes: Option<Vec<u8>> = password.as_deref().map(|s| s.as_bytes().to_vec());
    let mut archive = Archive::read_header_from_slice(&data)?;

    let mut tree = FileTree::new(archive_path.to_path_buf(), password);

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
                    add_normal_entry(&mut tree, e?, pw)?;
                }
            }
        }
    }

    // No need to reset dirty — FileTree::new() starts with dirty=false
    // and insert_node() does not set dirty (only write API methods do).
    Ok(tree)
}
```

Update `add_normal_entry` to use `FileTree::insert_node`, `FileTree::make_dir_all`, `FsNode`, `FsContent`, `FileData::Clean`, `DirContent`.

**Important**: Preserve duplicate entry handling for incremental archives. The current code (archive_io.rs:174-195) checks if a child with the same name already exists under the parent and updates it in-place (reusing the existing inode). The rewritten version must use `tree.lookup_child(parent, &name)` to detect duplicates, then either `tree.get_mut(existing_ino)` to update in-place, or `tree.unlink` + `tree.insert_node` to replace.

- [ ] **Step 3: Rewrite save() — use collect_dfs()**

```rust
pub(crate) fn save(tree: &FileTree) -> io::Result<()> {
    let archive_path = tree.archive_path();

    // Password guard
    for (_, node, _) in tree.collect_dfs() {
        if let FsContent::File(FileData::Clean { cipher: Some(_), .. }
            | FileData::Dirty { cipher: Some(_), .. }) = &node.content
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
    // ... tmp file + DFS write + finalize + rename (same pattern, adapted types) ...
}
```

Replace `write_subtree` recursive function with a flat loop over `collect_dfs()` results.

- [ ] **Step 4: Update test module**

Replace all `ArchiveStore` → `FileTree`, `NodeContent` → `FsContent`, `FileContent` → `FileData`, etc. in test code. `read_node_data` adapts to `FileData::Clean/Dirty/New`.

- [ ] **Step 5: Build and test**

```bash
cargo test archive_io::tests 2>&1
```
Expected: all archive_io tests pass.

- [ ] **Step 6: Update filesystem.rs — Mutex→RwLock + FileTree**

Update imports to use `file_tree` module, replace `Mutex` with `RwLock`:

```rust
use crate::archive_io;
use crate::file_tree::{FileTree, FileData, FsContent, FsNode};
use std::sync::RwLock;  // was Mutex

pub(crate) struct PnaFS {
    tree: RwLock<FileTree>,    // was Mutex<ArchiveStore>
    write_strategy: Option<WriteStrategy>,
}
```

- [ ] **Step 7: Update PnaFS::new and save_if_dirty**

```rust
impl PnaFS {
    pub(crate) fn new(
        archive: PathBuf,
        password: Option<String>,
        write_strategy: Option<WriteStrategy>,
    ) -> io::Result<Self> {
        let tree = archive_io::load(&archive, password)?;
        Ok(Self {
            tree: RwLock::new(tree),
            write_strategy,
        })
    }

    fn save_if_dirty(tree: &mut FileTree) -> io::Result<()> {
        if tree.is_dirty() {
            archive_io::save(tree)?;
            tree.mark_clean();
        }
        Ok(())
    }
}
```

- [ ] **Step 8: Update read operations to use .read().unwrap()**

Change `self.store.lock().unwrap()` → `self.tree.read().unwrap()` for:
- `lookup` — use `tree.lookup_child()` instead of `get_children().find()`
- `getattr` — use `tree.get()`
- `readlink` — use `tree.get()`
- `open` — use `tree.get()` for inode validation
- `read` — use `tree.get()` (no more inline force-load needed)
- `flush` — use `tree.get()`
- `readdir` — use `tree.children()`
- `getxattr` — use `tree.get()`
- `listxattr` — use `tree.get()`

- [ ] **Step 9: Update write operations to use .write().unwrap()**

Change `self.store.lock().unwrap()` → `self.tree.write().unwrap()` for:
- `write` — use `tree.write_file()`
- `setattr` — use `tree.set_size()`, `tree.set_times()`
- `create` — use `tree.lookup_child()` for existing check, `tree.create_file()`
- `mkdir` — use `tree.make_dir()`
- `unlink` — use `tree.unlink()`
- `rename` — use `tree.rename()`

- [ ] **Step 10: Update save/destroy paths**

- `release` / `fsync` — `self.tree.write().unwrap()` → `save_if_dirty`
- `destroy` — `self.tree.get_mut().unwrap()` → `save_if_dirty`

- [ ] **Step 11: Update main.rs and command/mount.rs**

Update `src/main.rs`:
```rust
mod archive_io;
mod file_tree;     // was: mod archive_store;
mod cli;
mod command;
mod filesystem;
```

`command/mount.rs` should not need changes if `PnaFS::new` signature is unchanged.

- [ ] **Step 12: Delete archive_store.rs + Remove dependencies**

```bash
git rm src/archive_store.rs
```

Remove `id_tree` and `memmap2` from `Cargo.toml` `[dependencies]`.

- [ ] **Step 13: Build and test**

```bash
cargo build 2>&1 | head -30
cargo test 2>&1 | tail -10
```
Expected: all tests pass. No reference to old `archive_store` module remains.

- [ ] **Step 14: Commit**

```bash
git add -u src/ Cargo.toml Cargo.lock
git commit -m "refactor: migrate to FileTree+RwLock, remove archive_store/id_tree/memmap2"
```

---

### Task 5: Add New Tests for FileTree-Specific Features

**Files:**
- Modify: `src/file_tree.rs`

- [ ] **Step 1: Add tests for BTreeMap ordering in readdir**

```rust
#[test]
fn children_returns_sorted_order() {
    let mut tree = make_tree();
    tree.create_file(ROOT_INODE, OsStr::new("z.txt"), 0o644).unwrap();
    tree.create_file(ROOT_INODE, OsStr::new("a.txt"), 0o644).unwrap();
    tree.create_file(ROOT_INODE, OsStr::new("m.txt"), 0o644).unwrap();
    let names: Vec<_> = tree.children(ROOT_INODE).unwrap()
        .map(|n| n.name.to_string_lossy().into_owned())
        .collect();
    assert_eq!(names, vec!["a.txt", "m.txt", "z.txt"]);
}
```

- [ ] **Step 2: Add tests for parent back-pointer correctness**

```rust
#[test]
fn parent_pointer_set_on_insert() {
    let mut tree = make_tree();
    tree.create_file(ROOT_INODE, OsStr::new("f.txt"), 0o644).unwrap();
    let child = tree.lookup_child(ROOT_INODE, OsStr::new("f.txt")).unwrap();
    assert_eq!(child.parent, Some(ROOT_INODE));
}

#[test]
fn parent_pointer_for_nested() {
    let mut tree = make_tree();
    let dir = tree.make_dir(ROOT_INODE, OsStr::new("sub"), 0o755, 0).unwrap();
    let dir_ino = dir.attr.ino.0;
    tree.create_file(dir_ino, OsStr::new("f.txt"), 0o644).unwrap();
    let child = tree.lookup_child(dir_ino, OsStr::new("f.txt")).unwrap();
    assert_eq!(child.parent, Some(dir_ino));
}
```

- [ ] **Step 3: Add tests for is_dirty on non-file mutations**

```rust
#[test]
fn is_dirty_after_make_dir() {
    let mut tree = make_tree();
    tree.make_dir(ROOT_INODE, OsStr::new("d"), 0o755, 0).unwrap();
    assert!(tree.is_dirty());
}

#[test]
fn is_dirty_after_unlink() {
    let mut tree = make_tree();
    tree.create_file(ROOT_INODE, OsStr::new("f.txt"), 0o644).unwrap();
    tree.mark_clean();
    tree.unlink(ROOT_INODE, OsStr::new("f.txt")).unwrap();
    assert!(tree.is_dirty());
}

#[test]
fn is_dirty_after_set_times_with_value() {
    let mut tree = make_tree();
    tree.create_file(ROOT_INODE, OsStr::new("f.txt"), 0o644).unwrap();
    tree.mark_clean();
    let t = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1000);
    let ino = tree.lookup_child(ROOT_INODE, OsStr::new("f.txt")).unwrap().attr.ino.0;
    tree.set_times(ino, Some(TimeOrNow::SpecificTime(t)), None).unwrap();
    assert!(tree.is_dirty());
}

#[test]
fn not_dirty_after_set_times_none_none() {
    let mut tree = make_tree();
    tree.create_file(ROOT_INODE, OsStr::new("f.txt"), 0o644).unwrap();
    tree.mark_clean();
    let ino = tree.lookup_child(ROOT_INODE, OsStr::new("f.txt")).unwrap().attr.ino.0;
    tree.set_times(ino, None, None).unwrap();
    assert!(!tree.is_dirty());
}
```

- [ ] **Step 4: Run all tests**

```bash
cargo test 2>&1 | tail -5
```
Expected: all tests pass (should be 100+ including new FileTree-specific tests).

- [ ] **Step 5: Commit**

```bash
git add src/file_tree.rs
git commit -m "test: add FileTree-specific tests (ordering, parent pointers, dirty tracking)"
```

---

## Verification

```bash
# All unit tests pass
cargo test

# No clippy errors
cargo clippy

# Shell integration tests syntax
bash -n scripts/tests/test_mount_write.sh
bash -n scripts/tests/test_mount_write_encrypted.sh
bash -n scripts/tests/test_mount_write_strategy.sh

# CI — push and verify
git push origin feat/write-mode
```

After CI passes, squash to 3 commits: docs, implementation+unit tests, shell tests.
