# pnafs Internal Redesign Specification

**Date**: 2026-03-21
**Status**: Draft
**Branch**: feat/write-mode (in-place rewrite)

## 1. Problem Statement

The current pnafs write implementation has seven architectural issues identified through four rounds of code review:

1. **Three-way data sync**: `tree` + `node_ids` + `nodes` must be kept in sync manually — no compile-time enforcement
2. **Global Mutex**: `Mutex<ArchiveStore>` serializes ALL FUSE operations including concurrent reads
3. **4-state FileContent with dead variant**: `Unloaded` is never produced; `Loaded`/`Modified`/`Created` require sentinel pattern transitions
4. **pub(crate) field leakage**: `archive_io` directly accesses `store.nodes`, `store.tree`, `store.dirty`
5. **id_tree dependency overhead**: Requires parallel `node_ids` HashMap for inode→NodeId mapping; no name-based child lookup
6. **memmap2 with no benefit**: mmap is created then immediately consumed and dropped — unsafe for zero gain
7. **O(n) directory lookup**: `get_children()` allocates Vec and does linear scan on every FUSE lookup

## 2. Goals

- Eliminate all seven issues above
- Maintain all 100 existing tests (72 unit + 28 shell integration)
- No new features (ENOSYS stubs remain as-is)
- No pna crate changes required
- Work on feat/write-mode branch (in-place rewrite)

## 3. Non-Goals

- Lazy loading (fSIZ cannot be trusted — all entries are eagerly decoded)
- Incremental/append save (pna crate limitation)
- New filesystem operations (rename, rmdir, chmod, etc.)
- Streaming large file support
- Verbatim pass-through of unchanged archive entries: since all entries are
  eagerly decoded into `FileData::Clean`, the original compressed/encrypted
  byte stream is discarded at load time. Every save re-encodes all entries.
  This is the current behavior and is accepted as a known inefficiency.

## 4. Architecture

### 4.1 Module Structure

```
src/
  main.rs           — entry point (unchanged)
  cli.rs            — CLI definitions (unchanged)
  command/mount.rs  — mount command (minor: ArchiveStore → FileTree)
  file_tree.rs      — NEW: core data structure (replaces archive_store.rs)
  archive_io.rs     — PNA serialization (adapted to FileTree API)
  filesystem.rs     — FUSE adapter (Mutex → RwLock, uses FileTree)
```

### 4.2 Core Data Structure: FileTree

Replaces `ArchiveStore` + `id_tree` dependency.

```rust
pub(crate) struct FileTree {
    inodes: HashMap<Inode, FsNode>,   // single source of truth
    next_inode: Inode,
    password: Option<String>,
    archive_path: PathBuf,
    dirty: bool,                      // structural dirty flag (see §4.7)
}

pub(crate) struct FsNode {
    name: OsString,
    parent: Option<Inode>,            // back-pointer for path construction
    attr: FileAttr,
    content: FsContent,
    xattrs: HashMap<OsString, Vec<u8>>,
}

pub(crate) enum FsContent {
    Directory(DirContent),
    File(FileData),
    Symlink(OsString),
}

pub(crate) struct DirContent {
    children: BTreeMap<OsString, Inode>,  // sorted for deterministic readdir
}

pub(crate) enum FileData {
    Clean { data: Vec<u8>, cipher: Option<CipherConfig> },
    Dirty { data: Vec<u8>, cipher: Option<CipherConfig> },
    New(Vec<u8>),
}
```

**Key design decisions**:

- **`BTreeMap` for `DirContent.children`** (not `HashMap`): FUSE `readdir`
  uses offset-based pagination. The kernel may call `readdir` multiple times
  with increasing offsets for the same listing. HashMap iteration order is
  non-deterministic and can change between calls, causing duplicate or
  missing entries. BTreeMap provides stable sorted order, making offset-based
  skip safe.

- **`parent: Option<Inode>` back-pointer**: Enables bottom-up path
  construction for `iter_dfs()` and future `rename()`. Maintained by:
  - `insert_node()`: sets `parent` to the provided parent inode
  - `unlink()`: removes node from parent's `DirContent.children` AND
    removes the node itself from `inodes` (parent pointer becomes moot)
  - Future `rename()`: would update `parent` on the moved node and update
    both old and new parent `DirContent.children`

- **`dirty: bool` on FileTree** (not derived): See §4.7 for rationale.

**Key invariants** (enforced by FileTree methods, not exposing fields):
- Every inode in `DirContent.children` exists in `inodes`
- Every `FsNode.parent` exists in `inodes` (or is None for root)
- `FsNode.attr.ino` matches the key in `inodes`
- `FsNode.attr.kind` agrees with `FsContent` variant

### 4.3 FileTree Public API

```rust
impl FileTree {
    // Construction
    pub(crate) fn new(archive_path: PathBuf, password: Option<String>) -> Self;

    // Read operations (used under RwLock::read)
    pub(crate) fn get(&self, ino: Inode) -> Option<&FsNode>;
    pub(crate) fn lookup_child(&self, parent: Inode, name: &OsStr) -> Option<&FsNode>;
    pub(crate) fn children(&self, parent: Inode) -> Option<impl Iterator<Item = &FsNode>>;
    pub(crate) fn is_dirty(&self) -> bool;
    pub(crate) fn archive_path(&self) -> &Path;
    pub(crate) fn password(&self) -> Option<&str>;

    // Write operations (used under RwLock::write)
    pub(crate) fn create_file(&mut self, parent: Inode, name: &OsStr, mode: u32) -> Result<&FsNode, Errno>;
    pub(crate) fn write_file(&mut self, ino: Inode, offset: u64, data: &[u8]) -> Result<usize, Errno>;
    pub(crate) fn set_size(&mut self, ino: Inode, size: u64) -> Result<(), Errno>;
    pub(crate) fn set_times(&mut self, ino: Inode, atime: Option<TimeOrNow>, mtime: Option<TimeOrNow>) -> Result<(), Errno>;
    pub(crate) fn make_dir(&mut self, parent: Inode, name: &OsStr, mode: u32, umask: u32) -> Result<&FsNode, Errno>;
    pub(crate) fn unlink(&mut self, parent: Inode, name: &OsStr) -> Result<(), Errno>;
    pub(crate) fn mark_clean(&mut self);

    // Archive I/O support
    pub(crate) fn insert_node(&mut self, node: FsNode, parent: Option<Inode>) -> io::Result<Inode>;
    pub(crate) fn collect_dfs(&self) -> Vec<(Inode, &FsNode, String)>;

    // Phase 2 stubs (ENOSYS)
    pub(crate) fn rmdir(&mut self, parent: Inode, name: &OsStr) -> Result<(), Errno>;
    pub(crate) fn rename(&mut self, ...) -> Result<(), Errno>;
    pub(crate) fn set_attr_full(&mut self, ...) -> Result<(), Errno>;
    pub(crate) fn create_symlink(&mut self, ...) -> Result<&FsNode, Errno>;
    pub(crate) fn create_hardlink(&mut self, ...) -> Result<(), Errno>;
    pub(crate) fn set_xattr(&mut self, ...) -> Result<(), Errno>;
    pub(crate) fn remove_xattr(&mut self, ...) -> Result<(), Errno>;
}
```

**Note on `collect_dfs()`**: Returns `Vec` (not `impl Iterator`) because
recursive DFS over a HashMap with parent pointers cannot produce a
zero-allocation iterator without boxing. Since this is only used in the
save path (not a hot path), eager collection is acceptable. The returned
`String` is the full archive path (e.g., `"dir/subdir/file.txt"`),
constructed by walking `parent` pointers up to root and reversing.

**Note on FUSE `create()` handler**: The `create()` handler needs to handle
`O_EXCL`/`O_TRUNC` for existing files. It calls `lookup_child()` first to
check if the name exists, gets the inode from `attr.ino`, then calls
`set_size(ino, 0)` for truncation. This is a restructuring from the
current approach but uses only the public API above.

### 4.4 FileData State Machine

Three states (down from four — Unloaded eliminated):

```
                    write/truncate
    Clean ──────────────────────────► Dirty
      ▲                                  │
      │            mark_clean()          │
      └──────────────────────────────────┘

    New ────────── mark_clean() ──────► Clean
```

Transitions implemented as methods on FileData:
- `promote_to_dirty()`: Clean → Dirty. No-op when already Dirty or New (already in a writable state).
- `make_clean(has_password)`: Dirty → Clean, New → Clean(+cipher if password)
- `data_mut()`: Returns `&mut Vec<u8>` for any state

No sentinel pattern. Uses `std::mem::take` for zero-cost ownership transfer.

### 4.5 Concurrency: RwLock

```rust
pub(crate) struct PnaFS {
    tree: RwLock<FileTree>,
    write_strategy: Option<WriteStrategy>,
}
```

- Read operations: `self.tree.read().unwrap()` — concurrent
- Write operations: `self.tree.write().unwrap()` — exclusive
- Save operations: `self.tree.write().unwrap()` → save → mark_clean

### 4.6 Archive I/O Changes

**Load** (`archive_io::load`):
- `fs::read()` replaces `memmap2::Mmap::map()` — removes unsafe
- Returns `FileTree` instead of `ArchiveStore`
- Uses `tree.insert_node()` instead of direct field access
- Resets `dirty` to false after loading completes

**Save** (`archive_io::save`):
- Takes `&FileTree` instead of `&ArchiveStore`
- Uses `tree.collect_dfs()` for DFS traversal — no direct field access
- Password guard via iterating collected nodes + pattern match on cipher
- Atomic write pattern unchanged (tmp + fsync + rename)
- Cleanup-on-error via scope guard (remove tmp on failure)
- **Symlinks**: Skipped during save with `log::warn!` (matching current
  behavior). Symlink serialization is a Phase 2 concern.

### 4.7 Dirty State Management

**Design**: `dirty: bool` field on `FileTree`, set by ALL mutation methods.

The initial brainstorming proposed deriving `is_dirty()` from `FileData`
state alone (`any(Dirty | New)`). Spec review identified a critical flaw:
this misses non-file mutations:
- `set_times()` on a `Clean` file changes `attr.mtime` but `FileData`
  stays `Clean` → change would be silently lost
- `make_dir()` creates a directory (not a file) → no `FileData` to check
- `unlink()` removes a node → no dirty `FileData` remains

**Solution**: Use a simple `dirty: bool` field, but unlike the old design,
only mutation methods that actually change state set it:
- `create_file()`, `make_dir()`, `unlink()`: always set `dirty = true`
- `write_file()`: sets `dirty = true` (but not for empty writes)
- `set_size()`: sets `dirty = true`
- `set_times()`: sets `dirty = true` only when at least one time is
  actually changed (not when both atime and mtime are None)
- `mark_clean()`: resets `dirty = false` and transitions Dirty→Clean,
  New→Clean

This is the same approach as the current design but with the `set_times`
None-None bug fixed.

## 5. Migration Strategy (Inside-Out)

### Step 1: Create file_tree.rs with FileTree + FsNode + FileData + tests

Write new module with full API and unit tests. Add `mod file_tree;` to
main.rs alongside existing `mod archive_store;`. Both modules coexist.

### Step 2: Migrate archive_io.rs to use FileTree

Change `load()` to return `FileTree`, `save()` to take `&FileTree`.
Replace `fs::read()` for memmap2. Keep old `archive_store` module
temporarily.

### Step 3: Migrate filesystem.rs to use FileTree + RwLock

Replace `Mutex<ArchiveStore>` with `RwLock<FileTree>`. Change all lock
calls (read vs write). Restructure `create()` handler to use
`lookup_child()` for the `O_EXCL`/`O_TRUNC` existing-file path.

### Step 4: Migrate command/mount.rs

Update `PnaFS::new()` signature.

### Step 5: Delete archive_store.rs + remove id_tree/memmap2 dependencies

Remove old module and unused crate dependencies from Cargo.toml.

### Step 6: Update all tests

Port 72 unit tests to FileTree API. Shell tests pass unchanged.

## 6. Dependencies Changed

| Crate | Action | Reason |
|-------|--------|--------|
| id_tree | **Remove** | Replaced by FileTree's HashMap + DirContent |
| memmap2 | **Remove** | Replaced by fs::read() |
| (no additions) | — | No new dependencies |

## 7. Testing Strategy

- All 72 existing unit tests are ported to the new API (same assertions,
  new types)
- All 28 shell integration tests pass unchanged (external behavior
  identical)
- New unit tests for:
  - `lookup_child()` O(1) behavior
  - `children()` iterator (deterministic BTreeMap order)
  - `collect_dfs()` traversal correctness and path construction
  - `is_dirty()` correctness for all mutation types (file write,
    set_times, make_dir, unlink)
  - FileData state transitions (`promote_to_dirty`, `make_clean`)
  - DirContent children consistency after insert/unlink
  - `parent` back-pointer correctness after mutations

## 8. Risks

| Risk | Mitigation |
|------|------------|
| Large diff (rewrite of 3 files) | Inside-out: each step compiles and tests pass |
| BTreeMap slower than HashMap for children | Negligible for typical directory sizes (<10K entries) |
| Borrow checker issues with RwLock | Same patterns as Mutex; read/write split is straightforward |
| fs::read() OOM on huge archives | Same as current (all data in RAM anyway); mmap didn't help |
| readdir offset stability | BTreeMap provides stable sorted iteration order |
