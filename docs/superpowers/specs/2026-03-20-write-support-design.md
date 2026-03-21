# Write Support Design for pnafs

**Date:** 2026-03-20
**Status:** Approved (rev 6 — adversarial review fixes applied)

## Overview

Add write support to pnafs, the FUSE filesystem for PNA archives. Currently read-only, the filesystem will support mutating operations with changes persisted back to the archive on unmount (default) or on file close (optional).

## Requirements

- **Phase 1 (initial):** `create`, `write`, `mkdir`, `unlink`, `setattr(size, timestamps)`
- **Phase 2 (full set):** `rmdir`, `rename`, full `setattr` (chmod/chown), `symlink`, `hardlink`, `setxattr`, `removexattr`
- **Default behavior:** read-only (backward compatible); write mode opt-in via `--write` flag
- **Persistence strategy:** configurable — lazy (on unmount, default) or immediate (on file close)
- **Encryption:** new/modified files inherit the original cipher configuration; new files are encrypted only when a password was provided at mount time
- **Atomicity:** archive written to a temp file then `rename`d into place (crash-safe)

## Architecture

Three layers replace the current single `FileManager`:

```
┌─────────────────────────────────────────┐
│  PnaFS  (FUSE adapter)                  │
│  Mutex<ArchiveStore> + Option<WriteStrategy> │
│  Delegates all FUSE ops to ArchiveStore │
│  Triggers flush in destroy() / release()│
└──────────────┬──────────────────────────┘
               │
┌──────────────▼──────────────────────────┐
│  ArchiveStore  (in-memory FS state)     │
│  · inode tree (id_tree)                 │
│  · Node / NodeContent / FileContent     │
│  · Write API covering all FUSE ops      │
└──────────────┬──────────────────────────┘
               │
┌──────────────▼──────────────────────────┐
│  archive_io  (PNA serialization)        │
│  load(path, password) → ArchiveStore   │
│  save(store, path, password) [atomic]  │
└─────────────────────────────────────────┘
```

`WriteStrategy` is owned by `PnaFS` as `Option<WriteStrategy>` — `None` means read-only. The store manages in-memory state only; persistence policy is the adapter's concern.

## Data Model

### CipherConfig

Stores the encryption configuration needed to re-encrypt a file on save. Extracted from `NormalEntry` at load time.

```rust
pub(crate) struct CipherConfig {
    pub encryption: pna::Encryption,
    pub cipher_mode: pna::CipherMode,
    // hash_algorithm is intentionally absent: pna::NormalEntry does not expose
    // hash_algorithm() in its public API. All re-encryption uses argon2id()
    // unconditionally, which is the modern recommended KDF.
}
```

When loading, if `entry.header().encryption() != Encryption::No`, extract `encryption` and `cipher_mode` from the entry header and store in `CipherConfig`. The `HashAlgorithm` is not recoverable from `NormalEntry`'s public API; `argon2id()` is always used on re-encryption.

Compile-time check in `archive_store.rs` (the real requirement is that `ArchiveStore: Send`; `NormalEntry` is included to document the dependency):
```rust
const _: fn() = || {
    fn assert_send<T: Send>() {}
    assert_send::<ArchiveStore>();
};
```

### ArchiveStore

```rust
pub(crate) struct ArchiveStore {
    tree: id_tree::Tree<Inode>,
    node_ids: HashMap<Inode, NodeId>,
    nodes: HashMap<Inode, Node>,
    last_inode: Inode,
    password: Option<String>,
    archive_path: PathBuf,
    dirty: bool,   // set true by any write op; reset by mark_clean()
}
```

### Node

```rust
pub(crate) struct Node {
    pub name: OsString,
    pub attr: FileAttr,
    pub content: NodeContent,
    pub xattrs: HashMap<OsString, Vec<u8>>,  // present for all node types
}

pub(crate) enum NodeContent {
    Directory,
    File(FileContent),
    Symlink(OsString),        // phase 2
}

pub(crate) enum FileContent {
    /// Not yet read from archive (lazy load). Only for non-solid NormalEntry objects
    /// (i.e., entries from pna::ReadEntry::Normal). Entries from pna::ReadEntry::Solid
    /// are force-loaded into Loaded at archive_io::load() time.
    Unloaded(pna::NormalEntry<Vec<u8>>, ReadOptions),
    /// Read into memory, unmodified. cipher is None if original was unencrypted.
    Loaded { data: Vec<u8>, cipher: Option<CipherConfig> },
    /// Written to via write() or set_size(). cipher inherited from Loaded.
    Modified { data: Vec<u8>, cipher: Option<CipherConfig> },
    /// Born via create(). Encrypted only when password is Some.
    Created(Vec<u8>),
}
```

**Write transitions:**

```
Unloaded ──force_load()──► Loaded { data, cipher } ──write()/set_size()──► Modified { data, cipher }
Created  ──────────────────────────────────────────────write()──────────►   Created   (stays Created)
```

`write()` and `set_size()` must force-load an `Unloaded` entry before operating (see ArchiveStore API). `write()` beyond EOF zero-fills the gap (POSIX-compliant): if `offset as usize > data.len()`, `data.resize(offset as usize, 0)` before writing. `offset` is cast to `usize`; on 32-bit targets or files > 4 GiB, check `usize::try_from(offset).ok_or(Errno::EFBIG)?` before use.

`ArchiveStore::is_dirty()` returns `self.dirty`. The `dirty` flag is set to `true` by any mutating operation (file writes, creates, unlink, mkdir, set_times, set_size, xattr mutations). After a successful `archive_io::save()`, `mark_clean()` resets it to `false`.

**xattrs** are stored directly on `Node` (not inside `FileContent`). They are populated at load time from `entry.xattrs()`. Phase 2 `setxattr`/`removexattr` mutate this map directly and set `dirty = true`.

### Known limitations

- **Compression loss on Loaded files:** The `Loaded` variant holds decompressed bytes (`entry.reader().read_to_end()` decompresses). On save, these files are written uncompressed. Archives that are remounted and unmounted will grow in size.
- **Solid archive conversion:** PNA solid archives (SHED/SEND blocks) are decompressed at load time into ordinary `Loaded` nodes. On save, `archive_io` writes a non-solid normal archive. The user's archive format silently converts on first write. Document in the CLI help for `--write`.
- **Cipher mode upgrade for new files:** Files created via `create()` are always encrypted with `(Encryption::Aes, CipherMode::CTR, argon2id)` when a password is provided, regardless of the encryption/KDF settings used by existing files in the archive. Similarly, re-encrypted files (Modified/Loaded) retain their original cipher mode (`CipherConfig.cipher_mode`) but switch to `argon2id` if the original used a weaker KDF. This is an intentional security upgrade. Users should be aware that a heterogeneous archive may result.
- **Mutex held during Immediate-mode save:** In `release()` with `WriteStrategy::Immediate`, the `Mutex<ArchiveStore>` is held for the entire duration of `archive_io::save()`. Concurrent FUSE operations will block. Acceptable for Phase 1 (single-user desktop filesystem).
- **Single-writer assumption:** No per-`FileHandle` write buffer is maintained. Concurrent writes to the same inode share one `Vec<u8>` and will interleave. `O_EXCL` in `create()` prevents concurrent creation; open-then-write races are undefined behavior in Phase 1.

## ArchiveStore API

### Phase 1

```rust
impl ArchiveStore {
    // ── Read (unchanged from FileManager) ────────────────────
    pub fn get_node(&self, ino: Inode) -> Option<&Node>;
    pub fn get_node_mut(&mut self, ino: Inode) -> Option<&mut Node>;
    pub fn get_children(&self, ino: Inode) -> Option<Vec<&Node>>;

    // ── Write — phase 1 ──────────────────────────────────────
    // O_EXCL / existing-file handling is done at the FUSE layer (see FUSE
    // Operation Mapping); create_file always creates a brand-new inode.
    pub fn create_file(&mut self, parent: Inode, name: &OsStr,
                       mode: u32) -> Result<&Node, Errno>;

    // Force-loads Unloaded content before writing. Zero-fills sparse gaps.
    // Updates node.attr.size = data.len() as u64 after the write.
    // Returns number of bytes written.
    pub fn write_file(&mut self, ino: Inode, offset: u64,
                      data: &[u8]) -> Result<usize, Errno>;

    // Force-loads Unloaded content before truncating/extending.
    // Updates node.attr.size = size after the operation.
    pub fn set_size(&mut self, ino: Inode, size: u64) -> Result<(), Errno>;

    // atime / mtime use fuser's TimeOrNow to correctly handle UTIME_NOW.
    pub fn set_times(&mut self, ino: Inode,
                     atime: Option<fuser::TimeOrNow>,
                     mtime: Option<fuser::TimeOrNow>) -> Result<(), Errno>;

    // New directory gets nlink = 2 (one for its entry in parent, one for ".").
    // Parent's nlink is incremented by 1.
    pub fn make_dir(&mut self, parent: Inode, name: &OsStr,
                    mode: u32, umask: u32) -> Result<&Node, Errno>;

    // Removes the node entirely from self.tree, self.node_ids, and self.nodes.
    pub fn unlink(&mut self, parent: Inode, name: &OsStr) -> Result<(), Errno>;

    // ── Post-flush state reset ────────────────────────────────
    pub fn mark_clean(&mut self);

    // ── Phase 2 stubs (return Err(Errno::ENOSYS) until implemented) ──
    pub fn rmdir(&mut self, parent: Inode, name: &OsStr) -> Result<(), Errno>;
    // flags carries RENAME_NOREPLACE / RENAME_EXCHANGE from fuser::RenameFlags.
    // Note: RENAME_NOREPLACE and RENAME_EXCHANGE are only defined on Linux
    // (fuser::RenameFlags constants are #[cfg(target_os = "linux")]); on macOS,
    // RenameFlags is an empty bitflags struct. Phase 2 rename handling must
    // gate non-trivial flag checks with #[cfg(target_os = "linux")].
    pub fn rename(&mut self, old_parent: Inode, old_name: &OsStr,
                  new_parent: Inode, new_name: &OsStr,
                  flags: fuser::RenameFlags) -> Result<(), Errno>;
    pub fn set_attr_full(&mut self, ino: Inode, mode: Option<u32>,
                         uid: Option<u32>, gid: Option<u32>) -> Result<(), Errno>;
    pub fn create_symlink(&mut self, parent: Inode, name: &OsStr,
                          target: &Path) -> Result<&Node, Errno>;
    pub fn create_hardlink(&mut self, parent: Inode, name: &OsStr,
                           target: Inode) -> Result<(), Errno>;
    pub fn set_xattr(&mut self, ino: Inode, name: &OsStr,
                     value: &[u8]) -> Result<(), Errno>;
    pub fn remove_xattr(&mut self, ino: Inode, name: &OsStr) -> Result<(), Errno>;

    // ── Accessors used by archive_io ─────────────────────────
    pub fn archive_path(&self) -> &Path;
    pub fn password(&self) -> Option<&str>;

    // ── Dirty check ──────────────────────────────────────────
    pub fn is_dirty(&self) -> bool;
}
```

`offset` uses `u64` throughout to match fuser's FUSE API.

**`unlink()` full semantics** — removes from all data structures:

```
1. Find the child inode by name in parent's children (via tree / get_children).
2. Detach the node from the tree: tree.remove_node(node_id, RemoveBehavior::OrphanChildren).
   (Files have no children; OrphanChildren is the correct id_tree variant for removal.)
3. Remove from self.node_ids: self.node_ids.remove(&ino).
4. Remove from self.nodes: self.nodes.remove(&ino).
5. Set dirty = true.
```

**`write_file()` and `set_size()` force-load semantics:**

Both operations must eagerly decode `Unloaded` entries before mutating:

```rust
// This pattern relies on Rust 2024 NLL (edition = "2024" in Cargo.toml):
// the immutable borrow of `node.content` ends at the last use of `entry`/`opts`
// (at `extract_cipher(...)`), so the assignment that follows compiles correctly.
// Do not introduce any use of `entry` or `opts` after the assignment.
if let NodeContent::File(FileContent::Unloaded(entry, opts)) = &node.content {
    let mut data = Vec::new();
    entry.reader(opts)?.read_to_end(&mut data)?;
    let cipher = extract_cipher(entry.header()); // → Option<CipherConfig>
    node.content = NodeContent::File(FileContent::Loaded { data, cipher });
}
// Now content is Loaded; proceed with truncate/write logic.
// After mutation, update: node.attr.size = data_len as u64
```

`set_times()` on an `Unloaded` node does **not** require a force-load — it only updates `node.attr`, which is applied to the entry on save.

**`mark_clean()` semantics:**

```
For each File(Unloaded): leave as Unloaded (no transition needed; still clean).
For each File(Modified { data, cipher }): replace with File(Loaded { data, cipher }).
For each File(Created(data)):
    if self.password.is_some():
        replace with File(Loaded { data, cipher: Some(CipherConfig {
            encryption: Encryption::Aes,
            cipher_mode: CipherMode::CTR,
        }) })
    else:
        replace with File(Loaded { data, cipher: None })
Set self.dirty = false.
```

**`set_times()` with `TimeOrNow`:**

```rust
match atime {
    Some(TimeOrNow::SpecificTime(t)) => node.attr.atime = t,
    Some(TimeOrNow::Now)             => node.attr.atime = SystemTime::now(),
    None                             => { /* unchanged */ }
}
// same for mtime
```

### Error codes

| Operation | Condition | Error |
|---|---|---|
| `create_file` / `make_dir` | name already exists in parent | `EEXIST` |
| `unlink` | name not found | `ENOENT` |
| `unlink` | name is a directory | `EPERM` (macOS) / `EISDIR` (Linux) |
| `rmdir` | directory is not empty | `ENOTEMPTY` |
| Any write op | `PnaFS` is read-only (`Option<WriteStrategy>` is `None`) | `EROFS` |
| `set_size` / `write_file` | inode is a directory | `EISDIR` |

### nlink maintenance

- `make_dir(parent, name)`: new directory gets `nlink = 2`; `parent.attr.nlink` incremented by 1
- `unlink(parent, name)` on a file: node is removed entirely from all data structures
- `rmdir(parent, name)` (phase 2): `parent.attr.nlink` decremented by 1; node removed

## FUSE Operation Mapping (Phase 1)

| FUSE method | ArchiveStore call | Notes |
|---|---|---|
| `create()` | see flags handling below | O_CREAT on existing file supported |
| `write()` | `write_file(ino, offset, data)` | offset is `u64` |
| `setattr(size=Some(n))` | `set_size(ino, n)` | Phase 1 truncate path |
| `setattr(atime/mtime)` | `set_times(ino, atime, mtime)` | `Option<TimeOrNow>` |
| `setattr(mode/uid/gid)` | `set_attr_full(...)` stub → `ENOSYS` | Phase 2 |
| `mkdir()` | `make_dir(parent, name, mode, umask)` | umask applied: `mode & !umask` |
| `unlink()` | `unlink(parent, name)` | Files only (phase 1); fully removes node |
| `open()` | existence check; `EROFS` if not read-only access | Use `flags.acc_mode()` |
| `release()` | flush if `Immediate` and `is_dirty()`; `mark_clean()` on success | `flags: OpenFlags` |
| `destroy()` | flush if `Lazy` and `is_dirty()`; `mark_clean()` on success | `get_mut()`, no lock |
| `fsync()` | flush if `write_strategy.is_some()` and `is_dirty()`; `mark_clean()` on success | guard on write mode |

Existing read operations (`lookup`, `getattr`, `read`, `readdir`, `getxattr`, `listxattr`, `flush`) delegate to `ArchiveStore` read methods unchanged.

### `open()` write-mode check

fuser 0.17.0 passes `flags: OpenFlags` to `open()`. Use `OpenFlags::acc_mode()` and `OpenAccMode` to check for write access:

```rust
fn open(&self, _req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
    if self.write_strategy.is_none() && flags.acc_mode() != OpenAccMode::O_RDONLY {
        reply.error(Errno::EROFS);
        return;
    }
    // …
}
```

**O_TRUNC note:** The FUSE protocol and fuser 0.17.0 strip `O_TRUNC` from `open()` flags before dispatch — the kernel converts it to a `setattr(size=0)` call. No O_TRUNC handling is needed in `open()`.

### `create()` flags handling

fuser 0.17.0 passes `flags: i32` to `create()` (raw `libc` flags, not `OpenFlags`). The FUSE layer inspects flags using bitwise operations:

**Case 1 — name does not exist in parent:**
Call `create_file(parent, name, mode)`. Normal creation path.

**Case 2 — name already exists in parent (O_CREAT on existing file):**
- If `(flags & libc::O_EXCL) != 0` → `reply.error(Errno::EEXIST)`, return.
- Otherwise → look up the existing inode. If `(flags & libc::O_TRUNC) != 0`, call `set_size(ino, 0)`. Return the existing node via `reply.created(...)`. Do **not** call `create_file()`.

**Guard — parent must be a directory:** Before Case 1 or Case 2, if the `parent` inode exists but is not a directory, return `reply.error(Errno::ENOTDIR)`.

## archive_io Layer

### load — Solid Archive Handling and Stale Temp Cleanup

**Solid archive detection:** The pna crate exposes `pna::ReadEntry<T>` which distinguishes:

```rust
pub enum ReadEntry<T> {
    Normal(NormalEntry<T>),
    Solid(SolidEntry<T>),
}
```

At load time:
- `ReadEntry::Normal(entry)` → store as `FileContent::Unloaded(entry, opts)` (lazy load).
- `ReadEntry::Solid(solid)` → iterate entries from the solid block; for each, immediately decode into `FileContent::Loaded { data, cipher }`. Note: `NormalEntry<Vec<u8>>` extracted from a solid block does hold its data in owned `Vec<u8>` chunks, so technically it could be stored as `Unloaded`. The force-load is a deliberate design choice: it simplifies the save path (no special-casing needed to identify solid-derived entries during save) at the cost of higher memory use for solid archives at mount time.

**Stale temp file cleanup:** Before reading the archive, scan the archive's parent directory for files matching `.{filename}.tmp.*` and remove them (best-effort, ignore errors).

```
cleanup_stale_tmp(archive_path):
    dir  = archive_path.parent().unwrap_or(Path::new("."))
    stem = archive_path.file_name().unwrap()
    for entry in dir.read_dir() that matches ".{stem}.tmp.*":
        fs::remove_file(entry.path())  // ignore errors
```

### save — Atomic Write Sequence

```
0. cleanup_stale_tmp(archive_path)   ← remove orphaned temps from prior crashes

1. Guard: walk all nodes; if any File(Loaded { cipher: Some(_) })
          or File(Modified { cipher: Some(_) }) and store.password().is_none():
       return Err(io::Error::new(
           io::ErrorKind::InvalidInput,
           "cannot re-encrypt: archive requires password but none was provided"))
   This check runs BEFORE any WriteOptions are built to avoid runtime panics.

2. Determine temp file path:
       dir = archive_path.parent().unwrap_or(Path::new("."))
       tmp = dir.join(format!(".{}.tmp.{}", filename, process::id()))

3. Walk all nodes in pre-order DFS (parent directory entries written before children).
   Skip the root inode (inode 1) — it is synthetic and has no archive entry.
   For each non-root node, write a PNA entry with metadata derived from node.attr
   and node.xattrs:

   Directory        → directory entry (with Metadata from node.attr + node.xattrs)

   File(Unloaded)   → entry.clone().with_metadata(meta)    ← NormalEntry is Clone;
                       then add_entry.                         clone needed because
                       Metadata is derived from node.attr.    save() takes &ArchiveStore.

                       Constructing `meta`: build a `Metadata` from node.attr using
                       the pna crate's builder (e.g., `Metadata::new().with_modified(...)`).
                       Timestamps must be converted from SystemTime to pna::Duration:
                         node.attr.mtime
                             .duration_since(UNIX_EPOCH)
                             .map(|d| pna::Duration::seconds(d.as_secs() as i64))
                       Note: `with_metadata()` preserves raw_file_size and
                       compressed_size from the original entry, so only atime/mtime
                       and permission fields need to be carried in `meta`.

                       For Phase 2 (xattr mutations possible):
                         entry.clone().with_metadata(meta).with_xattrs(converted_xattrs)
                       where converted_xattrs maps node.xattrs OsString keys to String
                       (lossy or error-on-non-UTF8 as appropriate).
                       Phase 1: xattr mutations return ENOSYS, so with_metadata alone
                       is sufficient.

   File(Loaded)     → write bytes with WriteOptions derived from cipher:
                       None    → no encryption, default compression
                       Some(c) → c.encryption / c.cipher_mode /
                                 HashAlgorithm::argon2id() + password

   File(Modified)   → same as Loaded but with modified data

   File(Created)    → if password.is_some():
                           (Encryption::Aes, CipherMode::CTR,
                            HashAlgorithm::argon2id()) + password
                       else: no encryption

   Symlink          → phase 2; not included in phase 1 save

4. fsync temp file
5. rename(tmp, archive_path)   ← atomic replacement
6. store.mark_clean()          ← reset dirty flag, transition Modified/Created → Loaded
```

**HashAlgorithm:** not stored in `CipherConfig`; all re-encryption uses `argon2id()`.

**CipherMode enum:** use `CipherMode::CTR` (all caps); `CipherMode::Ctr` does not exist.

### WriteStrategy and Flush Triggers

```rust
// WriteStrategy is defined once (e.g., in filesystem.rs or a shared module)
// with all three derives. clap::ValueEnum is placed here rather than in cli.rs
// so that filesystem.rs does not depend on cli.rs.
#[derive(clap::ValueEnum, Clone, PartialEq)]
pub(crate) enum WriteStrategy {
    Lazy,       // flush on unmount (default)
    Immediate,  // flush on every file close (release)
}
```

**Lazy:** flush happens only in `destroy()`.

**Immediate:** flush happens in `release()` when `is_dirty()` is true.

```rust
// PnaFS::destroy() — fuser 0.17.0 signature: fn destroy(&mut self)
fn destroy(&mut self) {
    let store = self.store.get_mut().unwrap();
    if store.is_dirty() {
        match archive_io::save(store) {
            Ok(()) => store.mark_clean(),
            Err(e) => log::error!("failed to flush archive on unmount: {e}"),
        }
    }
}

// PnaFS::release() — fuser 0.17.0 signature: flags: OpenFlags
fn release(&self, _req: &Request, _ino: INodeNo, _fh: FileHandle,
           _flags: OpenFlags, _lock_owner: Option<LockOwner>,
           _flush: bool, reply: ReplyEmpty) {
    // _flush is the kernel page-cache flush flag; it does NOT mean "persist archive".
    // Archive persistence is controlled solely by write_strategy.
    if matches!(self.write_strategy, Some(WriteStrategy::Immediate)) {
        let mut store = self.store.lock().unwrap();
        // Mutex held for duration of save. Concurrent FUSE reads block. OK for Phase 1.
        if store.is_dirty() {
            match archive_io::save(&*store) {
                Ok(()) => store.mark_clean(),
                Err(e) => {
                    log::error!("failed to flush archive on release: {e}");
                    reply.error(Errno::EIO);
                    return;
                }
            }
        }
    }
    reply.ok();
}

// PnaFS::fsync() — only flushes in write mode
fn fsync(&self, _req: &Request, _ino: INodeNo, _fh: FileHandle,
         _datasync: bool, reply: ReplyEmpty) {
    if self.write_strategy.is_some() {
        let mut store = self.store.lock().unwrap();
        if store.is_dirty() {
            match archive_io::save(&*store) {
                Ok(()) => store.mark_clean(),
                Err(e) => {
                    log::error!("failed to fsync archive: {e}");
                    reply.error(Errno::EIO);
                    return;
                }
            }
        }
    }
    reply.ok();
}
```

`destroy` takes `&mut self` in fuser 0.17.0 — use `get_mut()` to avoid unnecessary locking.

`archive_io::save(store)` takes `&ArchiveStore`; path and password are read from `store.archive_path()` and `store.password()` internally.

## CLI Changes

### New flags

```
pnafs mount [OPTIONS] <ARCHIVE> <MOUNTPOINT>

  --write                      Enable write mode (default: read-only)
  --write-strategy <STRATEGY>  When to flush changes back to archive
                               [default: lazy] [requires: --write]
                               [possible values: lazy, immediate]
```

`--write-strategy` without `--write` is rejected by clap (`requires = "write"`).

### MountOptions struct

```rust
#[derive(Args)]
struct MountOptions {
    #[arg(long)]
    allow_root: bool,
    #[arg(long)]
    allow_other: bool,
    #[arg(long)]
    write: bool,
    #[arg(long, default_value = "lazy", requires = "write")]
    write_strategy: WriteStrategy,
}

#[derive(clap::ValueEnum, Clone, PartialEq)]
pub(crate) enum WriteStrategy {
    Lazy,
    Immediate,
}
```

Mapping to `PnaFS`:

```rust
let write_strategy = if mount_options.write {
    Some(mount_options.write_strategy)
} else {
    None
};
// PnaFS holds Option<WriteStrategy>; None = read-only
// MountOption::RO is added only when write_strategy.is_none()
```

## File Layout

```
src/
├── main.rs
├── cli.rs
├── command.rs
├── archive_store.rs      ← replaces file_manager.rs
├── archive_io.rs         ← new: load/save PNA ↔ ArchiveStore
├── filesystem.rs         ← updated: ArchiveStore + write FUSE ops
└── command/
    ├── mount.rs          ← updated: --write / --write-strategy flags
    ├── complete.rs
    └── bugreport.rs
```

`file_manager.rs` is removed. `archive_store.rs` and `archive_io.rs` replace it with clear separation of concerns.

## Phase Boundaries

| Phase | Scope | FUSE ops added |
|---|---|---|
| 1 | Minimum write set | `create`, `write`, `open`, `release`, `fsync`, `mkdir`, `unlink`, `setattr(size, atime, mtime)` |
| 2 | Full set | `rmdir`, `rename`, `setattr(mode, uid, gid)`, `symlink`, `link`, `setxattr`, `removexattr`, `readlink` |

Phase 2 methods on `ArchiveStore` are defined in phase 1 but return `Err(Errno::ENOSYS)` until implemented. The corresponding FUSE methods in `PnaFS` return `reply.error(Errno::ENOSYS)`.
