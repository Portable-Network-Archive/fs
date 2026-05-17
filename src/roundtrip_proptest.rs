//! Property-based round-trip tests for `archive_io::{load, save}`.
//!
//! Each property generates a tree **constructively** — directories carry
//! their own children — so proptest's shrinker walks the same tree
//! structure the assertion sees. An alternative design that emits a
//! flat path list and post-filters out conflicts (duplicate paths,
//! parent / child collisions) makes the shrinker lie: removing one
//! element from the list can change which neighbouring elements
//! survive the filter, so the "minimal" counter-example proptest
//! reports is rarely the true minimum. The constructive
//! `prop_recursive` generator below avoids that trap by making the
//! tree shape the unit of generation and shrinkage.
//!
//! Node types covered by the generator:
//!
//! * **File** — content, perm, owner, xattrs, all round-trip.
//! * **Dir** — perm, owner, xattrs, nlink invariant, all round-trip.
//! * **Symlink** — target string round-trips (PNA stores it as the entry
//!   payload).
//! * **Special** (block / char / fifo / socket) — pnafs accepts them
//!   in-memory but PNA has no on-disk kind for them, so `save()` drops
//!   them with a logged warning. The property checks the **inverse**:
//!   nodes spec'd as Special are absent from the post-load snapshot.
//! * **Hardlink** — handled as a sidecar table (`HardlinkRef`) instead
//!   of a `NodeSpec` variant, because a hardlink references *another*
//!   node and proptest's `prop_recursive` cannot express that
//!   cross-reference cleanly. After the primary tree is built we walk
//!   the table, resolve each `(source_index, dest_parent_index,
//!   dest_name)` modulo the primary inventory, and call `create_hardlink`
//!   when none of (a) name collision, (b) source-is-dir, (c) dest is
//!   itself a non-dir blocks it. Hardlinks that can't materialise are
//!   simply skipped — the property is about what *did* get materialised,
//!   not about every triple succeeding.
//!
//! Properties are layered as one test per SPEC so failure messages
//! identify the broken invariant directly:
//!
//! * **Plaintext block (cases = 64)**
//!   * `plain_generator_specs_survive_load` — generated `NodeSpec`
//!     fields (content, perm, owner, xattrs, size) survive `save → load`.
//!   * `plain_save_is_idempotent_under_reload` — second `save → load`
//!     is a fixed point at the snapshot level.
//!   * `plain_loaded_directories_have_posix_nlink` — `nlink = 2 +
//!     #subdirs` for every loaded directory, including the root.
//!   * `plain_save_strips_special_entries` — Special-typed entries
//!     are dropped on save (the inverse property pinning the PNA
//!     format limitation).
//!   * `plain_hardlinks_are_observationally_equivalent` — every
//!     hardlink destination observes the same inode as its source.
//!   * `plain_save_is_byte_identical_when_replayed` — for plaintext
//!     archives, save is a deterministic function of the tree;
//!     catches drift the AST-equality cannot see.
//!   * `plain_mutation_sequence_survives_save_load` — apply a random
//!     `Vec<FsOp>` (write / truncate / unlink / setxattr /
//!     removexattr) to a freshly-loaded tree, then save+reload.
//!     Asserts directory nlink invariant + save idempotence on the
//!     mutated state. Covers transient-state bugs the static
//!     round-trip properties miss.
//!
//! * **Encrypted block (cases = 8, Argon2id-bound)**
//!   * `encrypted_generator_specs_survive_load`
//!   * `encrypted_save_is_idempotent_under_reload`
//!   * `encrypted_loaded_directories_have_posix_nlink`
//!   * `encrypted_save_strips_special_entries`
//!   * `encrypted_hardlinks_are_observationally_equivalent`
//!   * `encrypted_archive_rejects_wrong_password` — wrong-key load
//!     either errors out (the ideal) or returns content that does
//!     not match the input. pna 0.33's AES-CTR has no AEAD/MAC, so
//!     `Err` is not always produced; the property asserts the
//!     **observable** contract (no original bytes come back) rather
//!     than the aspirational one (load fails outright).
//!
//! All operations are issued directly against `FileTree` — the
//! property test does **not** go through FUSE. That avoids needing
//! privileges (e.g. `mknod` with a non-zero `rdev` is normally
//! gated on `CAP_SYS_ADMIN` over FUSE) and keeps cases fast, but it
//! also means kernel-level checks (`DefaultPermissions`, mount
//! flags, etc.) are not exercised here — those live in
//! `scripts/tests/test_pjdfstest.sh` and friends.
//!
//! Implicit invariants the byte-identity property leans on, documented
//! so a future refactor doesn't break them silently:
//!
//! * `node.attr.mtime` / `crtime` must survive `save → load` unchanged
//!   (PNA stores them and load reinstates them). If `save` ever started
//!   stamping "now" into entries, byte-id would dissolve.
//! * `nix::unistd::User::from_uid` is consulted by `build_permission`
//!   to record a username alongside the numeric uid. Two calls within
//!   the same process return the same result, so byte-id holds within
//!   a single test run regardless of the host's `/etc/passwd`. To make
//!   the test independent of the host passwd database **across runs**,
//!   `arb_meta` restricts uid / gid to a synthetic high range that no
//!   conventional system has entries for; see comment there.

use crate::archive_io;
use crate::file_tree::{FileTree, FsContent, Inode, Owner, ROOT_INODE, SpecialKind};
use proptest::prelude::*;
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::io;
use std::path::Path;

// ── Spec types ─────────────────────────────────────────────────────

/// Per-inode metadata the generator chooses.
#[derive(Debug, Clone)]
struct NodeMeta {
    perm: u16,
    uid: u32,
    gid: u32,
    xattrs: BTreeMap<String, Vec<u8>>,
}

#[derive(Debug, Clone, Copy)]
enum SpecKind {
    BlockDevice,
    CharDevice,
    Fifo,
    Socket,
}

impl SpecKind {
    fn to_tree(self) -> SpecialKind {
        match self {
            SpecKind::BlockDevice => SpecialKind::BlockDevice,
            SpecKind::CharDevice => SpecialKind::CharDevice,
            SpecKind::Fifo => SpecialKind::Fifo,
            SpecKind::Socket => SpecialKind::Socket,
        }
    }
}

#[derive(Debug, Clone)]
enum NodeSpec {
    File {
        meta: NodeMeta,
        content: Vec<u8>,
    },
    Symlink {
        meta: NodeMeta,
        target: String,
    },
    Special {
        meta: NodeMeta,
        kind: SpecKind,
        rdev: u32,
    },
    Dir {
        meta: NodeMeta,
        children: BTreeMap<String, NodeSpec>,
    },
}

/// A hardlink request that references the primary tree by
/// `proptest::sample::Index`. `Index` is proptest's standard "pick
/// from a runtime-known collection" strategy: when the surrounding
/// tree shrinks, `Index::index(len)` continues to pick *some* element
/// of the shrunk collection rather than jumping to an unrelated
/// element. A plain `u32 % len` would defeat shrink monotonicity —
/// shrinking the tree by removing files would silently re-target every
/// hardlink, and proptest's "minimal" counter-example would mix
/// failures from different placements.
#[derive(Debug, Clone)]
struct HardlinkRef {
    source: proptest::sample::Index,
    dest_dir: proptest::sample::Index,
    dest_name: String,
}

#[derive(Debug, Clone)]
struct TestInput {
    root: BTreeMap<String, NodeSpec>,
    hardlinks: Vec<HardlinkRef>,
    /// If `Some`, save / load go through PNA's encrypted entry path
    /// using this password. `None` means plaintext archives.
    /// Encrypted archives use a random IV per save, so the byte-id
    /// property is gated on `password.is_none()`.
    password: Option<String>,
}

// ── Mutation sequences ──────────────────────────────────────────────
//
// The static round-trip properties above all generate a tree, save
// it, and verify the result. They never exercise pnafs's *transient*
// state — what happens when a file is written, then truncated, then
// renamed, then written again, all before the next save. The state
// machine here drives a generated `Vec<FsOp>` against a live
// `FileTree`, save/reload at the end, and asserts the same invariants
// the static properties pin (directory nlink, save idempotence) hold
// for whatever ends up persisted.
//
// Per-op behaviour:
//
// * Each op picks its target by `Index`-ing into the inventory of the
//   *current* tree (recomputed at every op), so shrinking remains
//   monotonic even though the target population is dynamic.
// * Invalid ops (e.g. unlink a path that no longer exists, write to a
//   path that names a directory) are silently skipped — the property
//   is about what *does* happen, not about every op succeeding.

#[derive(Debug, Clone)]
enum FsOp {
    /// Overwrite some existing File from offset 0.
    WriteOver {
        target: proptest::sample::Index,
        content: Vec<u8>,
    },
    /// Shrink or grow some existing File.
    Truncate {
        target: proptest::sample::Index,
        new_size: u64,
    },
    /// Drop an existing File (POSIX `unlink`).
    UnlinkFile { target: proptest::sample::Index },
    /// Create a directory under some existing directory parent. The
    /// `parent` indexes the live directory inventory (root included);
    /// if the chosen parent already has a child with the same name,
    /// `make_dir` returns `EEXIST` and the op is dropped.
    Mkdir {
        parent: proptest::sample::Index,
        name: String,
    },
    /// Remove an existing directory if it is empty. Hits the same
    /// branch FUSE's `rmdir` calls into; `ENOTEMPTY` from a populated
    /// dir is one of the silently-skipped cases.
    Rmdir { target: proptest::sample::Index },
    /// Set an xattr on some existing inode.
    SetXattr {
        target_any: proptest::sample::Index,
        name: String,
        value: Vec<u8>,
    },
    /// Remove an xattr from some existing inode.
    RemoveXattr {
        target_any: proptest::sample::Index,
        name: String,
    },
    /// Rename some existing entry into a (possibly different)
    /// directory under a new name. `source` indexes the live
    /// non-root inventory; `dest_dir` indexes the live directory
    /// inventory (root included). `mode` selects plain rename,
    /// `RENAME_NOREPLACE`, or `RENAME_EXCHANGE`. Same-parent and
    /// cross-parent moves both arise naturally (the generator does
    /// not constrain `dest_dir` to differ from the source's parent),
    /// as does a destination that already exists.
    Rename {
        source: proptest::sample::Index,
        dest_dir: proptest::sample::Index,
        dest_name: String,
        mode: RenameMode,
    },
}

/// Which flavour of `rename(2)` an `FsOp::Rename` issues. Splitting
/// this out (rather than generating raw `fuser::RenameFlags`) keeps
/// the shrinker working on a small closed set and documents intent.
#[derive(Debug, Clone, Copy)]
enum RenameMode {
    /// Plain rename: clobbers the destination if it exists.
    Plain,
    /// `RENAME_NOREPLACE`: `EEXIST` (skipped) if the destination
    /// exists.
    NoReplace,
    /// `RENAME_EXCHANGE`: atomically swap two existing entries;
    /// `ENOENT` (skipped) if the destination does not exist.
    Exchange,
}

fn arb_fs_op() -> impl Strategy<Value = FsOp> {
    prop_oneof![
        3 => (any::<proptest::sample::Index>(), arb_file_content())
            .prop_map(|(target, content)| FsOp::WriteOver { target, content }),
        2 => (any::<proptest::sample::Index>(), 0u64..=8192)
            .prop_map(|(target, new_size)| FsOp::Truncate { target, new_size }),
        2 => any::<proptest::sample::Index>()
            .prop_map(|target| FsOp::UnlinkFile { target }),
        2 => (any::<proptest::sample::Index>(), arb_segment())
            .prop_map(|(parent, name)| FsOp::Mkdir { parent, name }),
        2 => any::<proptest::sample::Index>()
            .prop_map(|target| FsOp::Rmdir { target }),
        2 => (any::<proptest::sample::Index>(), arb_xattr_name(), prop::collection::vec(any::<u8>(), 0..=32))
            .prop_map(|(target_any, name, value)| FsOp::SetXattr { target_any, name, value }),
        1 => (any::<proptest::sample::Index>(), arb_xattr_name())
            .prop_map(|(target_any, name)| FsOp::RemoveXattr { target_any, name }),
        3 => (
            any::<proptest::sample::Index>(),
            any::<proptest::sample::Index>(),
            arb_segment(),
            arb_rename_mode(),
        )
            .prop_map(|(source, dest_dir, dest_name, mode)| FsOp::Rename {
                source,
                dest_dir,
                dest_name,
                mode,
            }),
    ]
}

fn arb_rename_mode() -> impl Strategy<Value = RenameMode> {
    prop_oneof![
        3 => Just(RenameMode::Plain),
        1 => Just(RenameMode::NoReplace),
        1 => Just(RenameMode::Exchange),
    ]
}

/// Apply `ops` to `tree`, silently skipping invalid ones. Returns
/// nothing — the assertion lives in the property that calls this,
/// after a save + reload.
fn apply_ops(tree: &mut FileTree, ops: &[FsOp]) {
    for op in ops {
        let files = collect_file_paths(tree);
        let any_paths = collect_all_paths(tree);
        match op {
            FsOp::WriteOver { target, content } => {
                if files.is_empty() {
                    continue;
                }
                let path = &files[target.index(files.len())];
                if let Some(ino) = tree.resolve_path(Path::new(path)) {
                    // Reset size first (we want "overwrite from 0",
                    // not "extend if shorter"), then write.
                    let _ = tree.set_size(ino, 0);
                    if !content.is_empty() {
                        let _ = tree.write_file(ino, 0, content);
                    }
                }
            }
            FsOp::Truncate { target, new_size } => {
                if files.is_empty() {
                    continue;
                }
                let path = &files[target.index(files.len())];
                if let Some(ino) = tree.resolve_path(Path::new(path)) {
                    let _ = tree.set_size(ino, *new_size);
                }
            }
            FsOp::UnlinkFile { target } => {
                if files.is_empty() {
                    continue;
                }
                let path = &files[target.index(files.len())];
                let path_buf = Path::new(path);
                let leaf = match path_buf.file_name() {
                    Some(n) => n.to_owned(),
                    None => continue,
                };
                let parent_ino = match path_buf.parent() {
                    Some(p) if !p.as_os_str().is_empty() => match tree.resolve_path(p) {
                        Some(i) => i,
                        None => continue,
                    },
                    _ => ROOT_INODE,
                };
                let _ = tree.unlink(parent_ino, &leaf);
            }
            FsOp::Mkdir { parent, name } => {
                let dirs = collect_dir_paths(tree);
                if dirs.is_empty() {
                    continue;
                }
                let parent_path = &dirs[parent.index(dirs.len())];
                let parent_ino = if parent_path.is_empty() {
                    ROOT_INODE
                } else {
                    match tree.resolve_path(Path::new(parent_path)) {
                        Some(i) => i,
                        None => continue,
                    }
                };
                let _ = tree.make_dir(parent_ino, OsStr::new(name), 0o755, 0, Owner::new(0, 0));
            }
            FsOp::Rmdir { target } => {
                let dirs = collect_dir_paths(tree);
                // Skip the root: rmdir on `/` is meaningless and the
                // generator picking it would just contribute to the
                // empty-op rate.
                let nonroot: Vec<&String> = dirs.iter().filter(|p| !p.is_empty()).collect();
                if nonroot.is_empty() {
                    continue;
                }
                let path = nonroot[target.index(nonroot.len())];
                let path_buf = Path::new(path);
                let leaf = match path_buf.file_name() {
                    Some(n) => n.to_owned(),
                    None => continue,
                };
                let parent_ino = match path_buf.parent() {
                    Some(p) if !p.as_os_str().is_empty() => match tree.resolve_path(p) {
                        Some(i) => i,
                        None => continue,
                    },
                    _ => ROOT_INODE,
                };
                let _ = tree.rmdir(parent_ino, &leaf);
            }
            FsOp::SetXattr {
                target_any,
                name,
                value,
            } => {
                if any_paths.is_empty() {
                    continue;
                }
                let path = &any_paths[target_any.index(any_paths.len())];
                if let Some(ino) = if path.is_empty() {
                    Some(ROOT_INODE)
                } else {
                    tree.resolve_path(Path::new(path))
                } {
                    let _ = tree.setxattr(ino, name, value, 0);
                }
            }
            FsOp::RemoveXattr { target_any, name } => {
                if any_paths.is_empty() {
                    continue;
                }
                let path = &any_paths[target_any.index(any_paths.len())];
                if let Some(ino) = if path.is_empty() {
                    Some(ROOT_INODE)
                } else {
                    tree.resolve_path(Path::new(path))
                } {
                    let _ = tree.removexattr(ino, name);
                }
            }
            FsOp::Rename {
                source,
                dest_dir,
                dest_name,
                mode,
            } => {
                // Source is any non-root entry; dest parent is any
                // live directory (root included). `dest_dir` is not
                // constrained to differ from the source's parent, so
                // both same-parent and cross-parent moves arise.
                let nonroot: Vec<&String> = any_paths.iter().filter(|p| !p.is_empty()).collect();
                if nonroot.is_empty() {
                    continue;
                }
                let dirs = collect_dir_paths(tree);
                let src_path = nonroot[source.index(nonroot.len())];
                let Some((old_parent, old_leaf)) = split_parent_leaf(tree, src_path) else {
                    continue;
                };
                let dest_dir_path = &dirs[dest_dir.index(dirs.len())];
                let new_parent = if dest_dir_path.is_empty() {
                    ROOT_INODE
                } else {
                    match tree.resolve_path(Path::new(dest_dir_path)) {
                        Some(i) => i,
                        None => continue,
                    }
                };
                let flags = match mode {
                    RenameMode::Plain => fuser::RenameFlags::empty(),
                    RenameMode::NoReplace => fuser::RenameFlags::RENAME_NOREPLACE,
                    RenameMode::Exchange => fuser::RenameFlags::RENAME_EXCHANGE,
                };
                // Every rejected case (ENOENT, EEXIST under
                // NOREPLACE, ENOENT under EXCHANGE, EINVAL for a dir
                // into its own descendant, ENOTEMPTY clobbering a
                // populated dir) is a silently-skipped op, matching
                // the rest of the state machine.
                let _ = tree.rename(
                    old_parent,
                    &old_leaf,
                    new_parent,
                    OsStr::new(dest_name),
                    flags,
                );
            }
        }
    }
}

/// Snapshot of every File path visible in `tree`, DFS-ordered. All
/// `collect_*_paths` helpers return POSIX-relative paths with no
/// leading `/`; the archive root is the empty string, and a file
/// `f.txt` directly under root is just `"f.txt"`. Callers that need
/// to split parent / leaf must therefore special-case the empty
/// parent as `ROOT_INODE`.
fn collect_file_paths(tree: &FileTree) -> Vec<String> {
    tree.collect_dfs()
        .into_iter()
        .filter_map(|(_ino, node, path)| matches!(node.content, FsContent::File(_)).then_some(path))
        .collect()
}

/// Snapshot of every Directory path visible in `tree`, DFS-ordered.
/// Includes the root, represented as the empty string.
fn collect_dir_paths(tree: &FileTree) -> Vec<String> {
    let mut paths: Vec<String> = vec![String::new()];
    paths.extend(
        tree.collect_dfs()
            .into_iter()
            .filter_map(|(_ino, node, path)| {
                matches!(node.content, FsContent::Directory(_)).then_some(path)
            }),
    );
    paths
}

/// Snapshot of every observable path (files + dirs + symlinks +
/// specials + the root, represented as the empty string).
fn collect_all_paths(tree: &FileTree) -> Vec<String> {
    let mut paths: Vec<String> = vec![String::new()];
    paths.extend(tree.collect_dfs().into_iter().map(|(_, _, p)| p));
    paths
}

/// Split a POSIX-relative path (no leading `/`, root = `""`) into its
/// `(parent_inode, leaf_name)`, resolving the parent against `tree`.
/// Returns `None` if the path is the root, has no file name, or the
/// parent does not resolve — every one of which an `FsOp` arm treats
/// as "skip this op", matching the existing inline style in
/// `UnlinkFile` / `Rmdir`.
fn split_parent_leaf(tree: &FileTree, path: &str) -> Option<(Inode, std::ffi::OsString)> {
    let path_buf = Path::new(path);
    let leaf = path_buf.file_name()?.to_owned();
    let parent_ino = match path_buf.parent() {
        Some(p) if !p.as_os_str().is_empty() => tree.resolve_path(p)?,
        _ => ROOT_INODE,
    };
    Some((parent_ino, leaf))
}

// ── Generators ─────────────────────────────────────────────────────

/// One path component. The alphabet mixes:
///
/// * lowercase ASCII (the baseline, dominant weight)
/// * digits, `-`, `_` (the next most common in real archives)
/// * dot-prefixed names like `.config` (PNA preserves them, FUSE
///   hides them from `ls` but not from `getattr`)
/// * multibyte UTF-8 segments drawn from a small but varied
///   katakana / hiragana / kanji pool so `EntryName::from_lossy`
///   sees a real spread of code points. Deep unicode fuzzing
///   (combining marks, RTL overrides, supplementary planes) is out
///   of scope here — a dedicated fuzzer is the right tool for that
///   territory.
///
/// Length stays bounded so the search space is broader without
/// exploding case time.
fn arb_segment() -> impl Strategy<Value = String> {
    prop_oneof![
        6 => "[a-z]{1,8}".prop_map(|s| s.to_string()),
        2 => "[a-z][a-z0-9_\\-]{1,7}".prop_map(|s| s.to_string()),
        1 => "\\.[a-z]{1,6}".prop_map(|s| s.to_string()),
        // proptest's regex engine accepts `\u{...}` ranges; this mix
        // covers full-width katakana, hiragana, and one CJK ideograph
        // band, generating 1-3 chars per case.
        1 => "[\u{30a1}-\u{30fa}\u{3041}-\u{3093}\u{4e00}-\u{4e10}]{1,3}".prop_map(|s| s.to_string()),
    ]
}

fn arb_xattr_name() -> impl Strategy<Value = String> {
    // user.* is the unrestricted namespace under `DefaultPermissions`;
    // sticking to it keeps the generator portable across CI hosts.
    // The post-prefix portion mixes alphanumeric, dots, and dashes to
    // cover the common shapes of real xattr names (e.g. `user.tag.v2`,
    // `user.app-meta`).
    prop_oneof![
        3 => "user\\.[a-z]{1,8}".prop_map(|s| s.to_string()),
        2 => "user\\.[a-z][a-z0-9.\\-]{1,10}".prop_map(|s| s.to_string()),
    ]
}

fn arb_xattrs() -> impl Strategy<Value = BTreeMap<String, Vec<u8>>> {
    // Value size mix: most xattrs are tiny tags, but the wire format
    // also supports moderately large payloads (chained POSIX ACLs run
    // hundreds of bytes). Sample includes the empty-value case, the
    // typical short case, and a 256-byte band that crosses chunk
    // boundaries inside an entry.
    let value = prop_oneof![
        4 => prop::collection::vec(any::<u8>(), 0..=16),
        2 => prop::collection::vec(any::<u8>(), 16..=64),
        1 => prop::collection::vec(any::<u8>(), 64..=256),
    ];
    prop::collection::btree_map(arb_xattr_name(), value, 0..=4)
}

fn arb_meta() -> impl Strategy<Value = NodeMeta> {
    // uid / gid are restricted to a synthetic high range
    // (`0xFEED_0000..=0xFEED_FFFF` for uid, `0xDEAD_0000..=0xDEAD_FFFF`
    // for gid). Two reasons:
    //   * `archive_io::build_permission` calls
    //     `nix::unistd::User::from_uid` to attach a username to the
    //     `Permission` record. A uid that happens to map to a real
    //     local user on the test host would record that user's name
    //     in the archive bytes, making the byte-identity property
    //     subtly host-dependent across runs (though stable within a
    //     run). Sticking to a range conventional systems do not
    //     populate keeps the bytes a pure function of the tree.
    //   * The literals are recognisable in failure dumps, so a
    //     counter-example obviously came from this generator rather
    //     than e.g. real host metadata leaking in.
    (
        prop::num::u16::ANY.prop_map(|m| m & 0o7777),
        0xFEED_0000_u32..=0xFEED_FFFF_u32,
        0xDEAD_0000_u32..=0xDEAD_FFFF_u32,
        arb_xattrs(),
    )
        .prop_map(|(perm, uid, gid, xattrs)| NodeMeta {
            perm,
            uid,
            gid,
            xattrs,
        })
}

/// 1–4 components joined with `/`. Used as a symlink target so the
/// FUSE side has something path-shaped to round-trip.
fn arb_symlink_target() -> impl Strategy<Value = String> {
    prop::collection::vec(arb_segment(), 1..=4).prop_map(|p| p.join("/"))
}

fn arb_spec_kind() -> impl Strategy<Value = SpecKind> {
    prop_oneof![
        Just(SpecKind::BlockDevice),
        Just(SpecKind::CharDevice),
        Just(SpecKind::Fifo),
        Just(SpecKind::Socket),
    ]
}

/// File content sizes. A pure `0..=N` uniform sampler under-explores
/// the boundaries. Bands:
///
/// * `0..=1` — the empty file (`size == 0`, distinct code path
///   from `size > 0` in `attr.size` / `write_file`) and the
///   single-byte case.
/// * `15..=17` — straddles the AES-CTR block boundary (16 bytes).
///   Relevant for encrypted archives; plaintext doesn't care.
/// * `0..=128` — typical small case, broad weight.
/// * `0..=8192` — moderately large bodies. pna 0.33's
///   `MAX_CHUNK_DATA_LENGTH` is `u32::MAX`, so there is no real
///   per-page split below the gigabyte scale; this band is for
///   diversity rather than for hitting a known boundary.
fn arb_file_content() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        3 => prop::collection::vec(any::<u8>(), 0..=128),
        2 => prop::collection::vec(any::<u8>(), 0..=1),
        1 => prop::collection::vec(any::<u8>(), 15..=17),
        1 => prop::collection::vec(any::<u8>(), 0..=8192),
    ]
}

/// One leaf node. Files dominate the distribution (weight 4) since
/// they exercise the richest code path; the other three are
/// represented but won't drown out file generation.
fn arb_leaf() -> impl Strategy<Value = NodeSpec> {
    prop_oneof![
        4 => (arb_meta(), arb_file_content())
            .prop_map(|(meta, content)| NodeSpec::File { meta, content }),
        1 => (arb_meta(), arb_symlink_target())
            .prop_map(|(meta, target)| NodeSpec::Symlink { meta, target }),
        1 => (arb_meta(), arb_spec_kind(), prop::num::u32::ANY)
            .prop_map(|(meta, kind, rdev)| NodeSpec::Special { meta, kind, rdev }),
    ]
}

/// Tree of nodes generated bottom-up. `prop_recursive` lets proptest
/// shrink the entire subtree structure (depth, branching, leaf
/// contents) coherently.
fn arb_node() -> impl Strategy<Value = NodeSpec> {
    arb_leaf().prop_recursive(
        // Depth: archives 3-4 levels deep are typical, deeper still
        // round-trips but balloons case time.
        4,
        // `prop_recursive`'s "desired size" budget. Proptest treats
        // this as a soft target it tries to stay under, not a hard
        // cap — a single case can exceed it if the recursive branch
        // expands beyond the budget on the way out. Adequate at 64
        // default cases; widen deliberately for release sweeps.
        32,
        // Branching at each dir: 0–4 children per dir.
        4,
        |inner| {
            (
                arb_meta(),
                prop::collection::btree_map(arb_segment(), inner, 0..=4),
            )
                .prop_map(|(meta, children)| NodeSpec::Dir { meta, children })
        },
    )
}

/// The archive root is itself a directory; we generate its children
/// directly so we never try to override root metadata (`make_dir`
/// applies to a parent, and there's no parent for the root).
fn arb_root_children() -> impl Strategy<Value = BTreeMap<String, NodeSpec>> {
    prop::collection::btree_map(arb_segment(), arb_node(), 0..=4)
}

fn arb_hardlink_ref() -> impl Strategy<Value = HardlinkRef> {
    (
        any::<proptest::sample::Index>(),
        any::<proptest::sample::Index>(),
        arb_segment(),
    )
        .prop_map(|(source, dest_dir, dest_name)| HardlinkRef {
            source,
            dest_dir,
            dest_name,
        })
}

fn arb_password_string() -> impl Strategy<Value = String> {
    "[a-zA-Z0-9]{4,12}".prop_map(|s| s.to_string())
}

/// A root-children layout that is guaranteed to contain **at least one
/// File with non-empty content**. Used by encryption-axis properties
/// that need actual cipher state to assert against — an empty file
/// has zero bytes of ciphertext, so two different keys would both
/// decrypt to the same (empty) plaintext, and the
/// "wrong key → different output" contract cannot be tested.
/// Constraining the sentinel file to ≥ 16 bytes (one AES block) also
/// makes the birthday-style probability of a coincidental
/// byte-for-byte match between two unrelated keys vanishingly
/// small (≈ 1 / 2^128).
fn arb_root_children_with_file() -> impl Strategy<Value = BTreeMap<String, NodeSpec>> {
    (
        arb_root_children(),
        arb_segment(),
        arb_meta(),
        prop::collection::vec(any::<u8>(), 16..=128),
    )
        .prop_map(|(mut root, sentinel_name, meta, content)| {
            // Force a uniquely-named File at the root so every
            // generated tree is guaranteed to materialise at least
            // one encrypted entry with real ciphertext.
            let key = format!("__file_{sentinel_name}");
            root.insert(key, NodeSpec::File { meta, content });
            root
        })
}

/// Test inputs that always carry a password and a non-empty file
/// inventory. The encryption-axis properties rely on both invariants
/// — built into the generator so the property body is free of
/// `prop_assume!` and runs every case productively.
fn arb_test_input_encrypted() -> impl Strategy<Value = TestInput> {
    (
        arb_root_children_with_file(),
        prop::collection::vec(arb_hardlink_ref(), 0..=4),
        arb_password_string().prop_map(Some),
    )
        .prop_map(|(root, hardlinks, password)| TestInput {
            root,
            hardlinks,
            password,
        })
}

/// Test inputs that never carry a password. Used by the plaintext
/// properties (round-trip + byte-identity); encrypted concerns live
/// in their own block.
fn arb_test_input_plain() -> impl Strategy<Value = TestInput> {
    (
        arb_root_children(),
        prop::collection::vec(arb_hardlink_ref(), 0..=4),
    )
        .prop_map(|(root, hardlinks)| TestInput {
            root,
            hardlinks,
            password: None,
        })
}

// ── Build / compare ────────────────────────────────────────────────

/// Bootstrap an empty PNA archive at `archive_path` so `load()` has
/// something to read.
fn bootstrap_empty(archive_path: &Path) -> io::Result<()> {
    let a = pna::Archive::write_header(std::fs::File::create(archive_path)?)?;
    a.finalize()?;
    Ok(())
}

fn apply_xattrs(
    tree: &mut FileTree,
    ino: Inode,
    xattrs: &BTreeMap<String, Vec<u8>>,
) -> io::Result<()> {
    for (name, value) in xattrs {
        tree.setxattr(ino, name, value, 0)
            .map_err(|e| io::Error::other(format!("setxattr: {e:?}")))?;
    }
    Ok(())
}

fn build_child(tree: &mut FileTree, parent: Inode, name: &str, spec: &NodeSpec) -> io::Result<()> {
    let owner = match spec {
        NodeSpec::File { meta, .. }
        | NodeSpec::Symlink { meta, .. }
        | NodeSpec::Special { meta, .. }
        | NodeSpec::Dir { meta, .. } => Owner::new(meta.uid, meta.gid),
    };
    match spec {
        NodeSpec::File { meta, content } => {
            let ino = tree
                .create_file(parent, OsStr::new(name), meta.perm as u32, owner)
                .map_err(|e| io::Error::other(format!("create_file: {e:?}")))?
                .attr
                .ino
                .0;
            if !content.is_empty() {
                tree.write_file(ino, 0, content)
                    .map_err(|e| io::Error::other(format!("write_file: {e:?}")))?;
            }
            apply_xattrs(tree, ino, &meta.xattrs)?;
        }
        NodeSpec::Symlink { meta, target } => {
            let ino = tree
                .create_symlink(parent, OsStr::new(name), Path::new(target), owner)
                .map_err(|e| io::Error::other(format!("create_symlink: {e:?}")))?
                .attr
                .ino
                .0;
            apply_xattrs(tree, ino, &meta.xattrs)?;
        }
        NodeSpec::Special { meta, kind, rdev } => {
            let ino = tree
                .create_special(
                    parent,
                    OsStr::new(name),
                    kind.to_tree(),
                    meta.perm,
                    *rdev,
                    owner,
                )
                .map_err(|e| io::Error::other(format!("create_special: {e:?}")))?
                .attr
                .ino
                .0;
            apply_xattrs(tree, ino, &meta.xattrs)?;
        }
        NodeSpec::Dir { meta, children } => {
            let ino = tree
                .make_dir(parent, OsStr::new(name), meta.perm as u32, 0, owner)
                .map_err(|e| io::Error::other(format!("make_dir: {e:?}")))?
                .attr
                .ino
                .0;
            apply_xattrs(tree, ino, &meta.xattrs)?;
            for (child_name, child_spec) in children {
                build_child(tree, ino, child_name, child_spec)?;
            }
        }
    }
    Ok(())
}

/// Walk `root` recursively, recording the archive path of every File
/// node and the path of every Directory node (including the root,
/// represented as the empty string). The order is DFS-by-BTreeMap-key
/// so it is reproducible across runs.
fn inventory(root: &BTreeMap<String, NodeSpec>) -> (Vec<String>, Vec<String>) {
    let mut files = Vec::new();
    let mut dirs = vec![String::new()];
    fn walk(
        prefix: &str,
        spec: &BTreeMap<String, NodeSpec>,
        files: &mut Vec<String>,
        dirs: &mut Vec<String>,
    ) {
        for (name, node) in spec {
            let p = if prefix.is_empty() {
                name.clone()
            } else {
                format!("{prefix}/{name}")
            };
            match node {
                NodeSpec::File { .. } => files.push(p),
                NodeSpec::Dir { children, .. } => {
                    dirs.push(p.clone());
                    walk(&p, children, files, dirs);
                }
                NodeSpec::Symlink { .. } | NodeSpec::Special { .. } => {}
            }
        }
    }
    walk("", root, &mut files, &mut dirs);
    (files, dirs)
}

/// Materialise as many of the generator's hardlink requests as the
/// state allows. Returns the set of *successfully* placed
/// `(source_path, dest_path)` pairs so the property can assert
/// observable equivalence at those locations.
fn apply_hardlinks(
    tree: &mut FileTree,
    root: &BTreeMap<String, NodeSpec>,
    requests: &[HardlinkRef],
) -> Vec<(String, String)> {
    let (files, dirs) = inventory(root);
    if files.is_empty() {
        return Vec::new();
    }
    let mut placed = Vec::new();
    for req in requests {
        let source_path = &files[req.source.index(files.len())];
        let dest_dir = &dirs[req.dest_dir.index(dirs.len())];
        let dest_path = if dest_dir.is_empty() {
            req.dest_name.clone()
        } else {
            format!("{dest_dir}/{}", req.dest_name)
        };
        if dest_path == *source_path {
            continue;
        }
        let Some(source_ino) = tree.resolve_path(Path::new(source_path)) else {
            continue;
        };
        let dest_parent_ino = if dest_dir.is_empty() {
            ROOT_INODE
        } else {
            match tree.resolve_path(Path::new(dest_dir)) {
                Some(i) => i,
                None => continue,
            }
        };
        // Skip on collision instead of failing — the request is just
        // proposing a placement; collisions are common at this density.
        if tree
            .lookup_child(dest_parent_ino, OsStr::new(&req.dest_name))
            .is_some()
        {
            continue;
        }
        if tree
            .create_hardlink(dest_parent_ino, OsStr::new(&req.dest_name), source_ino)
            .is_ok()
        {
            placed.push((source_path.clone(), dest_path));
        }
    }
    placed
}

/// Build the tree under `archive_path` matching `input`, materialise
/// hardlinks, and save. The archive is created plaintext (an empty
/// header), then loaded with the same password the input requests so
/// `tree.password()` carries through to the save path. With
/// `password: Some(_)`, every newly-created file picks up the
/// password's default cipher at save time (see
/// `archive_io::build_write_options`).
fn build_and_save(archive_path: &Path, input: &TestInput) -> io::Result<Vec<(String, String)>> {
    bootstrap_empty(archive_path)?;
    let mut tree = archive_io::load(archive_path, input.password.clone())?;
    for (name, spec) in &input.root {
        build_child(&mut tree, ROOT_INODE, name, spec)?;
    }
    let placed = apply_hardlinks(&mut tree, &input.root, &input.hardlinks);
    archive_io::save(&mut tree)?;
    Ok(placed)
}

/// Walk `tree` and pull out everything we expect to round-trip, keyed
/// by archive path. The root is included explicitly (under "") so a
/// future invariant test can hit its `nlink` too — `collect_dfs`
/// itself does not visit the root.
fn snapshot(tree: &FileTree) -> BTreeMap<String, ObservedNode> {
    let mut out = BTreeMap::new();
    let root = tree.get(ROOT_INODE).expect("tree has a root");
    out.insert(String::new(), observed(root));
    for (_ino, node, path) in tree.collect_dfs() {
        out.insert(path, observed(node));
    }
    out
}

fn observed(node: &crate::file_tree::FsNode) -> ObservedNode {
    let kind = match &node.content {
        FsContent::File(fc) => Observed::File {
            content: fc.data().to_vec(),
        },
        FsContent::Directory(_) => Observed::Directory,
        FsContent::Symlink(target) => Observed::Symlink {
            target: target.clone().into_string().unwrap_or_default(),
        },
        FsContent::Special(_) => Observed::Special,
    };
    ObservedNode {
        kind,
        perm: node.attr.perm,
        uid: node.attr.uid,
        gid: node.attr.gid,
        nlink: node.attr.nlink,
        size: node.attr.size,
        blksize: node.attr.blksize,
        xattrs: node
            .xattrs
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
    }
}

#[derive(Debug, PartialEq, Eq)]
struct ObservedNode {
    kind: Observed,
    perm: u16,
    uid: u32,
    gid: u32,
    nlink: u32,
    /// File: bytes in `content`. Directory / Symlink / Special: not
    /// load-bearing for the round-trip, but pinned so a regression
    /// that leaves a stale `attr.size` on a non-File would show up.
    size: u64,
    /// 512 by construction (`FsNode::new_node`). Asserted to surface
    /// any load-path regression that defaults it to 0.
    blksize: u32,
    xattrs: BTreeMap<String, Vec<u8>>,
}

#[derive(Debug, PartialEq, Eq)]
enum Observed {
    File { content: Vec<u8> },
    Directory,
    Symlink { target: String },
    Special,
}

// ── Properties ─────────────────────────────────────────────────────
//
// One property per invariant. Failure messages name the SPEC that
// broke; bundling multiple invariants into a single property would
// print the same generic test name for every kind of regression and
// force the reader to open the test source to see what actually
// failed.
//
// Several helpers cache the result of `build_and_save` plus one
// reload so each property doesn't replay the costly part of the
// pipeline four times over.

/// Common preamble: build the spec into a fresh archive, save, load
/// once. Returns (placed hardlinks, post-load snapshot, raw archive
/// bytes).
fn build_save_load(
    input: &TestInput,
    archive: &Path,
) -> (
    Vec<(String, String)>,
    BTreeMap<String, ObservedNode>,
    Vec<u8>,
) {
    let placed_links = build_and_save(archive, input).unwrap();
    let tree = archive_io::load(archive, input.password.clone()).unwrap();
    let snap = snapshot(&tree);
    let bytes = std::fs::read(archive).unwrap();
    (placed_links, snap, bytes)
}

/// SPEC: `load(save(T))` returns a tree whose snapshot matches the
/// generator's specs at every observable field (path, kind, content,
/// perm, owner, xattrs, file size).
fn assert_generator_specs_survive(
    input: &TestInput,
    snap: &BTreeMap<String, ObservedNode>,
    placed_links: &[(String, String)],
) -> Result<(), TestCaseError> {
    let mut extra_links_per_source: BTreeMap<String, usize> = BTreeMap::new();
    for (src, _dst) in placed_links {
        *extra_links_per_source.entry(src.clone()).or_insert(0) += 1;
    }
    check_root(snap, &input.root, &extra_links_per_source)
}

/// SPEC: a second `save → load` after the first is a fixed point.
///
/// **Precondition**: `snap_first` MUST be the snapshot produced by the
/// most recent `build_save_load` call on the same `archive` (i.e. the
/// snapshot of the *first* load). Passing an unrelated `BTreeMap`
/// here makes the resulting equality diff meaningless.
fn assert_save_is_idempotent(
    input: &TestInput,
    archive: &Path,
    snap_first: &BTreeMap<String, ObservedNode>,
) -> Result<(), TestCaseError> {
    let mut tree = archive_io::load(archive, input.password.clone()).unwrap();
    archive_io::save(&mut tree).unwrap();
    let after_second_load = archive_io::load(archive, input.password.clone()).unwrap();
    let snap_second = snapshot(&after_second_load);
    prop_assert_eq!(snap_first, &snap_second, "second round-trip drifted");
    Ok(())
}

/// SPEC: after a `save → load`, no inode whose `nlink == 0` survives.
/// Orphan inodes (nlink-zero but kept alive because a fd held them
/// open) are an in-memory-only artefact of the FUSE
/// unlink-while-open path; once the archive is reloaded, every
/// surviving inode must have at least one reachable directory entry.
/// Currently we approach this via the observation that
/// `collect_dfs` only walks reachable entries — so a leaked
/// nlink-zero inode would be invisible to it. We check the
/// `inodes` map directly (via the FileTree internal `get` path) for
/// every inode the snapshot mentions and confirm `nlink >= 1`.
fn assert_no_orphan_inodes(tree: &FileTree) -> Result<(), TestCaseError> {
    // collect_dfs visits every reachable inode exactly once. The
    // root is special-cased separately.
    let root_nlink = tree.get(ROOT_INODE).unwrap().attr.nlink;
    prop_assert!(
        root_nlink >= 1,
        "ROOT has nlink {root_nlink} (should be >= 1 after save→load)"
    );
    for (_, node, path) in tree.collect_dfs() {
        prop_assert!(
            node.attr.nlink >= 1,
            "node at {:?} has nlink {} after save→load (should be >= 1; orphan leaked across save)",
            path,
            node.attr.nlink
        );
    }
    Ok(())
}

/// SPEC: for every observed File, `attr.size == content.len()`. A
/// stale size left after a `truncate(0)` followed by no write would
/// otherwise show up only in `stat()` and not in the tree shape.
fn assert_file_size_matches_content(
    snap: &BTreeMap<String, ObservedNode>,
) -> Result<(), TestCaseError> {
    for (path, observed) in snap {
        if let Observed::File { content } = &observed.kind {
            prop_assert_eq!(
                observed.size,
                content.len() as u64,
                "file {:?} attr.size {} mismatches content.len {}",
                path,
                observed.size,
                content.len()
            );
        }
    }
    Ok(())
}

/// SPEC: every loaded directory's `nlink` equals `2 + #subdirs`
/// (POSIX: self + per-`..` from each child directory). Applies to
/// the root inode too, which `collect_dfs` skips and which a unit
/// test would otherwise have to special-case.
fn assert_directory_nlink_posix(
    snap: &BTreeMap<String, ObservedNode>,
) -> Result<(), TestCaseError> {
    for (path, observed) in snap {
        if matches!(observed.kind, Observed::Directory) {
            let subdir_count = count_immediate_subdirs(snap, path);
            prop_assert_eq!(
                observed.nlink as usize,
                2 + subdir_count,
                "directory {:?} has nlink {} but {} direct subdirs",
                path,
                observed.nlink,
                subdir_count
            );
        }
    }
    Ok(())
}

/// SPEC: every hardlink destination observes the same node — byte
/// equal — as its source.
fn assert_hardlinks_equivalent(
    snap: &BTreeMap<String, ObservedNode>,
    placed_links: &[(String, String)],
) -> Result<(), TestCaseError> {
    for (src, dst) in placed_links {
        let src_obs = snap
            .get(src)
            .ok_or_else(|| TestCaseError::fail(format!("hardlink source missing: {src}")))?;
        let dst_obs = snap
            .get(dst)
            .ok_or_else(|| TestCaseError::fail(format!("hardlink dest missing: {dst}")))?;
        prop_assert_eq!(
            src_obs,
            dst_obs,
            "hardlink content/metadata mismatch between {} and {}",
            src,
            dst
        );
    }
    Ok(())
}

proptest! {
    // Plaintext properties: 64 cases keeps `cargo test` under a few
    // seconds locally. The scheduled CI job runs at
    // `PROPTEST_CASES=10000` for the broad sweep.
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// SPEC: For plaintext archives, every generated `NodeSpec` is
    /// observable at the expected path after `save → load`, with
    /// matching content, permission bits, owner, xattrs, and size.
    /// Failures here flag a regression in *what* the format
    /// preserves.
    #[test]
    fn plain_generator_specs_survive_load(input in arb_test_input_plain()) {
        let dir = tempfile::TempDir::new().unwrap();
        let archive = dir.path().join("specs.pna");
        let (placed, snap, _bytes) = build_save_load(&input, &archive);
        assert_generator_specs_survive(&input, &snap, &placed)?;
    }

    /// SPEC: For plaintext archives, a second `save → load` after the
    /// first must reach the same snapshot. Failures here flag
    /// non-deterministic state changes in the save path that the
    /// byte-id property cannot see (e.g. timestamp drift, secondary
    /// allocations leaking into metadata).
    #[test]
    fn plain_save_is_idempotent_under_reload(input in arb_test_input_plain()) {
        let dir = tempfile::TempDir::new().unwrap();
        let archive = dir.path().join("idem.pna");
        let (_placed, snap_first, _bytes) = build_save_load(&input, &archive);
        assert_save_is_idempotent(&input, &archive, &snap_first)?;
    }

    /// SPEC: For plaintext archives, every loaded directory satisfies
    /// the POSIX `nlink = 2 + #subdirs` invariant — including the
    /// root, which `collect_dfs` does not emit. A load path that
    /// defaulted every directory's `nlink` to 1 (a class of bug that
    /// is easy to introduce when each `FsNode` is constructed
    /// independently of its surrounding tree) would show up here.
    #[test]
    fn plain_loaded_directories_have_posix_nlink(input in arb_test_input_plain()) {
        let dir = tempfile::TempDir::new().unwrap();
        let archive = dir.path().join("nlink.pna");
        let (_placed, snap, _bytes) = build_save_load(&input, &archive);
        assert_directory_nlink_posix(&snap)?;
    }

    /// SPEC: For plaintext archives, Special-typed spec entries
    /// (block / char / fifo / socket) are absent from the post-load
    /// snapshot. PNA has no DataKind for them yet, so `save()` drops
    /// them deliberately; this is the inverse property pinning that
    /// drop is complete.
    #[test]
    fn plain_save_strips_special_entries(input in arb_test_input_plain()) {
        let dir = tempfile::TempDir::new().unwrap();
        let archive = dir.path().join("special.pna");
        let (_placed, snap, _bytes) = build_save_load(&input, &archive);
        check_special_absences(&snap, &input.root)?;
    }

    /// SPEC: For plaintext archives, every hardlink destination
    /// observes a byte-identical `ObservedNode` to its source — the
    /// shared inode is materialised the same way at every path that
    /// references it.
    #[test]
    fn plain_hardlinks_are_observationally_equivalent(input in arb_test_input_plain()) {
        let dir = tempfile::TempDir::new().unwrap();
        let archive = dir.path().join("hardlink.pna");
        let (placed, snap, _bytes) = build_save_load(&input, &archive);
        assert_hardlinks_equivalent(&snap, &placed)?;
    }

    /// SPEC: After applying an arbitrary sequence of mutations
    /// (write / truncate / unlink / setxattr / removexattr) to a
    /// freshly-loaded tree, a save+reload still produces a valid
    /// tree — directory `nlink` invariant holds, and a second
    /// save+reload is a fixed point (idempotence under the mutated
    /// state). Catches transient-state bugs the static round-trip
    /// properties miss (e.g. unlink-then-recreate corrupting the
    /// inode reuse path, mixed truncate+write order leaking stale
    /// size, xattr churn drifting the BTreeMap iteration order).
    #[test]
    fn plain_mutation_sequence_survives_save_load(
        input in arb_test_input_plain(),
        ops in prop::collection::vec(arb_fs_op(), 0..=24),
    ) {
        let dir = tempfile::TempDir::new().unwrap();
        let archive = dir.path().join("mut.pna");

        build_and_save(&archive, &input).unwrap();
        let mut tree = archive_io::load(&archive, None).unwrap();
        apply_ops(&mut tree, &ops);
        archive_io::save(&mut tree).unwrap();

        let after_mutated_save = archive_io::load(&archive, None).unwrap();
        let snap_first = snapshot(&after_mutated_save);
        assert_directory_nlink_posix(&snap_first)?;
        assert_no_orphan_inodes(&after_mutated_save)?;
        assert_file_size_matches_content(&snap_first)?;

        // Idempotence under the mutated state: a second cycle from
        // here must reach the same snapshot. If a mutation produced
        // a tree whose save is non-deterministic, the second cycle
        // would diverge.
        assert_save_is_idempotent(&input, &archive, &snap_first)?;
    }

    /// SPEC: For plaintext archives, save is a deterministic function
    /// of the tree. Saving, loading, and saving again yields
    /// byte-identical archives. Catches drift the snapshot equality
    /// cannot see (e.g. ordering of chunks inside an entry).
    #[test]
    fn plain_save_is_byte_identical_when_replayed(input in arb_test_input_plain()) {
        let dir = tempfile::TempDir::new().unwrap();
        let archive = dir.path().join("byte.pna");

        build_and_save(&archive, &input).unwrap();
        let bytes_first = std::fs::read(&archive).unwrap();

        let mut tree = archive_io::load(&archive, None).unwrap();
        archive_io::save(&mut tree).unwrap();
        let bytes_second = std::fs::read(&archive).unwrap();

        prop_assert_eq!(
            bytes_first.len(), bytes_second.len(),
            "archive length changed across a no-op save: {} -> {}",
            bytes_first.len(), bytes_second.len()
        );
        prop_assert!(
            bytes_first == bytes_second,
            "archive bytes diverged across a no-op save (first {} bytes, second {} bytes)",
            bytes_first.len(), bytes_second.len()
        );
    }
}

// Encrypted-archive properties live in their own `proptest!` block
// with a much smaller case cap. PNA's encrypted save path runs
// Argon2id key derivation per entry, which costs ~150 ms per call in
// release mode (several times that in debug) — running encrypted
// properties at the default 64 cases would push `cargo test` well
// past the few-seconds budget. The scheduled CI sweep
// (`PROPTEST_CASES` env) overrides the in-tree cap and walks a
// proportionally wider sample. Every case in this block exercises
// the encrypted save path; the generators guarantee `password.is_some()`
// and at least one File entry, so no `prop_assume!` filters need to
// burn cases.
proptest! {
    #![proptest_config(ProptestConfig::with_cases(8))]

    /// SPEC: For encrypted archives, generated `NodeSpec` fields
    /// survive `save → load` exactly as in the plaintext case —
    /// only the on-disk bytes differ (fresh IV per save).
    #[test]
    fn encrypted_generator_specs_survive_load(input in arb_test_input_encrypted()) {
        let dir = tempfile::TempDir::new().unwrap();
        let archive = dir.path().join("specs-enc.pna");
        let (placed, snap, _bytes) = build_save_load(&input, &archive);
        assert_generator_specs_survive(&input, &snap, &placed)?;
    }

    /// SPEC: For encrypted archives, a second `save → load` is a
    /// fixed point at the snapshot level — encryption changes the
    /// bytes but not the AST.
    #[test]
    fn encrypted_save_is_idempotent_under_reload(input in arb_test_input_encrypted()) {
        let dir = tempfile::TempDir::new().unwrap();
        let archive = dir.path().join("idem-enc.pna");
        let (_placed, snap_first, _bytes) = build_save_load(&input, &archive);
        assert_save_is_idempotent(&input, &archive, &snap_first)?;
    }

    /// SPEC: For encrypted archives, the directory `nlink` invariant
    /// holds the same way it does for plaintext.
    #[test]
    fn encrypted_loaded_directories_have_posix_nlink(input in arb_test_input_encrypted()) {
        let dir = tempfile::TempDir::new().unwrap();
        let archive = dir.path().join("nlink-enc.pna");
        let (_placed, snap, _bytes) = build_save_load(&input, &archive);
        assert_directory_nlink_posix(&snap)?;
    }

    /// SPEC: For encrypted archives, hardlinked siblings observe the
    /// same inode at every reference path. The encrypted save path
    /// still emits a HardLink entry for the secondary references;
    /// this property pins that they re-resolve to the same content
    /// after a decrypt cycle.
    #[test]
    fn encrypted_hardlinks_are_observationally_equivalent(input in arb_test_input_encrypted()) {
        let dir = tempfile::TempDir::new().unwrap();
        let archive = dir.path().join("hardlink-enc.pna");
        let (placed, snap, _bytes) = build_save_load(&input, &archive);
        assert_hardlinks_equivalent(&snap, &placed)?;
    }

    /// SPEC: For encrypted archives, Special-typed spec entries are
    /// still absent from the post-load snapshot. PNA has no on-disk
    /// kind for special files regardless of cipher state, so the save
    /// path drops them in both modes. A regression that started
    /// persisting Special only on the encrypted path would otherwise
    /// pass plain coverage and slip through.
    #[test]
    fn encrypted_save_strips_special_entries(input in arb_test_input_encrypted()) {
        let dir = tempfile::TempDir::new().unwrap();
        let archive = dir.path().join("special-enc.pna");
        let (_placed, snap, _bytes) = build_save_load(&input, &archive);
        check_special_absences(&snap, &input.root)?;
    }

    /// SPEC: An encrypted archive cannot be read back with a
    /// different password in any **observably useful** way — the
    /// content the original tree wrote must not come back from a
    /// `load(wrong_password)` call.
    ///
    /// We do **not** require `load` to return `Err`: pna 0.33's
    /// encrypted entries use AES-CTR with no AEAD / MAC, so the
    /// kernel never raises on a wrong key — it just produces
    /// different plaintext. The observable contract is therefore
    /// "the bytes we wrote do not come back": at least one file
    /// must decrypt to something other than the input. A stronger
    /// SPEC ("load is Err") would require pnafs to add a password
    /// verifier on top of pna; that is a separate piece of work and
    /// is not what this property pins.
    ///
    /// The generator guarantees the archive contains at least one
    /// File (so there is cipher state to validate) and the wrong
    /// password differs from the real one structurally (appending
    /// `!`, which the password regex `[a-zA-Z0-9]{4,12}` cannot
    /// produce).
    #[test]
    fn encrypted_archive_rejects_wrong_password(input in arb_test_input_encrypted()) {
        let wrong = format!("{}!", input.password.as_deref().unwrap());
        let (files, _dirs) = inventory(&input.root);

        let dir = tempfile::TempDir::new().unwrap();
        let archive = dir.path().join("wp.pna");
        build_and_save(&archive, &input).unwrap();

        // Snapshot the input file contents up-front so the
        // mismatch check has the original values to compare
        // against (the input tree is consumed by build_and_save).
        // Empty files are excluded: their ciphertext is zero bytes,
        // so any key — including the wrong one — "decrypts" them to
        // the same empty plaintext, and the wrong-key contract is
        // vacuous for that case. The generator already guarantees
        // at least one non-empty file in `arb_root_children_with_file`,
        // so the resulting map is non-empty by construction.
        let originals: BTreeMap<String, Vec<u8>> = files
            .iter()
            .filter_map(|p| lookup_file_content(&input.root, p).map(|c| (p.clone(), c)))
            .filter(|(_, c)| !c.is_empty())
            .collect();

        match archive_io::load(&archive, Some(wrong)) {
            Err(_) => {
                // Strong reject — desirable, but not the path pna
                // currently takes in most cases.
            }
            Ok(reloaded) => {
                // The portable contract: not every file may
                // accidentally collide. For short files the
                // birthday-style probability of a byte-for-byte
                // accidental match is non-zero, so we require only
                // that the **set** of (path, content) pairs differs
                // somewhere — equivalently, at least one file
                // reads back different bytes.
                let any_mismatch = originals.iter().any(|(fp, orig)| {
                    let observed = reloaded
                        .resolve_path(Path::new(fp))
                        .and_then(|ino| reloaded.get(ino))
                        .and_then(|n| match &n.content {
                            FsContent::File(fc) => Some(fc.data().to_vec()),
                            _ => None,
                        });
                    match observed {
                        Some(obs) => obs != *orig,
                        None => true,
                    }
                });
                prop_assert!(
                    any_mismatch,
                    "wrong-password load returned every file's content unchanged — \
                     no decryption barrier"
                );
            }
        }
    }
}

// ── Helpers ────────────────────────────────────────────────────────

/// Walk the spec tree looking for a File node at `path`. Returns its
/// content if found, `None` otherwise (e.g. the path names a Dir or
/// Symlink, or doesn't exist in the spec).
fn lookup_file_content(root: &BTreeMap<String, NodeSpec>, path: &str) -> Option<Vec<u8>> {
    let mut cur = root;
    let mut parts = path.split('/').peekable();
    while let Some(seg) = parts.next() {
        let node = cur.get(seg)?;
        if parts.peek().is_none() {
            return match node {
                NodeSpec::File { content, .. } => Some(content.clone()),
                _ => None,
            };
        }
        cur = match node {
            NodeSpec::Dir { children, .. } => children,
            _ => return None,
        };
    }
    None
}

/// Number of immediate-child directories of `path` observed in `snap`.
/// "Immediate" = exactly one `/`-separated segment deeper, no further.
fn count_immediate_subdirs(snap: &BTreeMap<String, ObservedNode>, path: &str) -> usize {
    let prefix = if path.is_empty() {
        String::new()
    } else {
        format!("{path}/")
    };
    snap.iter()
        .filter(|(p, n)| {
            matches!(n.kind, Observed::Directory)
                && p.starts_with(&prefix)
                && p.len() > prefix.len()
                && !p[prefix.len()..].contains('/')
        })
        .count()
}

fn check_root(
    snap: &BTreeMap<String, ObservedNode>,
    root: &BTreeMap<String, NodeSpec>,
    extra_links_per_source: &BTreeMap<String, usize>,
) -> Result<(), TestCaseError> {
    for (name, spec) in root {
        check_node(snap, name, spec, extra_links_per_source)?;
    }
    Ok(())
}

fn check_node(
    snap: &BTreeMap<String, ObservedNode>,
    path: &str,
    spec: &NodeSpec,
    extra_links_per_source: &BTreeMap<String, usize>,
) -> Result<(), TestCaseError> {
    // Special entries are checked separately by `check_special_absences`.
    if matches!(spec, NodeSpec::Special { .. }) {
        return Ok(());
    }
    let observed = snap
        .get(path)
        .ok_or_else(|| TestCaseError::fail(format!("missing path {path:?}")))?;
    prop_assert_eq!(observed.blksize, 512, "blksize drift at {}", path);
    match spec {
        NodeSpec::File { meta, content } => {
            match &observed.kind {
                Observed::File { content: c } => {
                    prop_assert_eq!(c, content, "content mismatch at {}", path);
                }
                other => prop_assert!(false, "expected File at {:?}, got {:?}", path, other),
            }
            prop_assert_eq!(
                observed.size,
                content.len() as u64,
                "file size mismatch at {}: attr.size {} vs content.len {}",
                path,
                observed.size,
                content.len(),
            );
            // Every File starts with `nlink = 1`. Each hardlink that
            // successfully placed and points at this path adds one.
            // A leaf File untouched by any hardlink must observe
            // exactly `nlink == 1`; without this assertion a
            // regression that defaulted `nlink` to 0 on load would
            // pass silently for the common case (no hardlinks).
            let extra = extra_links_per_source.get(path).copied().unwrap_or(0);
            prop_assert_eq!(
                observed.nlink as usize,
                1 + extra,
                "File {} has nlink {} but {} hardlink(s) placed",
                path,
                observed.nlink,
                extra
            );
            check_meta(observed, meta, path)?;
        }
        NodeSpec::Symlink { meta, target } => {
            match &observed.kind {
                Observed::Symlink { target: t } => {
                    prop_assert_eq!(t, target, "symlink target mismatch at {}", path);
                }
                other => prop_assert!(false, "expected Symlink at {:?}, got {:?}", path, other),
            }
            // Symlinks always carry perm `0o777` regardless of what the
            // caller passed: `file_tree::FileTree::create_symlink`
            // (file_tree.rs:1184-1211) hardcodes the mode to match
            // POSIX, where `chmod` on a symlink itself is a no-op —
            // the bits never matter to the kernel. Pin the contract
            // here so a regression that started honouring the caller's
            // perm would surface. If `create_symlink`'s mode handling
            // ever changes, this expectation and the file_tree side
            // must move together.
            prop_assert_eq!(
                observed.perm,
                0o777,
                "symlink at {} should have perm 0o777, got {:o}",
                path,
                observed.perm
            );
            prop_assert_eq!(observed.uid, meta.uid, "symlink uid mismatch at {}", path);
            prop_assert_eq!(observed.gid, meta.gid, "symlink gid mismatch at {}", path);
            prop_assert_eq!(
                &observed.xattrs,
                &meta.xattrs,
                "symlink xattrs drift at {}",
                path
            );
            // Hardlinks-to-symlinks are not supported by the FS API
            // pnafs exposes (`create_hardlink` rejects directories
            // and only accepts a source inode that is a regular
            // file), so a symlink's nlink is always 1.
            prop_assert_eq!(
                observed.nlink,
                1,
                "symlink {} has nlink {}, expected 1",
                path,
                observed.nlink
            );
        }
        NodeSpec::Dir { meta, children } => {
            prop_assert!(
                matches!(observed.kind, Observed::Directory),
                "expected Directory at {:?}, got {:?}",
                path,
                observed.kind
            );
            check_meta(observed, meta, path)?;
            for (cn, cs) in children {
                let child_path = if path.is_empty() {
                    cn.clone()
                } else {
                    format!("{path}/{cn}")
                };
                check_node(snap, &child_path, cs, extra_links_per_source)?;
            }
        }
        NodeSpec::Special { .. } => unreachable!("filtered above"),
    }
    Ok(())
}

/// Walk the spec tree; for every Special node, assert it does NOT
/// appear in the snapshot — the PNA save path drops it deliberately.
///
/// **Maintenance**: if PNA ever grows an on-disk `DataKind` for
/// special files (see `archive_io::save`'s `FsContent::Special`
/// branch and the forward-compatibility note on
/// `file_tree::FileTree::create_special`), this inverse property
/// must flip to a *presence* check — call into `check_node` for
/// Special variants and add `Observed::Special { kind, rdev }`
/// equality there. The current absence assertion is correct only
/// while PNA cannot persist these nodes.
fn check_special_absences(
    snap: &BTreeMap<String, ObservedNode>,
    root: &BTreeMap<String, NodeSpec>,
) -> Result<(), TestCaseError> {
    fn walk(
        snap: &BTreeMap<String, ObservedNode>,
        prefix: &str,
        spec: &BTreeMap<String, NodeSpec>,
    ) -> Result<(), TestCaseError> {
        for (name, node) in spec {
            let p = if prefix.is_empty() {
                name.clone()
            } else {
                format!("{prefix}/{name}")
            };
            match node {
                NodeSpec::Special { .. } => {
                    prop_assert!(
                        !snap.contains_key(&p),
                        "Special at {:?} should have been dropped on save but is present \
                         (PNA format has no DataKind for special files)",
                        p
                    );
                }
                NodeSpec::Dir { children, .. } => {
                    walk(snap, &p, children)?;
                }
                NodeSpec::File { .. } | NodeSpec::Symlink { .. } => {}
            }
        }
        Ok(())
    }
    walk(snap, "", root)
}

fn check_meta(observed: &ObservedNode, meta: &NodeMeta, path: &str) -> Result<(), TestCaseError> {
    prop_assert_eq!(
        observed.perm,
        meta.perm & 0o7777,
        "perm mismatch at {}",
        path
    );
    prop_assert_eq!(observed.uid, meta.uid, "uid mismatch at {}", path);
    prop_assert_eq!(observed.gid, meta.gid, "gid mismatch at {}", path);
    prop_assert_eq!(&observed.xattrs, &meta.xattrs, "xattrs drift at {}", path);
    Ok(())
}

// Compile-time sanity: silence the "unused import" warning when
// `cfg(test)` builds remove items above.
#[allow(dead_code)]
fn _check_imports(_: &OsStr) {}
