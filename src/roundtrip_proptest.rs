//! Property-based round-trip tests for `archive_io::{load, save}`.
//!
//! Each property generates a tree **constructively** — directories carry
//! their own children — so proptest's shrinker walks the same tree
//! structure the assertion sees. Post-hoc filters (the old pattern of
//! generating a flat path list and discarding conflicts) make minimal
//! counter-examples lie, because shrinking by element removal changes
//! which specs survive the filter; the explicit `prop_recursive`
//! generator avoids that trap.
//!
//! Node types covered by the generator (Phase 2 onwards):
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
//! identify the broken invariant directly (Phase 4):
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

// ── Generators ─────────────────────────────────────────────────────

/// One path component: 1–6 lowercase ASCII letters. Constrained on
/// purpose; PNA EntryName accepts more, but expanding the alphabet is
/// the job of a later phase that explicitly probes lossy-conversion
/// edge cases.
fn arb_segment() -> impl Strategy<Value = String> {
    "[a-z]{1,6}".prop_map(|s| s.to_string())
}

fn arb_xattr_name() -> impl Strategy<Value = String> {
    // user.* is the unrestricted namespace under `DefaultPermissions`;
    // sticking to it keeps the generator portable across CI hosts.
    "user\\.[a-z]{1,6}".prop_map(|s| s.to_string())
}

fn arb_xattrs() -> impl Strategy<Value = BTreeMap<String, Vec<u8>>> {
    prop::collection::btree_map(
        arb_xattr_name(),
        prop::collection::vec(any::<u8>(), 0..=32),
        0..=4,
    )
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

/// One leaf node. Files dominate the distribution (weight 4) since
/// they exercise the richest code path; the other three are
/// represented but won't drown out file generation.
fn arb_leaf() -> impl Strategy<Value = NodeSpec> {
    prop_oneof![
        4 => (arb_meta(), prop::collection::vec(any::<u8>(), 0..=128))
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
/// File** somewhere in the tree. Used by encryption-axis properties
/// that need actual cipher content to assert against — generating
/// trees with no files and then `prop_assume!`-filtering would burn
/// the small budget those properties run at.
fn arb_root_children_with_file() -> impl Strategy<Value = BTreeMap<String, NodeSpec>> {
    (
        arb_root_children(),
        arb_segment(),
        arb_meta(),
        prop::collection::vec(any::<u8>(), 0..=128),
    )
        .prop_map(|(mut root, sentinel_name, meta, content)| {
            // Force a uniquely-named File at the root so every
            // generated tree is guaranteed to materialise at least
            // one encrypted entry.
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
// broke; a single combined property would print the same outer test
// name for every kind of regression, which made the previous
// versions of this test painful to triage.
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
    /// root, which `collect_dfs` does not emit. A regression to
    /// `nlink: 1` (the original load-path bug) would show up here.
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
    /// verifier on top of pna; that lives outside Phase 3's scope.
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
        let originals: BTreeMap<String, Vec<u8>> = files
            .iter()
            .filter_map(|p| {
                lookup_file_content(&input.root, p).map(|c| (p.clone(), c))
            })
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
