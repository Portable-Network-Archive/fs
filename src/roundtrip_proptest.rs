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
//! Properties layered here:
//!
//! 1. `save_load_roundtrip_preserves_tree` — every observable field
//!    (paths, file content, perm, owner uid/gid, xattrs, file size,
//!    directory nlink) survives `save → load`, and a second cycle is a
//!    fixed point. Special-typed spec entries are asserted to be
//!    absent post-load (the PNA-format-limit inverse property).
//!    Hardlinked siblings observe the same content / perm / owner /
//!    xattrs and the source's `nlink` reflects every successful link.
//!
//! 2. `plain_save_is_byte_identical_when_replayed` — saving a tree,
//!    loading it, and saving again produces byte-identical archives.
//!    Catches non-determinism the snapshot-equality check cannot see.
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

fn arb_test_input() -> impl Strategy<Value = TestInput> {
    (
        arb_root_children(),
        prop::collection::vec(arb_hardlink_ref(), 0..=4),
    )
        .prop_map(|(root, hardlinks)| TestInput { root, hardlinks })
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
/// hardlinks, and save.
fn build_and_save(archive_path: &Path, input: &TestInput) -> io::Result<Vec<(String, String)>> {
    bootstrap_empty(archive_path)?;
    let mut tree = archive_io::load(archive_path, None)?;
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

proptest! {
    // Default 64 cases keeps `cargo test` under a few seconds.
    // `PROPTEST_CASES=N cargo test` widens the sweep; the scheduled
    // CI job runs at 10 000.
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// SPEC: `load(save(T)) ≅ T` for every generated tree T, observed
    /// over (path, kind, content, perm, uid, gid, xattrs, nlink,
    /// size). A second `save → load` cycle must be a fixed point.
    ///
    /// Special-typed entries disappear (PNA format has no on-disk
    /// kind for them); the property asserts the inverse explicitly.
    /// Hardlinked siblings observe the same metadata and the source
    /// inode's `nlink` reflects every successful placement.
    #[test]
    fn save_load_roundtrip_preserves_tree(input in arb_test_input()) {
        let dir = tempfile::TempDir::new().unwrap();
        let archive = dir.path().join("rt.pna");

        let placed_links = build_and_save(&archive, &input).unwrap();
        let after_first_load = archive_io::load(&archive, None).unwrap();
        let snap_first = snapshot(&after_first_load);

        let mut tree = archive_io::load(&archive, None).unwrap();
        archive_io::save(&mut tree).unwrap();
        let after_second_load = archive_io::load(&archive, None).unwrap();
        let snap_second = snapshot(&after_second_load);

        prop_assert_eq!(&snap_first, &snap_second, "second round-trip drifted");

        // Count successfully-placed hardlinks per source path so
        // every File leaf can be checked against `nlink == 1 + extra`
        // — including those with zero links, where a load-time
        // regression that defaulted nlink to 0 would otherwise pass
        // silently.
        let mut extra_links_per_source: BTreeMap<String, usize> = BTreeMap::new();
        for (src, _dst) in &placed_links {
            *extra_links_per_source.entry(src.clone()).or_insert(0) += 1;
        }

        // Persistent specs (File / Dir / Symlink) survived intact.
        check_root(&snap_first, &input.root, &extra_links_per_source)?;

        // Special spec entries: PNA cannot persist these, so the save
        // path drops them with a logged warning. Assert they are
        // absent post-load.
        check_special_absences(&snap_first, &input.root)?;

        // Hardlinks: at every placed destination, the observed node
        // must be byte-equal to the observed node at the source path.
        // `prop_assert_eq!(src_obs, dst_obs)` exercises the full
        // `ObservedNode` `PartialEq`, which compares
        // kind / perm / uid / gid / nlink / size / blksize / xattrs —
        // any field that drifts between the two views of the same
        // inode lights up here.
        for (src, dst) in &placed_links {
            let src_obs = snap_first.get(src)
                .ok_or_else(|| TestCaseError::fail(format!("hardlink source missing: {src}")))?;
            let dst_obs = snap_first.get(dst)
                .ok_or_else(|| TestCaseError::fail(format!("hardlink dest missing: {dst}")))?;
            prop_assert_eq!(
                src_obs, dst_obs,
                "hardlink content/metadata mismatch between {} and {}",
                src, dst
            );
        }

        // POSIX nlink invariant for every directory in the snapshot,
        // including the root.
        for (path, observed) in &snap_first {
            if matches!(observed.kind, Observed::Directory) {
                let subdir_count = count_immediate_subdirs(&snap_first, path);
                prop_assert_eq!(
                    observed.nlink as usize,
                    2 + subdir_count,
                    "directory {:?} has nlink {} but {} direct subdirs",
                    path, observed.nlink, subdir_count
                );
            }
        }
    }

    /// SPEC: For plain (unencrypted) archives, save is a deterministic
    /// function of the tree. Saving, loading, and saving again must
    /// yield byte-identical archives — there is no source of
    /// run-to-run nondeterminism in the serialiser for this slice of
    /// the input space.
    ///
    /// This is the strongest form of idempotence: the snapshot-equality
    /// check above can't catch e.g. xattr or chunk ordering drift if
    /// it doesn't change the AST. Byte equality does.
    #[test]
    fn plain_save_is_byte_identical_when_replayed(input in arb_test_input()) {
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

// ── Helpers ────────────────────────────────────────────────────────

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
