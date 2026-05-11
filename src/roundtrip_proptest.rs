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
//! Properties layered here:
//!
//! 1. `save_load_roundtrip_preserves_tree` — every observable field
//!    (paths, file content, permission bits, owner uid/gid, xattrs,
//!    directory nlink) survives a `save → load` cycle, and a second
//!    `save → load` is a fixed point (observational idempotence).
//!
//! 2. `plain_save_is_byte_identical_when_replayed` — saving a tree,
//!    loading it, and saving the result must produce the **same archive
//!    bytes**. Anything the format records non-deterministically (e.g.
//!    iteration order of a `HashMap`-backed metadata field) would show
//!    up here long before it became a user-visible corruption.
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
use crate::file_tree::{FileTree, FsContent, Inode, Owner, ROOT_INODE};
use proptest::prelude::*;
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::io;
use std::path::Path;

// ── Spec types ─────────────────────────────────────────────────────

/// Per-inode metadata the generator chooses. Distinct from `Vec<u8>`
/// content so dir nodes can carry the same set of fields.
#[derive(Debug, Clone)]
struct NodeMeta {
    perm: u16,
    uid: u32,
    gid: u32,
    xattrs: BTreeMap<String, Vec<u8>>,
}

/// One node in the generated tree. The map of children is a
/// `BTreeMap` so sibling names are deduplicated by construction (no
/// need for a post-filter), and proptest can shrink it by removing
/// individual entries without restructuring the rest.
#[derive(Debug, Clone)]
enum NodeSpec {
    File {
        meta: NodeMeta,
        content: Vec<u8>,
    },
    Dir {
        meta: NodeMeta,
        children: BTreeMap<String, NodeSpec>,
    },
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

/// Tree of nodes generated bottom-up. `prop_recursive` lets proptest
/// shrink the entire subtree structure (depth, branching, leaf
/// contents) coherently.
fn arb_node() -> impl Strategy<Value = NodeSpec> {
    let leaf = (arb_meta(), prop::collection::vec(any::<u8>(), 0..=128))
        .prop_map(|(meta, content)| NodeSpec::File { meta, content });

    leaf.prop_recursive(
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
        NodeSpec::File { meta, .. } | NodeSpec::Dir { meta, .. } => Owner::new(meta.uid, meta.gid),
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

/// Build the tree under `archive_path` matching `root_children` and
/// save it.
fn build_and_save(
    archive_path: &Path,
    root_children: &BTreeMap<String, NodeSpec>,
) -> io::Result<()> {
    bootstrap_empty(archive_path)?;
    let mut tree = archive_io::load(archive_path, None)?;
    for (name, spec) in root_children {
        build_child(&mut tree, ROOT_INODE, name, spec)?;
    }
    archive_io::save(&mut tree)
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
    /// over (path, kind, content, perm, uid, gid, xattrs, nlink). A
    /// second `save → load` cycle must be a fixed point (no
    /// observational drift), which catches non-determinism in the
    /// save path that AST equality between in-memory tree and reload
    /// alone would miss.
    #[test]
    fn save_load_roundtrip_preserves_tree(root in arb_root_children()) {
        let dir = tempfile::TempDir::new().unwrap();
        let archive = dir.path().join("rt.pna");

        build_and_save(&archive, &root).unwrap();
        let after_first_load = archive_io::load(&archive, None).unwrap();
        let snap_first = snapshot(&after_first_load);

        let mut tree = archive_io::load(&archive, None).unwrap();
        archive_io::save(&mut tree).unwrap();
        let after_second_load = archive_io::load(&archive, None).unwrap();
        let snap_second = snapshot(&after_second_load);

        prop_assert_eq!(&snap_first, &snap_second, "second round-trip drifted");

        // Spot-check the generated specs survived intact.
        check_root(&snap_first, &root)?;

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
    fn plain_save_is_byte_identical_when_replayed(root in arb_root_children()) {
        let dir = tempfile::TempDir::new().unwrap();
        let archive = dir.path().join("byte.pna");

        build_and_save(&archive, &root).unwrap();
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
) -> Result<(), TestCaseError> {
    for (name, spec) in root {
        check_node(snap, name, spec)?;
    }
    Ok(())
}

fn check_node(
    snap: &BTreeMap<String, ObservedNode>,
    path: &str,
    spec: &NodeSpec,
) -> Result<(), TestCaseError> {
    let observed = snap
        .get(path)
        .ok_or_else(|| TestCaseError::fail(format!("missing path {path:?}")))?;
    // Every node carries the constructor-default blocksize. Any load
    // path that defaulted this to 0 (e.g. on a future format change)
    // would surface here.
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
            check_meta(observed, meta, path)?;
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
                check_node(snap, &child_path, cs)?;
            }
        }
    }
    Ok(())
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
