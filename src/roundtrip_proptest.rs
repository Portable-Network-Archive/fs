//! Property-based round-trip tests for `archive_io::{load, save}`.
//!
//! A FileTree is built from a randomly generated set of file/directory
//! specs, saved to a fresh archive, reloaded, and compared structurally.
//! Whatever survives a save → load cycle in production must round-trip
//! here too: file paths, content, permission bits, owner uid/gid, and
//! xattrs.
//!
//! This is the regression net for the class of bug PR review caught
//! one-off (load-time directory `nlink: 1`, dropped xattrs, plaintext
//! files re-encrypted because the mount carried a password). A property
//! generator hits state combinations enumerated tests would never reach.

use crate::archive_io;
use crate::file_tree::{FileTree, FsContent, Owner, ROOT_INODE};
use proptest::prelude::*;
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::path::Path;

/// One node we ask the tree to materialise. `Dir` is implied by parent
/// segments of any file path, so we only generate File specs explicitly
/// — but each File can carry its own metadata + xattrs.
#[derive(Debug, Clone)]
struct FileSpec {
    /// Forward-slash path with no leading `/`. Generator guarantees
    /// non-empty, no `.` / `..` components, and unique names per parent.
    path: String,
    content: Vec<u8>,
    perm: u16,
    uid: u32,
    gid: u32,
    /// xattr names are UTF-8 (PNA wire format). Values are arbitrary bytes.
    xattrs: BTreeMap<String, Vec<u8>>,
}

// ── Generators ─────────────────────────────────────────────────────

/// One path component: 1–6 lowercase ASCII letters. Constrained on
/// purpose to keep the search space focused; PNA EntryName accepts more,
/// but the round-trip property doesn't care about character classes.
fn arb_segment() -> impl Strategy<Value = String> {
    "[a-z]{1,6}".prop_map(|s| s.to_string())
}

/// 0–3 components joined with `/`, plus a final filename.
fn arb_path() -> impl Strategy<Value = String> {
    (prop::collection::vec(arb_segment(), 0..=3), arb_segment()).prop_map(|(parents, leaf)| {
        let mut parts = parents;
        parts.push(leaf);
        parts.join("/")
    })
}

fn arb_xattr_name() -> impl Strategy<Value = String> {
    // POSIX namespace prefix + 1–6 chars. user.* is the unrestricted
    // namespace; sticking to it keeps the test working under
    // DefaultPermissions on systems where security.* is gated.
    "user\\.[a-z]{1,6}".prop_map(|s| s.to_string())
}

fn arb_xattrs() -> impl Strategy<Value = BTreeMap<String, Vec<u8>>> {
    prop::collection::btree_map(
        arb_xattr_name(),
        prop::collection::vec(any::<u8>(), 0..=32),
        0..=4,
    )
}

fn arb_filespec() -> impl Strategy<Value = FileSpec> {
    (
        arb_path(),
        prop::collection::vec(any::<u8>(), 0..=128),
        // Mode bits: file always has S_IFREG implicitly via FileType,
        // so just generate the permission portion.
        prop::num::u16::ANY.prop_map(|m| m & 0o7777),
        prop::num::u32::ANY,
        prop::num::u32::ANY,
        arb_xattrs(),
    )
        .prop_map(|(path, content, perm, uid, gid, xattrs)| FileSpec {
            path,
            content,
            perm,
            uid,
            gid,
            xattrs,
        })
}

/// Up to N specs whose paths form a valid tree: distinct final paths,
/// and no path is also a directory ancestor of another path. The
/// second constraint matters because a file at `g` would block any
/// later attempt to materialise `g/a` (the file can't be a directory
/// parent). We sort lexicographically — shorter paths come first —
/// then keep each spec only if none of its parent components were
/// already taken by a previously kept (file) spec.
fn arb_filespec_set() -> impl Strategy<Value = Vec<FileSpec>> {
    prop::collection::vec(arb_filespec(), 0..=8).prop_map(|mut specs| {
        specs.sort_by(|a, b| a.path.cmp(&b.path));
        let mut kept: Vec<FileSpec> = Vec::new();
        'next: for s in specs {
            if kept.iter().any(|k| k.path == s.path) {
                continue;
            }
            let components: Vec<&str> = s.path.split('/').collect();
            for i in 1..components.len() {
                let parent = components[..i].join("/");
                if kept.iter().any(|k| k.path == parent) {
                    continue 'next;
                }
            }
            kept.push(s);
        }
        kept
    })
}

// ── Build / compare ────────────────────────────────────────────────

/// Build a tree under `archive_path` matching `specs` and save it.
fn build_and_save(archive_path: &Path, specs: &[FileSpec]) -> std::io::Result<()> {
    // Bootstrap: load() requires an existing archive, so write an empty
    // header first, then load it back to get a writable FileTree.
    {
        let a = pna::Archive::write_header(std::fs::File::create(archive_path)?)?;
        a.finalize()?;
    }
    let mut tree = archive_io::load(archive_path, None)?;

    for spec in specs {
        let path = Path::new(&spec.path);
        let parent_ino = match path.parent() {
            Some(p) if !p.as_os_str().is_empty() => tree.make_dir_all(p, ROOT_INODE)?,
            _ => ROOT_INODE,
        };
        let leaf = path
            .file_name()
            .expect("generator guarantees non-empty leaf");
        let owner = Owner::new(spec.uid, spec.gid);
        let ino = tree
            .create_file(parent_ino, leaf, spec.perm as u32, owner)
            .map_err(|e| std::io::Error::other(format!("create_file: {e:?}")))?
            .attr
            .ino
            .0;
        if !spec.content.is_empty() {
            tree.write_file(ino, 0, &spec.content)
                .map_err(|e| std::io::Error::other(format!("write_file: {e:?}")))?;
        }
        for (name, value) in &spec.xattrs {
            tree.setxattr(ino, name, value, 0)
                .map_err(|e| std::io::Error::other(format!("setxattr: {e:?}")))?;
        }
    }

    archive_io::save(&mut tree)
}

/// Walk `tree` and pull out everything we expect to round-trip, keyed
/// by archive path. Skips the root entry (it has no path).
fn snapshot(tree: &FileTree) -> BTreeMap<String, ObservedNode> {
    tree.collect_dfs()
        .into_iter()
        .map(|(_ino, node, path)| {
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
            let xattrs = node
                .xattrs
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            (
                path,
                ObservedNode {
                    kind,
                    perm: node.attr.perm,
                    uid: node.attr.uid,
                    gid: node.attr.gid,
                    xattrs,
                },
            )
        })
        .collect()
}

#[derive(Debug, PartialEq, Eq)]
struct ObservedNode {
    kind: Observed,
    perm: u16,
    uid: u32,
    gid: u32,
    xattrs: BTreeMap<String, Vec<u8>>,
}

#[derive(Debug, PartialEq, Eq)]
enum Observed {
    File { content: Vec<u8> },
    Directory,
    Symlink { target: String },
    Special,
}

// ── Property ───────────────────────────────────────────────────────

proptest! {
    // Cap at 64 cases per `cargo test` run so the suite stays under a
    // few seconds even with FUSE-free in-memory work; rerun with
    // PROPTEST_CASES=N to widen the sweep before a release.
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn save_load_roundtrip_preserves_tree(specs in arb_filespec_set()) {
        let dir = tempfile::TempDir::new().unwrap();
        let archive = dir.path().join("rt.pna");

        build_and_save(&archive, &specs).unwrap();
        let original_after_save = archive_io::load(&archive, None).unwrap();
        let snap_a = snapshot(&original_after_save);

        // Re-save and load again. A second round must be byte-identical
        // to the first — anything that drifts on a no-op rewrite is a
        // bug (it would compound on every Immediate save).
        let mut tree = archive_io::load(&archive, None).unwrap();
        archive_io::save(&mut tree).unwrap();
        let reloaded = archive_io::load(&archive, None).unwrap();
        let snap_b = snapshot(&reloaded);

        prop_assert_eq!(&snap_a, &snap_b, "second round-trip drifted");

        // Generated specs must be present with matching content/metadata.
        for spec in &specs {
            let observed = snap_a
                .get(&spec.path)
                .unwrap_or_else(|| panic!("missing path {:?} after save+load", spec.path));
            match &observed.kind {
                Observed::File { content } => {
                    prop_assert_eq!(content, &spec.content, "content mismatch at {}", &spec.path);
                }
                other => panic!("expected File at {:?}, got {:?}", spec.path, other),
            }
            prop_assert_eq!(observed.perm, spec.perm & 0o7777,
                "perm mismatch at {}", &spec.path);
            prop_assert_eq!(observed.uid, spec.uid, "uid mismatch at {}", &spec.path);
            prop_assert_eq!(observed.gid, spec.gid, "gid mismatch at {}", &spec.path);
            prop_assert_eq!(&observed.xattrs, &spec.xattrs,
                "xattrs drift at {}", &spec.path);
        }

        // Every loaded directory must satisfy POSIX nlink = 2 + #subdirs.
        for (path, node) in &snap_a {
            if matches!(node.kind, Observed::Directory) {
                let prefix = format!("{path}/");
                let direct_subdirs = snap_a.iter().filter(|(p, n)| {
                    matches!(n.kind, Observed::Directory)
                        && p.starts_with(&prefix)
                        && p[prefix.len()..].find('/').is_none()
                }).count();
                let ino = original_after_save
                    .resolve_path(Path::new(path))
                    .expect("collected path resolves");
                let nlink = original_after_save.get(ino).unwrap().attr.nlink;
                prop_assert_eq!(
                    nlink as usize,
                    2 + direct_subdirs,
                    "directory {} has nlink {} but {} direct subdirs",
                    path, nlink, direct_subdirs
                );
            }
        }
    }
}

// Compile-time sanity: silence the "unused import" warning when
// `cfg(test)` builds remove items above.
#[allow(dead_code)]
fn _check_imports(_: &OsStr) {}
