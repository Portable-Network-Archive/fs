//! Mount-lifetime archive locking.
//!
//! Holding an [`ArchiveLock`] guarantees that no other pnafs process has
//! the same archive mounted in a conflicting mode. The lock is a kernel
//! `flock(2)` on a sidecar file (`.{archive_name}.lock`) next to the
//! archive — never on the archive itself, because `archive_io::save`
//! replaces the archive inode via tmp + `rename(2)`, which would leave a
//! lock held on the original fd attached to a dead inode.
//!
//! The sidecar path is derived from the *canonicalized* archive directory
//! so that logically-identical archives map to the same lock regardless of
//! how the path was spelled on the CLI (relative vs absolute, a different
//! cwd, or a symlinked parent directory); keying on the raw textual path
//! would let two spellings of one archive both take an "exclusive" lock.
//!
//! The sidecar file is created on demand and intentionally **never
//! deleted**: unlinking a lock file while another process still holds an
//! fd to it lets a third process lock a fresh inode at the same path,
//! producing two simultaneous "holders". An empty leftover
//! `.{name}.lock` is harmless and documented behavior.
//!
//! Crash safety: the kernel releases a `flock` when the holding fd is
//! closed, including on abnormal process death — there is no stale-lock
//! state to detect or clean up.

use nix::errno::Errno;
use nix::fcntl::{Flock, FlockArg};
use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::ffi::OsStringExt;
use std::path::{Path, PathBuf};

/// How the mount intends to use the archive.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum LockMode {
    /// Read-only mount: any number of concurrent read-only mounts may
    /// share the archive.
    Shared,
    /// `--write` mount: excludes every other mount, shared or exclusive.
    Exclusive,
}

/// RAII guard for the mount-lifetime archive lock.
///
/// Dropping the guard closes the fd, which releases the kernel lock.
#[derive(Debug)]
pub(crate) struct ArchiveLock {
    /// Held only for its `Drop` impl (closing the fd releases the lock).
    ///
    /// `None` is a soft degradation: a read-only mount could not take the
    /// lock for an environmental reason — the sidecar could be neither
    /// created nor opened (read-only media, an unwritable directory), or
    /// the filesystem has no `flock` support (ENOLCK / ENOTSUP) — so it
    /// proceeds without the cross-process guard (logged), the same
    /// philosophy as the documented NFS caveat. An actual lock conflict
    /// (EWOULDBLOCK) is never degraded. A write mount never degrades —
    /// it requires a writable directory to save anyway.
    _lock: Option<Flock<File>>,
}

impl ArchiveLock {
    /// Sidecar lock file path for `archive_path`: `.{file_name}.lock`
    /// in the archive's directory. The leading-dot family matches the
    /// save tmp file (`.{name}.tmp.{pid}`) but cannot collide with
    /// `cleanup_stale_tmp`, which only matches the `.{name}.tmp.`
    /// prefix followed by a numeric PID.
    ///
    /// The parent directory is canonicalized so that logically-identical
    /// archives map to the same sidecar inode: without this, two mounts
    /// naming the same file via different spellings (relative vs
    /// absolute, a different cwd, a trailing-slash parent, or a symlinked
    /// parent directory) would compute different lock files and both
    /// acquire an "exclusive" lock, defeating the guard. The file name is
    /// kept verbatim (byte-faithful, no lossy UTF-8 conversion) and only
    /// the directory is resolved, so the sidecar lands next to the same
    /// inode the save tmp + `rename(2)` uses.
    fn lock_path(archive_path: &Path) -> io::Result<PathBuf> {
        let name = archive_path.file_name().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("archive path {} has no file name", archive_path.display()),
            )
        })?;
        let parent = archive_path.parent().unwrap_or(Path::new("."));
        // An empty parent (e.g. a bare `a.pna`) means the current
        // directory; canonicalize resolves that to an absolute path.
        let parent = if parent.as_os_str().is_empty() {
            Path::new(".")
        } else {
            parent
        };
        let dir = std::fs::canonicalize(parent)?;
        // Build `.{name}.lock` from raw bytes rather than a lossy String:
        // on Unix a file name is an arbitrary byte sequence, and
        // `to_string_lossy` would collapse distinct non-UTF8 names onto
        // one sidecar, falsely rejecting a different archive as "already
        // mounted".
        let mut file_name = OsString::from(".");
        file_name.push(name);
        let mut bytes = file_name.into_vec();
        bytes.extend_from_slice(b".lock");
        Ok(dir.join(OsString::from_vec(bytes)))
    }

    /// Acquire the lock for `archive_path` in `mode`, without blocking.
    ///
    /// Returns `ErrorKind::ResourceBusy` when another process (or this
    /// one) already holds a conflicting lock — i.e. the archive is
    /// already mounted.
    pub(crate) fn acquire(archive_path: &Path, mode: LockMode) -> io::Result<Self> {
        let lock_path = Self::lock_path(archive_path)?;
        let file = match Self::open_sidecar(&lock_path, mode) {
            Ok(file) => file,
            // Shared (read-only) mounts degrade gracefully when the
            // sidecar cannot be created/opened — e.g. read-only media or
            // a directory the user can read but not write. A `flock`
            // needs no writable fd, but creating a missing sidecar does;
            // rather than fail an otherwise valid read-only mount we
            // proceed without the cross-process guard (same philosophy as
            // the NFS caveat). Exclusive mounts never reach here: they
            // require a writable directory to save anyway.
            Err(e) if mode == LockMode::Shared => {
                log::warn!(
                    "could not open mount lock sidecar {} ({e}); proceeding without \
                     the cross-process mount guard for this read-only mount",
                    lock_path.display(),
                );
                return Ok(Self { _lock: None });
            }
            Err(e) => return Err(e),
        };
        let arg = match mode {
            LockMode::Shared => FlockArg::LockSharedNonblock,
            LockMode::Exclusive => FlockArg::LockExclusiveNonblock,
        };
        match Flock::lock(file, arg) {
            Ok(lock) => Ok(Self { _lock: Some(lock) }),
            // EWOULDBLOCK is an associated alias of EAGAIN, so it cannot
            // appear in a match pattern; compare in a guard instead.
            Err((_file, errno)) if errno == Errno::EWOULDBLOCK => Err(io::Error::new(
                io::ErrorKind::ResourceBusy,
                format!(
                    "archive {} is already mounted by another pnafs instance \
                     (conflicting lock on {}); unmount it first",
                    archive_path.display(),
                    lock_path.display(),
                ),
            )),
            // Any other errno means the lock could not be evaluated at
            // all (e.g. ENOLCK / ENOTSUP / ENOSYS when the filesystem has
            // no flock support) — unlike EWOULDBLOCK above, it is NOT
            // evidence of a conflicting mount. Shared (read-only) mounts
            // degrade gracefully, exactly like the unopenable-sidecar
            // case: failing an otherwise valid read-only mount on such a
            // filesystem would be a regression. Exclusive mounts stay
            // strict, with the lock-file path and operation wrapped in so
            // the user can diagnose it rather than a bare, context-free
            // errno.
            Err((_file, errno)) if mode == LockMode::Shared => {
                log::warn!(
                    "could not flock mount lock sidecar {} ({errno}); proceeding \
                     without the cross-process mount guard for this read-only mount",
                    lock_path.display(),
                );
                Ok(Self { _lock: None })
            }
            Err((_file, errno)) => Err(io::Error::new(
                io::Error::from(errno).kind(),
                format!(
                    "failed to acquire mount lock on {}: {errno}",
                    lock_path.display(),
                ),
            )),
        }
    }

    /// Open the sidecar fd for the requested `mode`.
    ///
    /// Exclusive mounts open read-write and create the sidecar if it is
    /// missing. Shared mounts only need a readable fd for `flock(LOCK_SH)`,
    /// so they try to create the sidecar (read-write) and, when the
    /// directory is not writable, fall back to opening an existing sidecar
    /// read-only — keeping the guard whenever the sidecar already exists.
    fn open_sidecar(lock_path: &Path, mode: LockMode) -> io::Result<File> {
        let create = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(lock_path);
        match (create, mode) {
            (Ok(file), _) => Ok(file),
            (Err(_), LockMode::Shared) => OpenOptions::new().read(true).open(lock_path),
            (Err(e), LockMode::Exclusive) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// The archive itself does not need to exist: locking only touches
    /// the sidecar file.
    fn archive_path(dir: &TempDir) -> PathBuf {
        dir.path().join("archive.pna")
    }

    #[test]
    fn exclusive_excludes_exclusive() {
        let dir = TempDir::new().unwrap();
        let path = archive_path(&dir);
        let _first = ArchiveLock::acquire(&path, LockMode::Exclusive).unwrap();
        let err = ArchiveLock::acquire(&path, LockMode::Exclusive).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::ResourceBusy);
    }

    #[test]
    fn shared_allows_shared() {
        let dir = TempDir::new().unwrap();
        let path = archive_path(&dir);
        let _first = ArchiveLock::acquire(&path, LockMode::Shared).unwrap();
        let _second = ArchiveLock::acquire(&path, LockMode::Shared)
            .expect("two read-only mounts must be able to share the archive");
    }

    #[test]
    fn shared_excludes_exclusive() {
        let dir = TempDir::new().unwrap();
        let path = archive_path(&dir);
        let _shared = ArchiveLock::acquire(&path, LockMode::Shared).unwrap();
        let err = ArchiveLock::acquire(&path, LockMode::Exclusive).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::ResourceBusy);
    }

    #[test]
    fn exclusive_excludes_shared() {
        let dir = TempDir::new().unwrap();
        let path = archive_path(&dir);
        let _exclusive = ArchiveLock::acquire(&path, LockMode::Exclusive).unwrap();
        let err = ArchiveLock::acquire(&path, LockMode::Shared).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::ResourceBusy);
    }

    #[test]
    fn lock_released_on_drop() {
        let dir = TempDir::new().unwrap();
        let path = archive_path(&dir);
        let first = ArchiveLock::acquire(&path, LockMode::Exclusive).unwrap();
        drop(first);
        let _second = ArchiveLock::acquire(&path, LockMode::Exclusive)
            .expect("dropping the guard must release the lock");
    }

    #[test]
    fn lock_file_persists_after_drop() {
        let dir = TempDir::new().unwrap();
        let path = archive_path(&dir);
        let lock_file = dir.path().join(".archive.pna.lock");
        let guard = ArchiveLock::acquire(&path, LockMode::Exclusive).unwrap();
        assert!(lock_file.exists(), "sidecar lock file must be created");
        drop(guard);
        assert!(
            lock_file.exists(),
            "lock file must never be deleted (unlink races)"
        );
    }

    #[test]
    fn busy_error_mentions_already_mounted() {
        let dir = TempDir::new().unwrap();
        let path = archive_path(&dir);
        let _first = ArchiveLock::acquire(&path, LockMode::Exclusive).unwrap();
        let err = ArchiveLock::acquire(&path, LockMode::Exclusive).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("already mounted"),
            "user-facing message should explain the conflict, got: {msg}"
        );
    }

    #[test]
    fn path_without_file_name_is_invalid_input() {
        let err = ArchiveLock::acquire(Path::new("/"), LockMode::Shared).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn equivalent_spellings_share_one_lock() {
        // The same on-disk archive named two different ways (relative
        // with a `..` detour vs the plain path) must compute the same
        // sidecar inode and therefore conflict — otherwise two `--write`
        // mounts of one archive could both believe they hold it.
        let dir = TempDir::new().unwrap();
        let plain = archive_path(&dir);
        let detour = dir.path().join("sub").join("..").join("archive.pna");
        std::fs::create_dir(dir.path().join("sub")).unwrap();
        let _first = ArchiveLock::acquire(&plain, LockMode::Exclusive).unwrap();
        let err = ArchiveLock::acquire(&detour, LockMode::Exclusive).unwrap_err();
        assert_eq!(
            err.kind(),
            io::ErrorKind::ResourceBusy,
            "two spellings of the same archive must take the same lock"
        );
    }

    #[test]
    fn lock_path_keeps_file_name_byte_faithful() {
        // Two archive names that differ only in distinct non-UTF8 byte
        // runs must NOT collapse onto one sidecar (which would falsely
        // reject the second as already mounted).
        use std::os::unix::ffi::OsStrExt;
        let dir = TempDir::new().unwrap();
        let a = dir.path().join(std::ffi::OsStr::from_bytes(b"\xff.pna"));
        let b = dir.path().join(std::ffi::OsStr::from_bytes(b"\xfe.pna"));
        let lock_a = ArchiveLock::lock_path(&a).unwrap();
        let lock_b = ArchiveLock::lock_path(&b).unwrap();
        assert_ne!(
            lock_a, lock_b,
            "byte-distinct archive names must not share a lock"
        );
    }

    #[test]
    fn shared_lock_degrades_when_sidecar_unopenable() {
        // A read-only mount must not fail just because the sidecar can be
        // neither created nor opened (e.g. read-only media, or a
        // directory the user can read but not write). We provoke a
        // deterministic open failure — independent of uid, so it holds
        // even when the test runs as root — by giving the sidecar a
        // parent that is a regular file rather than a directory, which
        // makes both the create and the read-only fallback fail with
        // ENOTDIR. The mount proceeds without the cross-process guard.
        let dir = TempDir::new().unwrap();
        let not_a_dir = dir.path().join("file");
        std::fs::write(&not_a_dir, b"").unwrap();
        let lock = ArchiveLock {
            _lock: match ArchiveLock::open_sidecar(
                &not_a_dir.join("archive.pna.lock"),
                LockMode::Shared,
            ) {
                Ok(file) => Some(Flock::lock(file, FlockArg::LockSharedNonblock).unwrap()),
                Err(_) => None,
            },
        };
        assert!(
            lock._lock.is_none(),
            "an unopenable sidecar must degrade a shared lock, not error"
        );

        // The same unopenable sidecar must be a hard error for an
        // exclusive (--write) mount, which cannot safely proceed without
        // the guard.
        ArchiveLock::open_sidecar(&not_a_dir.join("archive.pna.lock"), LockMode::Exclusive)
            .expect_err("exclusive mount must not degrade");
    }
}
