# Mount-level tests

Shell harnesses that exercise `pnafs` end-to-end against a real FUSE
mount. Unit and integration tests in Rust live alongside the source
under `src/`; the harnesses listed below cover the cases that can only
be verified through an actual mount point.

```bash
./scripts/tests/run.sh                  # mount / round-trip checks (FUSE)
./scripts/tests/test_pjdfstest.sh       # POSIX conformance via pjdfstest
./scripts/tests/test_fsx.sh             # randomised I/O via fsx-rs
./scripts/tests/test_fsstress.sh        # multi-process stress via fsstress
```

Each external tool is cloned into `target/<tool>/` on first run
(cached, pinned to a known-good commit), built, and exercised against
a fresh `pnafs` mount opened with the `--write` flag (a read-write
mount; without `--write` the filesystem is read-only). Override the
upstream pin via the `*_REF` environment variable when you bump it
deliberately.

## `run.sh` — mount / round-trip checks

Smoke tests that mount an archive, perform basic operations
(create / write / read / unmount / re-read), and verify durability
under both write strategies pnafs supports: `--write-strategy lazy`
(the archive is rewritten only on unmount) and
`--write-strategy immediate` (the archive is rewritten on every
file close). Plain and encrypted archive variants are covered.

## `test_pjdfstest.sh` — POSIX conformance

Runs the Rust port of [pjdfstest][1] against the mount. Of the 398
tests in the suite, **382 pass and 16 are skipped** on Linux — every
test that runs passes; the skipped ones need pjdfstest's `chflags`
(BSD-only flag), NFSv4 ACLs, or `allow_remount` opt-ins to even be
attempted. `allow_remount` in particular cannot be honoured because
the kernel FUSE driver has no `reconfigure` op, so an in-place
`mount -o remount,ro` of any FUSE filesystem fails before the request
reaches user space. `pjdfstest.toml` is the live spec for what must
pass; pjdfstest's `[features]` toggles for capabilities pnafs supports
(`posix_fallocate`, `rename_ctime`, `utimensat`, `utime_now`,
`stat_st_birthtime`) are enabled there.

**Host requirements:** `libacl1-dev` plus three system users pjdfstest
insists on at startup — `nobody`, `tests`, `pjdfstest`. The script
will not create them on your host by default; create them yourself,
or pass `PJDFSTEST_CREATE_USERS=1` (the `posix_conformance` job in
`.github/workflows/test.yml` does this on its ephemeral runner; only
sensible on a disposable host) to have the script run `useradd` for
the missing ones.

**Triage failures** in `pjdfstest.toml` (next to this README): the
`expected_failures` TOML array is the allow-list of test names that
may fail without breaking the build. Add an entry only when the
failure is a missing pnafs feature you don't intend to implement
immediately; fix real bugs in pnafs instead. A bloated
`expected_failures` list hides regressions, so it ships empty.

[1]: https://github.com/saidsay-so/pjdfstest

## `test_fsx.sh` — randomised I/O

Runs [fsx-rs][2] (Rust port of Apple's File System eXerciser) for
`FSX_NUMOPS=50000` operations across read / write / truncate / mmap /
sendfile / posix_fadvise / posix_fallocate / punch_hole /
copy_file_range. `fsx.toml` (next to this README) tunes per-op
weights.

Failures are reproducible: fsx prints the seed plus a per-op log, and
the `fsx` job in `.github/workflows/test.yml` uploads the artifact
directory on failure. Replay with `FSX_SEED=<n>`.

[2]: https://github.com/asomers/fsx-rs

## `test_fsstress.sh` — multi-process stress

Builds the `fsstress` packaged in [secfs.test][3] and runs
`FSSTRESS_NPROC=4` worker processes, each issuing
`FSSTRESS_NOPS=2000` random FS ops (mkdir / unlink / rename / write /
truncate / chown / ...) against a shared subtree. The point of this
harness is concurrent access from multiple processes at once: it
surfaces races and lock-ordering issues in pnafs's in-memory tree,
and exercises the lifecycle of inodes that are unlinked while a
file descriptor is still open. The suites above (`run.sh`,
pjdfstest, fsx) drive the mount from a single process at a time and
do not reach those code paths.

`mknod` is zeroed via `-f mknod=0`. pnafs accepts special files
(block / char / fifo / socket) at runtime but the on-disk PNA format
has no entry kind for them yet, so when pnafs writes the archive
back to disk on unmount it logs a "cannot represent special-file
nodes" warning per node — pjdfstest already exercises the runtime
semantics, and leaving mknod on here only floods the output.
Reproducible with `FSSTRESS_SEED=<n>`.

[3]: https://github.com/billziss-gh/secfs.test
