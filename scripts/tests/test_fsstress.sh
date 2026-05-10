#!/usr/bin/env bash
# Run fsstress (multi-process FS stress tester from xfstests, packaged
# standalone in billziss-gh/secfs.test) against a pnafs --write mount.
#
# Reference: https://github.com/billziss-gh/secfs.test
#
# fsstress forks N worker processes that each issue M random FS ops
# (mkdir / unlink / rename / write / truncate / chown / etc.) against a
# shared base directory. It is the standard tool for finding races,
# deadlocks, and dirty-tracking bugs under concurrent load. We use it
# to exercise pnafs's RwLock<FileTree> and the (nlink, open_count)
# orphan-collection state machine.
#
# Environment overrides:
#   PNA_BIN            Path to the pna CLI       (default: pna).
#   PNAFS_BIN          Path to the pnafs binary  (default: pnafs).
#   FSSTRESS_REPO      Git URL                   (default: secfs.test).
#   FSSTRESS_REF       Branch/tag/SHA            (default: pinned commit).
#   FSSTRESS_DIR       Where to clone secfs.test (default: target/secfs.test).
#   FSSTRESS_NPROC     Worker processes          (default: 4).
#   FSSTRESS_NOPS      Ops per process per loop  (default: 2000).
#   FSSTRESS_LOOPS     Outer loop count          (default: 1).
#   FSSTRESS_SEED      RNG seed                  (default: random per run).
#   FSSTRESS_REBUILD=1 Force a fresh fetch + rebuild even if cached.

set -euo pipefail

PNA_BIN="${PNA_BIN:-pna}"
PNAFS_BIN="${PNAFS_BIN:-pnafs}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

FSSTRESS_REPO="${FSSTRESS_REPO:-https://github.com/billziss-gh/secfs.test.git}"
# Pinned to master @ 2025-08-30 for reproducibility. Bump deliberately.
FSSTRESS_REF="${FSSTRESS_REF:-edf5eb4a108bfb41073f765aef0cdd32bb3ee1ed}"
FSSTRESS_DIR="${FSSTRESS_DIR:-$REPO_ROOT/target/secfs.test}"
FSSTRESS_NPROC="${FSSTRESS_NPROC:-4}"
FSSTRESS_NOPS="${FSSTRESS_NOPS:-2000}"
FSSTRESS_LOOPS="${FSSTRESS_LOOPS:-1}"

WORKDIR="$(mktemp -d)"
chmod 0755 "$WORKDIR"
ARCHIVE="$WORKDIR/fsstress.pna"
MOUNTPOINT="$WORKDIR/mnt"
TESTROOT="$MOUNTPOINT/work"
MOUNT_PID=""

cleanup() {
  if [ -n "$MOUNT_PID" ]; then
    kill "$MOUNT_PID" 2>/dev/null || true
  fi
  if mount | grep -q "$MOUNTPOINT"; then
    fusermount -u "$MOUNTPOINT" 2>/dev/null || umount "$MOUNTPOINT" 2>/dev/null || true
  fi
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

ensure_fsstress() {
  if [ "${FSSTRESS_REBUILD:-0}" = "1" ]; then
    rm -rf "$FSSTRESS_DIR"
  fi
  if [ ! -d "$FSSTRESS_DIR/.git" ]; then
    echo "Cloning secfs.test into $FSSTRESS_DIR (ref: $FSSTRESS_REF) ..."
    git clone "$FSSTRESS_REPO" "$FSSTRESS_DIR"
    git -C "$FSSTRESS_DIR" checkout --detach "$FSSTRESS_REF"
  else
    local current_sha
    current_sha="$(git -C "$FSSTRESS_DIR" rev-parse HEAD)"
    if [ "$current_sha" != "$FSSTRESS_REF" ]; then
      echo "Updating secfs.test checkout to $FSSTRESS_REF ..."
      git -C "$FSSTRESS_DIR" fetch origin
      git -C "$FSSTRESS_DIR" checkout --detach "$FSSTRESS_REF"
    fi
  fi
  echo "Building fsstress ..."
  make -s -C "$FSSTRESS_DIR/fsstress"
  FSSTRESS_BIN="$FSSTRESS_DIR/fsstress/fsstress"
  test -x "$FSSTRESS_BIN" || { echo "FAIL: fsstress binary not built"; exit 1; }
}

create_seed_archive() {
  ( cd "$WORKDIR" && echo "seed" > seed.txt && \
    "$PNA_BIN" create "$ARCHIVE" --overwrite seed.txt 2>/dev/null || \
    "$PNA_BIN" create --file "$ARCHIVE" --overwrite seed.txt )
  rm -f "$WORKDIR/seed.txt"
}

mount_rw() {
  mkdir -p "$MOUNTPOINT"
  "$PNAFS_BIN" mount --write "$ARCHIVE" "$MOUNTPOINT" &
  MOUNT_PID=$!
  for _ in $(seq 1 20); do
    if mount | grep -q "$MOUNTPOINT"; then break; fi
    sleep 0.5
  done
  mount | grep -q "$MOUNTPOINT" || { echo "FAIL: mount did not succeed"; exit 1; }
}

unmount_wait() {
  fusermount -u "$MOUNTPOINT" 2>/dev/null || umount "$MOUNTPOINT"
  wait "$MOUNT_PID" 2>/dev/null || true
  MOUNT_PID=""
}

main() {
  ensure_fsstress
  create_seed_archive
  mount_rw

  mkdir -p "$TESTROOT"

  # mknod creates char/block/fifo/socket nodes. pnafs accepts them
  # in-memory but the on-disk PNA format has no DataKind for special
  # files yet, so save-time emits one warning per node and drowns the
  # log. pjdfstest already covers special-file semantics; let fsstress
  # focus on the concurrent-write / rename / unlink paths it's actually
  # known for. (Re-enable when PNA grows a special-file DataKind.)
  local op_args=(-f mknod=0)

  local seed_arg=()
  if [ -n "${FSSTRESS_SEED:-}" ]; then
    seed_arg=(-s "$FSSTRESS_SEED")
  fi

  echo "Running fsstress -p $FSSTRESS_NPROC -n $FSSTRESS_NOPS -l $FSSTRESS_LOOPS against $TESTROOT ..."
  set +e
  "$FSSTRESS_BIN" \
    -d "$TESTROOT" \
    -p "$FSSTRESS_NPROC" \
    -n "$FSSTRESS_NOPS" \
    -l "$FSSTRESS_LOOPS" \
    "${op_args[@]}" \
    "${seed_arg[@]}" \
    "$@"
  rc=$?
  set -e

  if [ "$rc" -ne 0 ]; then
    echo "fsstress reported a failure (exit $rc)."
    unmount_wait
    exit "$rc"
  fi

  unmount_wait
  echo "fsstress run complete (no failures)."
}

main "$@"
