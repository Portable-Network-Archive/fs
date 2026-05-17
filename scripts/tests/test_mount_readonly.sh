#!/usr/bin/env bash
# Regression test: a read-only mount (no --write) must reject every
# mutating operation with EROFS and must not alter the archive bytes.
#
# `PnaFS::require_writable` (src/filesystem.rs) returns EROFS whenever no
# write strategy is configured, and that gate fronts the mutating FUSE
# ops. `test_mount.sh` only smoke-tests reads; this asserts the failures.
#
# Archive and mountpoint live inside `mktemp -d`, so the script never
# writes to or deletes anything in the caller's CWD. Deterministic and
# self-cleaning.
set -euo pipefail

PNA_BIN="${PNA_BIN:-pna}"
PNAFS_BIN="${PNAFS_BIN:-pnafs}"

WORKDIR="$(mktemp -d)"
ARCHIVE="$WORKDIR/ro.pna"
MOUNTPOINT="$WORKDIR/mnt"

cleanup() {
  if [ -n "${MOUNT_PID:-}" ]; then
    kill "$MOUNT_PID" 2>/dev/null || true
  fi
  if mount | grep -q "$MOUNTPOINT"; then
    fusermount -u "$MOUNTPOINT" 2>/dev/null || umount "$MOUNTPOINT" 2>/dev/null || true
  fi
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

# Report context then bail. $1 is the operation that misbehaved.
fail() {
  echo "FAIL: $1"
  echo "  mount dir:    $MOUNTPOINT"
  echo "  archive path: $ARCHIVE"
  echo "  failing op:   $2"
  exit 1
}

mount_ro() {
  mkdir -p "$MOUNTPOINT"
  "$PNAFS_BIN" mount "$ARCHIVE" "$MOUNTPOINT" &
  MOUNT_PID=$!
  for _ in $(seq 1 20); do
    if mount | grep -q "$MOUNTPOINT"; then break; fi
    sleep 0.5
  done
  mount | grep -q "$MOUNTPOINT" || fail "mount did not succeed" "pnafs mount (read-only)"
}

unmount_wait() {
  fusermount -u "$MOUNTPOINT" 2>/dev/null || umount "$MOUNTPOINT"
  wait "$MOUNT_PID" 2>/dev/null || true
  MOUNT_PID=""
  sleep 0.2
}

# Seed an archive holding one regular file and one directory, using
# relative paths so the entry names are not absolute.
(
  cd "$WORKDIR"
  echo "original content" > seed.txt
  mkdir seeddir
  echo "nested" > seeddir/inner.txt
  "$PNA_BIN" create "$ARCHIVE" --overwrite seed.txt seeddir 2>/dev/null ||
    "$PNA_BIN" create --file "$ARCHIVE" --overwrite seed.txt seeddir
)
rm -rf "$WORKDIR/seed.txt" "$WORKDIR/seeddir"

# Byte snapshot of the archive before any mount activity.
SUM_BEFORE="$(sha256sum "$ARCHIVE" | awk '{print $1}')"

mount_ro

echo "=== read-only mount: create file is rejected ==="
if sh -c ': > "$1"' _ "$MOUNTPOINT/new_file.txt" 2>/dev/null; then
  fail "create file was not rejected on a read-only mount" "create $MOUNTPOINT/new_file.txt"
fi
[ ! -e "$MOUNTPOINT/new_file.txt" ] ||
  fail "create file left an entry behind" "create $MOUNTPOINT/new_file.txt"
echo "PASS"

echo "=== read-only mount: mkdir is rejected ==="
if mkdir "$MOUNTPOINT/new_dir" 2>/dev/null; then
  fail "mkdir was not rejected on a read-only mount" "mkdir $MOUNTPOINT/new_dir"
fi
[ ! -e "$MOUNTPOINT/new_dir" ] ||
  fail "mkdir left an entry behind" "mkdir $MOUNTPOINT/new_dir"
echo "PASS"

echo "=== read-only mount: unlink is rejected ==="
if rm -f "$MOUNTPOINT/seed.txt" 2>/dev/null; then
  fail "unlink was not rejected on a read-only mount" "rm $MOUNTPOINT/seed.txt"
fi
[ -e "$MOUNTPOINT/seed.txt" ] ||
  fail "unlink removed the entry on a read-only mount" "rm $MOUNTPOINT/seed.txt"
echo "PASS"

echo "=== read-only mount: write to existing file is rejected ==="
if sh -c 'echo overwrite > "$1"' _ "$MOUNTPOINT/seed.txt" 2>/dev/null; then
  fail "write was not rejected on a read-only mount" "write $MOUNTPOINT/seed.txt"
fi
CONTENT="$(cat "$MOUNTPOINT/seed.txt")"
[ "$CONTENT" = "original content" ] ||
  fail "write mutated file content on a read-only mount" "write $MOUNTPOINT/seed.txt"
echo "PASS"

echo "=== read-only mount: truncate is rejected ==="
if truncate -s 0 "$MOUNTPOINT/seed.txt" 2>/dev/null; then
  fail "truncate was not rejected on a read-only mount" "truncate $MOUNTPOINT/seed.txt"
fi
SIZE="$(wc -c < "$MOUNTPOINT/seed.txt")"
[ "$SIZE" -eq 17 ] ||
  fail "truncate changed the file size on a read-only mount (size=$SIZE)" "truncate $MOUNTPOINT/seed.txt"
echo "PASS"

unmount_wait

echo "=== read-only mount: archive bytes unchanged after unmount ==="
SUM_AFTER="$(sha256sum "$ARCHIVE" | awk '{print $1}')"
if [ "$SUM_BEFORE" != "$SUM_AFTER" ]; then
  fail "archive bytes changed after a read-only mount session ($SUM_BEFORE -> $SUM_AFTER)" "unmount"
fi
echo "PASS"

echo "All read-only mount tests passed."
