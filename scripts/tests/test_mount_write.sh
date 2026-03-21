#!/usr/bin/env bash
# Integration tests for pnafs write support (plain archive)
set -euo pipefail

PNA_BIN="${PNA_BIN:-pna}"
PNAFS_BIN="${PNAFS_BIN:-pnafs}"
WORKDIR="$(mktemp -d)"
ARCHIVE="$WORKDIR/test.pna"
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

mount_rw() {
  mkdir -p "$MOUNTPOINT"
  "$PNAFS_BIN" mount --write "$ARCHIVE" "$MOUNTPOINT" &
  MOUNT_PID=$!
  for i in $(seq 1 10); do
    if mount | grep -q "$MOUNTPOINT"; then break; fi
    sleep 0.5
  done
  mount | grep -q "$MOUNTPOINT" || { echo "FAIL: mount did not succeed"; exit 1; }
}

unmount_wait() {
  fusermount -u "$MOUNTPOINT" 2>/dev/null || umount "$MOUNTPOINT"
  wait "$MOUNT_PID" 2>/dev/null || true
  sleep 0.2
}

# Create archive with a seed file using a relative path so the entry name
# inside the archive is just "seed.txt" (not an absolute path).
(cd "$WORKDIR" && echo "seed" > seed.txt && \
  "$PNA_BIN" create "$ARCHIVE" --overwrite seed.txt 2>/dev/null || \
  "$PNA_BIN" create --file "$ARCHIVE" --overwrite seed.txt)
rm -f "$WORKDIR/seed.txt"

echo "=== Test 1: Create file and verify after remount ==="
mount_rw
echo "hello world" > "$MOUNTPOINT/hello.txt"
unmount_wait
mount_rw
CONTENT="$(cat "$MOUNTPOINT/hello.txt")"
[ "$CONTENT" = "hello world" ] || { echo "FAIL: content mismatch: $CONTENT"; exit 1; }
unmount_wait
echo "PASS"

echo "=== Test 2: mkdir and verify after remount ==="
mount_rw
mkdir "$MOUNTPOINT/newdir"
unmount_wait
mount_rw
[ -d "$MOUNTPOINT/newdir" ] || { echo "FAIL: directory missing"; exit 1; }
unmount_wait
echo "PASS"

echo "=== Test 3: unlink file and verify gone after remount ==="
mount_rw
echo "delete me" > "$MOUNTPOINT/todelete.txt"
unmount_wait
mount_rw
rm "$MOUNTPOINT/todelete.txt"
unmount_wait
mount_rw
[ ! -f "$MOUNTPOINT/todelete.txt" ] || { echo "FAIL: file still exists"; exit 1; }
unmount_wait
echo "PASS"

echo "=== Test 4: Overwrite file content ==="
mount_rw
echo "first" > "$MOUNTPOINT/overwrite.txt"
unmount_wait
mount_rw
echo "second" > "$MOUNTPOINT/overwrite.txt"
unmount_wait
mount_rw
CONTENT="$(cat "$MOUNTPOINT/overwrite.txt")"
[ "$CONTENT" = "second" ] || { echo "FAIL: content: $CONTENT"; exit 1; }
unmount_wait
echo "PASS"

echo "=== Test 5: Truncate file to specific size ==="
mount_rw
echo "some data here" > "$MOUNTPOINT/truncate.txt"
unmount_wait
mount_rw
truncate -s 100 "$MOUNTPOINT/truncate.txt"
unmount_wait
mount_rw
SIZE="$(wc -c < "$MOUNTPOINT/truncate.txt")"
[ "$SIZE" -eq 100 ] || { echo "FAIL: size=$SIZE, expected 100"; exit 1; }
unmount_wait
echo "PASS"

echo "=== Test 6: Truncate file to zero ==="
mount_rw
echo "some data" > "$MOUNTPOINT/trunczero.txt"
unmount_wait
mount_rw
truncate -s 0 "$MOUNTPOINT/trunczero.txt"
unmount_wait
mount_rw
SIZE="$(wc -c < "$MOUNTPOINT/trunczero.txt")"
[ "$SIZE" -eq 0 ] || { echo "FAIL: size=$SIZE"; exit 1; }
unmount_wait
echo "PASS"

echo "=== Test 7: mtime survives round-trip ==="
mount_rw
echo "ts" > "$MOUNTPOINT/ts.txt"
TZ=UTC touch -t 202501011200.00 "$MOUNTPOINT/ts.txt"
unmount_wait
mount_rw
MTIME="$(stat -c %Y "$MOUNTPOINT/ts.txt" 2>/dev/null || stat -f %m "$MOUNTPOINT/ts.txt")"
# 2025-01-01 12:00 UTC = 1735732800
[ "$MTIME" -eq 1735732800 ] || { echo "FAIL: mtime=$MTIME, expected 1735732800"; exit 1; }
unmount_wait
echo "PASS"

echo "=== Test 8: Nested file in subdirectory ==="
mount_rw
mkdir -p "$MOUNTPOINT/sub/dir"
echo "nested" > "$MOUNTPOINT/sub/dir/file.txt"
unmount_wait
mount_rw
CONTENT="$(cat "$MOUNTPOINT/sub/dir/file.txt")"
[ "$CONTENT" = "nested" ] || { echo "FAIL: $CONTENT"; exit 1; }
unmount_wait
echo "PASS"

echo "=== Test 9: Read-only mount rejects write ==="
mkdir -p "$MOUNTPOINT"
"$PNAFS_BIN" mount "$ARCHIVE" "$MOUNTPOINT" &
MOUNT_PID=$!
for i in $(seq 1 10); do
  if mount | grep -q "$MOUNTPOINT"; then break; fi
  sleep 0.5
done
mount | grep -q "$MOUNTPOINT" || { echo "FAIL: mount did not succeed"; exit 1; }
if echo "fail" > "$MOUNTPOINT/should_fail.txt" 2>/dev/null; then
  fusermount -u "$MOUNTPOINT" 2>/dev/null || umount "$MOUNTPOINT" || true
  wait "$MOUNT_PID" 2>/dev/null || true
  echo "FAIL: write should have been rejected"
  exit 1
fi
fusermount -u "$MOUNTPOINT" 2>/dev/null || umount "$MOUNTPOINT" || true
wait "$MOUNT_PID" 2>/dev/null || true
echo "PASS"

echo "=== Test 10: Multiple files in one session ==="
mount_rw
for i in 1 2 3 4 5; do
  echo "content$i" > "$MOUNTPOINT/file$i.txt"
done
unmount_wait
mount_rw
for i in 1 2 3 4 5; do
  CONTENT="$(cat "$MOUNTPOINT/file$i.txt")"
  [ "$CONTENT" = "content$i" ] || { echo "FAIL file$i: $CONTENT"; exit 1; }
done
unmount_wait
echo "PASS"

echo "=== Test 11: Empty file via touch ==="
mount_rw
touch "$MOUNTPOINT/empty.txt"
unmount_wait
mount_rw
[ -f "$MOUNTPOINT/empty.txt" ] || { echo "FAIL: empty file missing"; exit 1; }
SIZE="$(wc -c < "$MOUNTPOINT/empty.txt")"
[ "$SIZE" -eq 0 ] || { echo "FAIL: size=$SIZE, expected 0"; exit 1; }
unmount_wait
echo "PASS"

echo "=== Test 12: Binary data round-trip ==="
# Generate reference data outside FUSE, then copy into mount
dd if=/dev/urandom bs=1024 count=4 of="$WORKDIR/binary_expected.dat" 2>/dev/null
mount_rw
cp "$WORKDIR/binary_expected.dat" "$MOUNTPOINT/binary.dat"
unmount_wait
mount_rw
cmp "$MOUNTPOINT/binary.dat" "$WORKDIR/binary_expected.dat" || { echo "FAIL: binary mismatch"; exit 1; }
unmount_wait
echo "PASS"

echo "=== Test 13: Filename with spaces ==="
mount_rw
echo "spaced" > "$MOUNTPOINT/file with spaces.txt"
unmount_wait
mount_rw
CONTENT="$(cat "$MOUNTPOINT/file with spaces.txt")"
[ "$CONTENT" = "spaced" ] || { echo "FAIL: $CONTENT"; exit 1; }
unmount_wait
echo "PASS"

echo "=== Test 14: Overwrite long file with short content ==="
mount_rw
echo "a very long string of content that is quite lengthy" > "$MOUNTPOINT/shrink.txt"
unmount_wait
mount_rw
echo "short" > "$MOUNTPOINT/shrink.txt"
unmount_wait
mount_rw
CONTENT="$(cat "$MOUNTPOINT/shrink.txt")"
[ "$CONTENT" = "short" ] || { echo "FAIL: $CONTENT"; exit 1; }
unmount_wait
echo "PASS"

echo "=== Test 15: rename returns error (not yet supported) ==="
mount_rw
echo "data" > "$MOUNTPOINT/before_rename.txt"
if mv "$MOUNTPOINT/before_rename.txt" "$MOUNTPOINT/after_rename.txt" 2>/dev/null; then
  echo "FAIL: rename should have returned error"
  unmount_wait
  exit 1
fi
unmount_wait
echo "PASS"

echo "=== Test 16: readdir lists all entries ==="
mount_rw
echo "a" > "$MOUNTPOINT/rd_a.txt"
echo "b" > "$MOUNTPOINT/rd_b.txt"
mkdir "$MOUNTPOINT/rd_dir"
ENTRIES="$(ls "$MOUNTPOINT" | grep ^rd_ | sort | tr '\n' ',')"
[ "$ENTRIES" = "rd_a.txt,rd_b.txt,rd_dir," ] || { echo "FAIL: entries=$ENTRIES"; exit 1; }
unmount_wait
echo "PASS"

echo "=== Test 17: stat reports correct file size ==="
mount_rw
echo -n "exactly20byteslong!!" > "$MOUNTPOINT/sized.txt"
SIZE="$(stat -c %s "$MOUNTPOINT/sized.txt" 2>/dev/null || stat -f %z "$MOUNTPOINT/sized.txt")"
[ "$SIZE" -eq 20 ] || { echo "FAIL: size=$SIZE, expected 20"; exit 1; }
unmount_wait
echo "PASS"

echo "=== Test 18: Modify pre-existing archive entry ==="
mount_rw
# seed.txt is in the original archive (created with relative path)
echo "modified seed" > "$MOUNTPOINT/seed.txt"
unmount_wait
mount_rw
CONTENT="$(cat "$MOUNTPOINT/seed.txt")"
[ "$CONTENT" = "modified seed" ] || { echo "FAIL: $CONTENT"; exit 1; }
unmount_wait
echo "PASS"

echo "All write tests passed."
