#!/usr/bin/env bash
# Integration tests for pnafs write support (encrypted archive)
set -euo pipefail

PNA_BIN="${PNA_BIN:-pna}"
PNAFS_BIN="${PNAFS_BIN:-pnafs}"
WORKDIR="$(mktemp -d)"
ARCHIVE="$WORKDIR/enc.pna"
MOUNTPOINT="$WORKDIR/mnt"
PASSWORD="testpassword123"

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

# Create encrypted archive with one file (use relative path for predictable entry name)
(cd "$WORKDIR" && echo "original" > seed.txt && \
  "$PNA_BIN" create "$ARCHIVE" --overwrite --password "$PASSWORD" seed.txt 2>/dev/null || \
  "$PNA_BIN" create --file "$ARCHIVE" --overwrite --password "$PASSWORD" seed.txt)
rm -f "$WORKDIR/seed.txt"

mount_enc() {
  mkdir -p "$MOUNTPOINT"
  "$PNAFS_BIN" mount --write --password "$PASSWORD" "$ARCHIVE" "$MOUNTPOINT" &
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

echo "=== Encrypted Test 1: Create new file in encrypted archive ==="
mount_enc
echo "new encrypted content" > "$MOUNTPOINT/newfile.txt"
unmount_wait

# Verify by remounting with correct password
mount_enc
CONTENT="$(cat "$MOUNTPOINT/newfile.txt")"
[ "$CONTENT" = "new encrypted content" ] || { echo "FAIL: $CONTENT"; exit 1; }
unmount_wait
echo "PASS"

echo "=== Encrypted Test 2: Overwrite file in encrypted archive ==="
mount_enc
echo "overwritten" > "$MOUNTPOINT/newfile.txt"
unmount_wait
mount_enc
CONTENT="$(cat "$MOUNTPOINT/newfile.txt")"
[ "$CONTENT" = "overwritten" ] || { echo "FAIL: $CONTENT"; exit 1; }
unmount_wait
echo "PASS"

echo "=== Encrypted Test 3: Mount without password rejects or fails ==="
mkdir -p "$MOUNTPOINT"
# Mount encrypted archive without password in background; expect failure or no mount
"$PNAFS_BIN" mount --write "$ARCHIVE" "$MOUNTPOINT" 2>/dev/null &
TEST3_PID=$!
sleep 2
if mount | grep -q "$MOUNTPOINT"; then
  # Mounted — try writing (should fail)
  if echo "bad" > "$MOUNTPOINT/bad.txt" 2>/dev/null; then
    fusermount -u "$MOUNTPOINT" 2>/dev/null || umount "$MOUNTPOINT" || true
    wait "$TEST3_PID" 2>/dev/null || true
    echo "FAIL: write should not succeed without password"
    exit 1
  fi
  fusermount -u "$MOUNTPOINT" 2>/dev/null || umount "$MOUNTPOINT" || true
fi
wait "$TEST3_PID" 2>/dev/null || true
echo "PASS"

echo "All encrypted write tests passed."
