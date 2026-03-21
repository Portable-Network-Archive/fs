#!/usr/bin/env bash
# Integration tests for pnafs write strategies (lazy / immediate)
set -euo pipefail

PNA_BIN="${PNA_BIN:-pna}"
PNAFS_BIN="${PNAFS_BIN:-pnafs}"
WORKDIR="$(mktemp -d)"
ARCHIVE="$WORKDIR/strategy.pna"
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

# Create archive with a seed file using a relative path so the entry name
# inside the archive is just "seed.txt" (not an absolute path).
(cd "$WORKDIR" && echo "seed" > seed.txt && \
  "$PNA_BIN" create "$ARCHIVE" --overwrite seed.txt 2>/dev/null || \
  "$PNA_BIN" create --file "$ARCHIVE" --overwrite seed.txt)
rm -f "$WORKDIR/seed.txt"

mount_with_strategy() {
  mkdir -p "$MOUNTPOINT"
  "$PNAFS_BIN" mount --write --write-strategy "$1" "$ARCHIVE" "$MOUNTPOINT" &
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

echo "=== Strategy Test 1: lazy — file persisted after unmount ==="
mount_with_strategy lazy
echo "lazy content" > "$MOUNTPOINT/lazy.txt"
unmount_wait
mount_with_strategy lazy
CONTENT="$(cat "$MOUNTPOINT/lazy.txt")"
[ "$CONTENT" = "lazy content" ] || { echo "FAIL: $CONTENT"; exit 1; }
unmount_wait
echo "PASS"

echo "=== Strategy Test 2: immediate — file persisted ==="
mount_with_strategy immediate
echo "immediate content" > "$MOUNTPOINT/immediate.txt"
unmount_wait
mount_with_strategy immediate
CONTENT="$(cat "$MOUNTPOINT/immediate.txt")"
[ "$CONTENT" = "immediate content" ] || { echo "FAIL: $CONTENT"; exit 1; }
unmount_wait
echo "PASS"

echo "=== Strategy Test 3: lazy — multiple files all persisted ==="
mount_with_strategy lazy
for i in 1 2 3; do echo "data$i" > "$MOUNTPOINT/multi$i.txt"; done
unmount_wait
mount_with_strategy lazy
for i in 1 2 3; do
  C="$(cat "$MOUNTPOINT/multi$i.txt")"
  [ "$C" = "data$i" ] || { echo "FAIL multi$i: $C"; exit 1; }
done
unmount_wait
echo "PASS"

echo "=== Strategy Test 4: immediate — multiple files all persisted ==="
mount_with_strategy immediate
for i in 1 2 3; do echo "imm$i" > "$MOUNTPOINT/imm$i.txt"; done
unmount_wait
mount_with_strategy immediate
for i in 1 2 3; do
  C="$(cat "$MOUNTPOINT/imm$i.txt")"
  [ "$C" = "imm$i" ] || { echo "FAIL imm$i: $C"; exit 1; }
done
unmount_wait
echo "PASS"

echo "All write-strategy tests passed."
