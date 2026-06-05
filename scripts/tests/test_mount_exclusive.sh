#!/usr/bin/env bash
# Regression test: the mount-lifetime archive lock must reject
# conflicting concurrent mounts of the same archive.
#
# Rules (src/archive_lock.rs): read-only mounts take a shared flock on
# the sidecar `.{name}.lock` file and may coexist; a --write mount takes
# an exclusive flock and excludes every other mount in either mode.
#
# Archive and mountpoints live inside `mktemp -d`; deterministic and
# self-cleaning.
set -euo pipefail

PNA_BIN="${PNA_BIN:-pna}"
PNAFS_BIN="${PNAFS_BIN:-pnafs}"

# Resolve symlinks in the temp path (pwd -P): on macOS `mktemp -d`
# returns /var/... but the kernel reports FUSE mounts under the real
# /private/var/..., which would make the `mount | grep " on $mp "`
# readiness checks below miss their own mountpoints and time out.
WORKDIR="$(cd "$(mktemp -d)" && pwd -P)"
ARCHIVE="$WORKDIR/excl.pna"
MNT_A="$WORKDIR/mnt_a"
MNT_B="$WORKDIR/mnt_b"

cleanup() {
  for pid in "${MOUNT_PID_A:-}" "${MOUNT_PID_B:-}"; do
    [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
  done
  for mp in "$MNT_A" "$MNT_B"; do
    if mount | grep -qF " on $mp "; then
      fusermount -u "$mp" 2>/dev/null || umount "$mp" 2>/dev/null || true
    fi
  done
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

fail() {
  echo "FAIL: $1"
  echo "  archive path: $ARCHIVE"
  exit 1
}

# Mount in the background and wait for the FUSE mount to appear.
# $1 = mountpoint, $2 = pid variable name, remaining args = extra flags.
mount_bg() {
  local mp="$1" pid_var="$2"
  shift 2
  mkdir -p "$mp"
  "$PNAFS_BIN" mount "$@" "$ARCHIVE" "$mp" &
  printf -v "$pid_var" '%s' "$!"
  for _ in $(seq 1 20); do
    if mount | grep -qF " on $mp "; then return 0; fi
    sleep 0.5
  done
  fail "mount on $mp did not come up"
}

# Expect a mount attempt to be rejected quickly with the lock error.
# Remaining args = pnafs mount arguments.
expect_mount_rejected() {
  local out rc
  set +e
  out="$(timeout 10 "$PNAFS_BIN" mount "$@" 2>&1)"
  rc=$?
  set -e
  if [ "$rc" -eq 124 ]; then
    fail "conflicting mount was not rejected (still running after 10s): pnafs mount $*"
  fi
  if [ "$rc" -eq 0 ]; then
    fail "conflicting mount unexpectedly succeeded: pnafs mount $*"
  fi
  echo "$out" | grep -qi "already mounted" ||
    fail "rejection message should mention 'already mounted', got: $out"
}

unmount_wait() {
  local mp="$1" pid_var="$2"
  fusermount -u "$mp" 2>/dev/null || umount "$mp"
  wait "${!pid_var}" 2>/dev/null || true
  printf -v "$pid_var" ''
  sleep 0.2
}

# Seed a minimal archive.
(
  cd "$WORKDIR"
  echo "seed" > seed.txt
  "$PNA_BIN" create --file "$ARCHIVE" --overwrite seed.txt
)
rm -f "$WORKDIR/seed.txt"

echo "=== exclusive lock: write mount rejects a second write mount ==="
mkdir -p "$MNT_B"
mount_bg "$MNT_A" MOUNT_PID_A --write
expect_mount_rejected --write "$ARCHIVE" "$MNT_B"
echo "PASS"

echo "=== exclusive lock: write mount rejects a read-only mount ==="
expect_mount_rejected "$ARCHIVE" "$MNT_B"
echo "PASS"

unmount_wait "$MNT_A" MOUNT_PID_A

echo "=== shared lock: two read-only mounts coexist ==="
mount_bg "$MNT_A" MOUNT_PID_A
mount_bg "$MNT_B" MOUNT_PID_B
cat "$MNT_A/seed.txt" >/dev/null || fail "read through mount A failed"
cat "$MNT_B/seed.txt" >/dev/null || fail "read through mount B failed"
echo "PASS"

echo "=== shared lock: read-only mounts reject a write mount ==="
expect_mount_rejected --write "$ARCHIVE" "$WORKDIR/mnt_c"
echo "PASS"

unmount_wait "$MNT_B" MOUNT_PID_B
unmount_wait "$MNT_A" MOUNT_PID_A

echo "=== lock released after unmount: write mount succeeds again ==="
mount_bg "$MNT_A" MOUNT_PID_A --write
unmount_wait "$MNT_A" MOUNT_PID_A
echo "PASS"

echo "=== sidecar lock file is left in place by design ==="
[ -e "$WORKDIR/.excl.pna.lock" ] || fail "expected sidecar lock file to persist"
echo "PASS"

echo "All exclusive-mount tests passed."
