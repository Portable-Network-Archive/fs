#!/usr/bin/env bash
# Round-trip pnafs mount tests: archive a real directory tree, mount it,
# and `diff -r` against the original. Exercised three times — default,
# --keep-dir, and password-encrypted — using the project's own `src/`
# tree as the input corpus.
#
# Archive and mountpoint live inside `mktemp -d`, so the script never
# writes to or deletes anything in the caller's CWD. The pna `create -r`
# step does need a relative path (otherwise the archive entries pick up
# absolute prefixes), so we cd into the project root first.
set -eu

PNA_BIN="${PNA_BIN:-pna}"
PNAFS_BIN="${PNAFS_BIN:-pnafs}"
PASSWORD="password"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

WORKDIR="$(mktemp -d)"
ARCHIVE="$WORKDIR/src.pna"
MOUNTPOINT="$WORKDIR/mnt"
mkdir -p "$MOUNTPOINT"

cleanup() {
  echo "Cleaning up..."
  if mount | grep -q "$MOUNTPOINT"; then
    fusermount -u "$MOUNTPOINT" 2>/dev/null || umount "$MOUNTPOINT" 2>/dev/null || true
  fi
  rm -rf "$WORKDIR"
  echo "Done."
}

run() {
  ( cd "$PROJECT_ROOT" && $PNA_BIN create --file "$ARCHIVE" -r src --overwrite $PNA_OPTIONS )
  $PNAFS_BIN mount "$ARCHIVE" "$MOUNTPOINT" $PNA_FS_OPTIONS &
  PID="$!"
  while [ ! -e "$MOUNTPOINT/src" ]; do
    echo "Wait while mount ..."
    sleep 1
    set +eu
    ps -p "$PID" > /dev/null
    EXIT_CODE=$?
    set -eu
    if [ $EXIT_CODE -ne 0 ]; then
      echo "mount process failed"
      break
    fi
  done
  echo "Checking files ..."
  diff -r "$PROJECT_ROOT/src" "$MOUNTPOINT/src"
  umount "$MOUNTPOINT"
  echo "Done."
}

main() {
  trap cleanup EXIT
  PNA_OPTIONS="--keep-permission --keep-timestamp --keep-xattr" PNA_FS_OPTIONS="" run
  PNA_OPTIONS="--keep-dir --keep-permission --keep-timestamp --keep-xattr" PNA_FS_OPTIONS="" run
  PNA_OPTIONS="--password $PASSWORD" PNA_FS_OPTIONS="--password $PASSWORD" run
}

main "$@"
