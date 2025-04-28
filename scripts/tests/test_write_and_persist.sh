#!/usr/bin/env bash
set -euo pipefail

PNA_BIN="${PNA_BIN:-pna}"
PNAFS_BIN="${PNAFS_BIN:-pnafs}"
PASSWORD="password"
SRC_DIR="test_work/src"
ARCHIVE="test_work/test.pna"
MOUNTPOINT="test_work/mnt"
EXISTING_FILE="existing.txt"
NEW_FILE="newfile.txt"

cleanup() {
  echo "Cleaning up..."
  if mount | grep -q "$MOUNTPOINT"; then
    fusermount -u "$MOUNTPOINT" || umount "$MOUNTPOINT"
  fi
  rm -rf ./test_work
  echo "Done."
}

initialize() {
  echo "Creating test source directory..."
  mkdir -p "$SRC_DIR"
  echo "This is original content." > "$SRC_DIR/$EXISTING_FILE"
}

run() {
  $PNA_BIN create "$ARCHIVE" -r "$SRC_DIR" --overwrite
  $PNAFS_BIN mount --read-write "$ARCHIVE" "$MOUNTPOINT" &
  PID="$!"

  while [ ! -e "$MOUNTPOINT/$SRC_DIR" ]; do
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

  echo "This is a new file." > "$MOUNTPOINT/$SRC_DIR/$NEW_FILE"

  echo "Modifying existing file..."
  echo "Appended line." >> "$MOUNTPOINT/$SRC_DIR/$EXISTING_FILE"

  echo "Unmounting filesystem..."
  fusermount -u "$MOUNTPOINT" || umount "$MOUNTPOINT"

  echo "Verifying new file content..."
  grep -q "This is a new file." "$MOUNTPOINT/$SRC_DIR/$NEW_FILE" && echo "New file OK"

  echo "Verifying modified existing file..."
  grep -q "Appended line." "$MOUNTPOINT/$SRC_DIR/$EXISTING_FILE" && echo "Modification OK"

  echo "Done."
}

main() {
  trap cleanup EXIT
  initialize
  run
}

main "$@"
