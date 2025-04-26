
set -eu

PNA_BIN="${PNA_BIN:-pna}"
PNAFS_BIN="${PNAFS_BIN:-pnafs}"
PASSWORD="password"

cleanup() {
  echo "Cleaning up..."
  if mount | grep -q ./mnt/pna/src/; then
    fusermount -u ./mnt/pna/src/ || umount ./mnt/pna/src/
  fi
  rm -rf ./mnt/pna/src ./src.pna
  echo "Done."
}

run() {
  $PNA_BIN create src.pna -r ./src --overwrite $PNA_OPTIONS
  $PNAFS_BIN mount src.pna ./mnt/pna/src/ $PNA_FS_OPTIONS &
  PID="$!"
  while [ ! -e ./mnt/pna/src/src ]; do
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
  diff -r ./src ./mnt/pna/src/src
  umount ./mnt/pna/src/
  echo "Done."
}

main() {
  trap cleanup EXIT
  PNA_OPTIONS="--keep-permission --keep-timestamp --keep-xattr" PNA_FS_OPTIONS="" run
  PNA_OPTIONS="--keep-dir --keep-permission --keep-timestamp --keep-xattr" PNA_FS_OPTIONS="" run
  PNA_OPTIONS="--password $PASSWORD" PNA_FS_OPTIONS="--password $PASSWORD" run
}

main "$@"
