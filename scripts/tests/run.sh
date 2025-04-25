
set -eu

PASSWORD="password"

run() {
  pna create src.pna -r ./src --overwrite $PNA_OPTIONS
  pnafs mount src.pna ./mnt/pna/src/ $PNA_FS_OPTIONS &
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
  PNA_OPTIONS="--keep-permission --keep-timestamp --keep-xattr" PNA_FS_OPTIONS="" run
  PNA_OPTIONS="--keep-dir --keep-permission --keep-timestamp --keep-xattr" PNA_FS_OPTIONS="" run
  PNA_OPTIONS="--password $PASSWORD" PNA_FS_OPTIONS="--password $PASSWORD" run
}

main "$@"
