
set -eu

PASSWORD="password"

run_with() {
  EXTRA_OPTIONS=$*
  pna create src.pna -r ./src --keep-dir --keep-permission --keep-timestamp --overwrite $EXTRA_OPTIONS
  pnafs mount src.pna ./mnt/pna/src/ $EXTRA_OPTIONS &
  PID=$(echo $!)
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
  run_with
  run_with --password "$PASSWORD"
}

main "$@"
