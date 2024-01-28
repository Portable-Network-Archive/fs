
set -eu

main() {
  pna create src.pna -r ./src --keep-dir --keep-permission --keep-timestamp --overwrite
  pna-fs mount src.pna ./mnt/pna/src/ &
  while [ ! -e ./mnt/pna/src/src ]; do
    echo "Wait while mount ..."
    sleep 1
  done
  echo "Checking files ..."
  diff -r ./src ./mnt/pna/src/src
  umount ./mnt/pna/src/
  echo "Done."
}

main "$@"
