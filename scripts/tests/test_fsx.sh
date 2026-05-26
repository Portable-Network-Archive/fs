#!/usr/bin/env bash
# Run fsx-rs (File System eXerciser) against a pnafs --write mount to
# stress the read / write / truncate / mmap paths with reproducible
# pseudorandom I/O.
#
# Reference: https://github.com/asomers/fsx-rs
#
# The script clones fsx-rs into target/fsx-rs (cached across runs),
# builds the release binary into target/fsx-rs/build/ (overriding any
# host CARGO_TARGET_DIR), mounts a fresh pnafs archive in --write mode,
# and runs fsx against a single test file.
#
# Environment overrides:
#   PNA_BIN         Path to the pna CLI       (default: pna).
#   PNAFS_BIN       Path to the pnafs binary  (default: pnafs).
#   FSX_REPO        Git URL                   (default: asomers/fsx-rs).
#   FSX_REF         Branch/tag/SHA            (default: pinned commit).
#   FSX_DIR         Where to clone fsx-rs     (default: target/fsx-rs).
#   FSX_CONFIG      Config file               (default: this dir).
#   FSX_NUMOPS      How many ops to run       (default: 50000 — about a
#                                              minute on Linux x86_64).
#   FSX_SEED        RNG seed                  (default: random per run).
#   FSX_REBUILD=1   Force a fresh fetch + rebuild even if cached.

set -euo pipefail

PNA_BIN="${PNA_BIN:-pna}"
PNAFS_BIN="${PNAFS_BIN:-pnafs}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

FSX_REPO="${FSX_REPO:-https://github.com/asomers/fsx-rs.git}"
# Pinned to master @ 2026-04-17 for reproducibility. Bump deliberately.
FSX_REF="${FSX_REF:-b2337bf49292a6710eb88d700ab83dd7bda87c0d}"
FSX_DIR="${FSX_DIR:-$REPO_ROOT/target/fsx-rs}"
FSX_CONFIG="${FSX_CONFIG:-$SCRIPT_DIR/fsx.toml}"
FSX_NUMOPS="${FSX_NUMOPS:-50000}"

WORKDIR="$(mktemp -d)"
chmod 0755 "$WORKDIR"
ARCHIVE="$WORKDIR/fsx.pna"
MOUNTPOINT="$WORKDIR/mnt"
TESTFILE="$MOUNTPOINT/fsx_target"
ARTIFACTS="$WORKDIR/fsx-artifacts"
mkdir -p "$ARTIFACTS"
MOUNT_PID=""

cleanup() {
  if [ -n "$MOUNT_PID" ]; then
    kill "$MOUNT_PID" 2>/dev/null || true
  fi
  if mount | grep -q "$MOUNTPOINT"; then
    fusermount -u "$MOUNTPOINT" 2>/dev/null || umount "$MOUNTPOINT" 2>/dev/null || true
  fi
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

ensure_fsx() {
  if [ "${FSX_REBUILD:-0}" = "1" ]; then
    rm -rf "$FSX_DIR"
  fi
  if [ ! -d "$FSX_DIR/.git" ]; then
    echo "Cloning fsx-rs into $FSX_DIR (ref: $FSX_REF) ..."
    git clone "$FSX_REPO" "$FSX_DIR"
    git -C "$FSX_DIR" checkout --detach "$FSX_REF"
  else
    local current_sha
    current_sha="$(git -C "$FSX_DIR" rev-parse HEAD)"
    if [ "$current_sha" != "$FSX_REF" ]; then
      echo "Updating fsx-rs checkout to $FSX_REF ..."
      git -C "$FSX_DIR" fetch origin
      git -C "$FSX_DIR" checkout --detach "$FSX_REF"
    fi
  fi
  echo "Building fsx (release) ..."
  # Override any host CARGO_TARGET_DIR so the binary always lands in a
  # predictable spot under FSX_DIR — otherwise we have to chase wherever
  # the env happened to point cargo.
  CARGO_TARGET_DIR="$FSX_DIR/build" \
    cargo build --release --quiet --manifest-path "$FSX_DIR/Cargo.toml"
  FSX_BIN="$FSX_DIR/build/release/fsx"
  test -x "$FSX_BIN" || { echo "FAIL: fsx binary not built"; exit 1; }
}

create_seed_archive() {
  ( cd "$WORKDIR" && echo "seed" > seed.txt && \
    "$PNA_BIN" create --file "$ARCHIVE" --overwrite seed.txt )
  rm -f "$WORKDIR/seed.txt"
}

mount_rw() {
  mkdir -p "$MOUNTPOINT"
  "$PNAFS_BIN" mount --write "$ARCHIVE" "$MOUNTPOINT" &
  MOUNT_PID=$!
  for _ in $(seq 1 20); do
    if mount | grep -q "$MOUNTPOINT"; then break; fi
    sleep 0.5
  done
  mount | grep -q "$MOUNTPOINT" || { echo "FAIL: mount did not succeed"; exit 1; }
}

unmount_wait() {
  fusermount -u "$MOUNTPOINT" 2>/dev/null || umount "$MOUNTPOINT"
  wait "$MOUNT_PID" 2>/dev/null || true
  MOUNT_PID=""
}

main() {
  ensure_fsx
  create_seed_archive
  mount_rw

  : > "$TESTFILE"

  local seed_arg=()
  if [ -n "${FSX_SEED:-}" ]; then
    seed_arg=(-S "$FSX_SEED")
  fi

  echo "Running fsx -N $FSX_NUMOPS against $TESTFILE ..."
  set +e
  "$FSX_BIN" \
    -f "$FSX_CONFIG" \
    -N "$FSX_NUMOPS" \
    -P "$ARTIFACTS" \
    "${seed_arg[@]}" \
    "$TESTFILE" \
    "$@"
  rc=$?
  set -e

  if [ "$rc" -ne 0 ]; then
    echo "fsx reported a failure (exit $rc). Artifacts under: $ARTIFACTS"
    # Copy artifacts out of the about-to-be-deleted workdir so the
    # caller can post-mortem the seed/op log.
    if [ -n "${FSX_ARTIFACTS_OUT:-}" ]; then
      mkdir -p "$FSX_ARTIFACTS_OUT"
      cp -a "$ARTIFACTS"/. "$FSX_ARTIFACTS_OUT/" 2>/dev/null || true
      echo "Artifacts also copied to: $FSX_ARTIFACTS_OUT"
    fi
    unmount_wait
    exit "$rc"
  fi

  unmount_wait
  echo "fsx run complete (no failures)."
}

main "$@"
