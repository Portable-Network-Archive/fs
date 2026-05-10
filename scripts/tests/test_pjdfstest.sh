#!/usr/bin/env bash
# Run pjdfstest (Rust port) against a pnafs --write mount for POSIX conformance.
#
# Reference: https://github.com/saidsay-so/pjdfstest
#
# The script clones pjdfstest into target/pjdfstest (gitignored, cached across
# runs), builds the release binary, mounts a fresh pnafs archive in --write
# mode, and runs the suite against a subdirectory of the mount.
#
# Environment overrides:
#   PNA_BIN              Path to the pna CLI (default: pna).
#   PNAFS_BIN            Path to the pnafs binary (default: pnafs).
#   PJDFSTEST_REPO       Git URL of pjdfstest (default: saidsay-so/pjdfstest).
#   PJDFSTEST_REF        Branch/tag/SHA to check out (default: pinned commit).
#   PJDFSTEST_DIR        Where to clone pjdfstest (default: target/pjdfstest).
#   PJDFSTEST_CONFIG     Config file passed to pjdfstest (default: this dir).
#   PJDFSTEST_REBUILD=1       Force a fresh fetch + rebuild even if cached.
#   PJDFSTEST_CREATE_USERS=1  Auto-create the dummy users (`nobody`,
#                             `tests`, `pjdfstest`) pjdfstest needs at
#                             startup. Off by default — the script
#                             writes to /etc/passwd, only sensible in
#                             ephemeral environments (containers, CI).
#
# Extra positional args are forwarded to pjdfstest as test patterns:
#   ./scripts/tests/test_pjdfstest.sh chmod open

set -euo pipefail

PNA_BIN="${PNA_BIN:-pna}"
PNAFS_BIN="${PNAFS_BIN:-pnafs}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

PJDFSTEST_REPO="${PJDFSTEST_REPO:-https://github.com/saidsay-so/pjdfstest.git}"
# Pinned to master @ 2026-05-06 for reproducibility. Bump deliberately.
PJDFSTEST_REF="${PJDFSTEST_REF:-13490bbefc97ab81fe10127b6f518eadb496aa39}"
PJDFSTEST_DIR="${PJDFSTEST_DIR:-$REPO_ROOT/target/pjdfstest}"
PJDFSTEST_CONFIG="${PJDFSTEST_CONFIG:-$SCRIPT_DIR/pjdfstest.toml}"

WORKDIR="$(mktemp -d)"
# mktemp -d defaults to mode 0700, which is fine for the tester process but
# blocks pjdfstest's seteuid()-to-nobody/tests/pjdfstest steps from even
# reaching the mount point — every "as_user" assertion would fail with
# EACCES at path traversal, masquerading as a pnafs permission bug.
chmod 0755 "$WORKDIR"
ARCHIVE="$WORKDIR/pjdfstest.pna"
MOUNTPOINT="$WORKDIR/mnt"
TESTROOT_NAME="pjdfstest_root"
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

ensure_dummy_users() {
  # pjdfstest needs three real system users (nobody, tests, pjdfstest) at
  # startup, even in rootless mode — its config-file deserializer panics
  # otherwise. Creating the missing ones writes to /etc/passwd and
  # /etc/group, which is a host-modifying side effect we do **not** turn
  # on by default. Local runs check and abort with instructions; CI sets
  # `PJDFSTEST_CREATE_USERS=1` because the runner is ephemeral.
  local missing=()
  for user in nobody tests pjdfstest; do
    if ! id "$user" >/dev/null 2>&1; then
      missing+=("$user")
    fi
  done
  if [ "${#missing[@]}" -eq 0 ]; then
    return 0
  fi

  if [ "${PJDFSTEST_CREATE_USERS:-0}" != "1" ]; then
    cat >&2 <<EOF
test_pjdfstest.sh: missing users required by pjdfstest: ${missing[*]}

Create them yourself, e.g.:
  sudo useradd --system --no-create-home --user-group --shell /usr/sbin/nologin tests
  sudo useradd --system --no-create-home --user-group --shell /usr/sbin/nologin pjdfstest

Or rerun this script with PJDFSTEST_CREATE_USERS=1 to have it create
them via useradd. That writes to /etc/passwd and /etc/group on this
host — only sensible in disposable environments (containers, CI).
EOF
    exit 1
  fi

  local sudo_cmd=""
  if [ "$(id -u)" -ne 0 ]; then
    if command -v sudo >/dev/null 2>&1; then
      sudo_cmd="sudo"
    else
      echo "test_pjdfstest.sh: PJDFSTEST_CREATE_USERS=1 set but not root and sudo unavailable" >&2
      exit 1
    fi
  fi
  for user in "${missing[@]}"; do
    # `nobody` is universally present on real systems; if it really is
    # missing we still create it, but it's a strong signal of an unusual
    # environment.
    echo "Creating dummy user '$user' for pjdfstest ..."
    $sudo_cmd useradd --system --no-create-home --user-group --shell /usr/sbin/nologin "$user" || {
      echo "test_pjdfstest.sh: failed to create user '$user'" >&2
      exit 1
    }
  done
}

ensure_pjdfstest() {
  if [ "${PJDFSTEST_REBUILD:-0}" = "1" ]; then
    rm -rf "$PJDFSTEST_DIR"
  fi
  if [ ! -d "$PJDFSTEST_DIR/.git" ]; then
    echo "Cloning pjdfstest into $PJDFSTEST_DIR (ref: $PJDFSTEST_REF) ..."
    git clone "$PJDFSTEST_REPO" "$PJDFSTEST_DIR"
    git -C "$PJDFSTEST_DIR" checkout --detach "$PJDFSTEST_REF"
  else
    CURRENT_SHA="$(git -C "$PJDFSTEST_DIR" rev-parse HEAD)"
    if [ "$CURRENT_SHA" != "$PJDFSTEST_REF" ]; then
      echo "Updating pjdfstest checkout to $PJDFSTEST_REF ..."
      git -C "$PJDFSTEST_DIR" fetch origin
      git -C "$PJDFSTEST_DIR" checkout --detach "$PJDFSTEST_REF"
    fi
  fi
  echo "Building pjdfstest (release) ..."
  (cd "$PJDFSTEST_DIR/rust" && cargo build --release --quiet)
  PJDFSTEST_BIN="$PJDFSTEST_DIR/rust/target/release/pjdfstest"
  test -x "$PJDFSTEST_BIN" || { echo "FAIL: pjdfstest binary not built"; exit 1; }
}

create_seed_archive() {
  # pna create requires at least one input. Use a relative path so the entry
  # name is just `seed.txt` (not absolute), matching the convention in the
  # other test scripts.
  (cd "$WORKDIR" && echo "seed" > seed.txt && \
    "$PNA_BIN" create "$ARCHIVE" --overwrite seed.txt 2>/dev/null || \
    "$PNA_BIN" create --file "$ARCHIVE" --overwrite seed.txt)
  rm -f "$WORKDIR/seed.txt"
}

mount_rw() {
  mkdir -p "$MOUNTPOINT"
  # --allow-other lets pjdfstest's seteuid()-to-nobody/tests/pjdfstest paths
  # actually reach the FUSE filesystem. Without it the kernel rejects every
  # access at the mount point with EACCES before we get a chance to apply
  # POSIX permission semantics.
  "$PNAFS_BIN" mount --write --allow-other "$ARCHIVE" "$MOUNTPOINT" &
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
  ensure_dummy_users
  ensure_pjdfstest
  create_seed_archive
  mount_rw

  # Run pjdfstest in a subdirectory so the seed file (and any pna metadata)
  # do not pollute its working tree.
  mkdir "$MOUNTPOINT/$TESTROOT_NAME"
  # Ensure the test root is world-traversable so pjdfstest's seteuid()
  # tests can reach it.
  chmod 0755 "$MOUNTPOINT/$TESTROOT_NAME"

  echo "Running pjdfstest against $MOUNTPOINT/$TESTROOT_NAME ..."
  set +e
  "$PJDFSTEST_BIN" \
    --configuration-file "$PJDFSTEST_CONFIG" \
    --path "$MOUNTPOINT/$TESTROOT_NAME" \
    "$@"
  rc=$?
  set -e

  unmount_wait

  if [ "$rc" -ne 0 ]; then
    echo "pjdfstest reported failures (exit $rc)."
    echo "Triage: missing pnafs feature -> add to expected_failures in"
    echo "        $PJDFSTEST_CONFIG"
    echo "        Real bug -> fix in pnafs."
    exit "$rc"
  fi
  echo "pjdfstest run complete (no failures)."
}

main "$@"
