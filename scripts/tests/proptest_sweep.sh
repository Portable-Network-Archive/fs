#!/usr/bin/env bash
# Heavy-duty proptest sweep, intended to run before a release or after
# touching anything in `src/archive_io.rs` / `src/file_tree.rs` /
# `src/roundtrip_proptest.rs`. The in-tree default of 64 cases keeps
# `cargo test` fast for the iteration loop; this script widens the
# sample so bugs that need a thousand cases to surface still get a
# chance to show up before they reach users.
#
# Defaults can be overridden via env:
#   PROPTEST_CASES   How many cases per property (default 100000)
#   FILTER           Test-name substring to scope the sweep
#                    (default: all `roundtrip_proptest` tests)
#
# Examples:
#   ./scripts/tests/proptest_sweep.sh
#   PROPTEST_CASES=10000 ./scripts/tests/proptest_sweep.sh
#   FILTER=plain ./scripts/tests/proptest_sweep.sh
#
# Encrypted properties cap at `cases(8)` in source — they share the
# `PROPTEST_CASES` env, so a 100000 setting also widens the encrypted
# block proportionally. Expect ~30 min total wall time at the default
# setting on a modern x86_64 runner; the encrypted Argon2id key
# derivation dominates.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

PROPTEST_CASES="${PROPTEST_CASES:-100000}"
FILTER="${FILTER:-roundtrip_proptest}"

echo "Running roundtrip property tests at PROPTEST_CASES=$PROPTEST_CASES (filter: $FILTER)"
echo "Repo root: $REPO_ROOT"
echo

export PROPTEST_CASES

# Forward extra args so a developer can do e.g.
# `./scripts/tests/proptest_sweep.sh -- --nocapture`.
cd "$REPO_ROOT"
exec cargo test --locked --release "$FILTER" "$@"
