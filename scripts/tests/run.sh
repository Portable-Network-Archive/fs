#!/usr/bin/env bash

# Strict error handling
set -eu

# Get the directory where this script is located
SCRIPT_DIR="$(dirname "$0")"

"$SCRIPT_DIR/test_mount.sh"
"$SCRIPT_DIR/test_mount_write.sh"
"$SCRIPT_DIR/test_mount_write_encrypted.sh"
"$SCRIPT_DIR/test_mount_write_strategy.sh"
