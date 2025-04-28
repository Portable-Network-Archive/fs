#!/usr/bin/env bash

# Strict error handling
set -eu

# Get the directory where this script is located
SCRIPT_DIR="$(dirname "$0")"
MOUNT_TEST_SCRIPT="${SCRIPT_DIR}/test_mount.sh"
WRITE_AND_PERSIST_SCRIPT="${SCRIPT_DIR}/test_write_and_persist.sh"

# Check if mount test script exists
if [ ! -f "$MOUNT_TEST_SCRIPT" ]; then
    echo "Error: Mount test script not found at: $MOUNT_TEST_SCRIPT" >&2
    exit 1
fi

# Check if write and persist test script exists
if [ ! -f "$WRITE_AND_PERSIST_SCRIPT" ]; then
    echo "Error: Mount test script not found at: $WRITE_AND_PERSIST_SCRIPT" >&2
    exit 1
fi

# Execute the mount test script
"$MOUNT_TEST_SCRIPT"
# Execute the write and persist test script
"$WRITE_AND_PERSIST_SCRIPT"
