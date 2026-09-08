#!/usr/bin/env bash
set -euo pipefail

package_file="${1:-nix/portable-network-archive.nix}"
repo="ChanTsune/Portable-Network-Archive"
release="$(curl -fsSL "https://api.github.com/repos/${repo}/releases/latest")"
tag="$(jq -r '.tag_name' <<<"$release")"
version="${tag#portable-network-archive-}"

if [[ -z "$version" || "$version" == "$tag" ]]; then
  echo "unexpected release tag: $tag" >&2
  exit 1
fi

asset_digest() {
  local target="$1"
  local name="portable-network-archive-${target}.tar.xz"
  local digest
  digest="$(jq -r --arg name "$name" '.assets[] | select(.name == $name) | .digest' <<<"$release")"
  if [[ -z "$digest" || "$digest" == "null" || "$digest" != sha256:* ]]; then
    echo "missing SHA-256 digest for $name" >&2
    exit 1
  fi
  printf '%s' "${digest#sha256:}"
}

x86_hex="$(asset_digest x86_64-unknown-linux-gnu)"
arm_hex="$(asset_digest aarch64-unknown-linux-gnu)"

sri() {
  python3 - "$1" <<'PY'
import base64
import sys
print("sha256-" + base64.b64encode(bytes.fromhex(sys.argv[1])).decode())
PY
}

x86_hash="$(sri "$x86_hex")"
arm_hash="$(sri "$arm_hex")"

VERSION="$version" X86_HASH="$x86_hash" ARM_HASH="$arm_hash" python3 - "$package_file" <<'PY'
import os
import re
import sys

path = sys.argv[1]
text = open(path, encoding="utf-8").read()

text, count = re.subn(r'version = "[^"]+";', f'version = "{os.environ["VERSION"]}";', text, count=1)
if count != 1:
    raise SystemExit("failed to update version")
text, count = re.subn(
    r'(x86_64-linux = \{\n\s+target = "x86_64-unknown-linux-gnu";\n\s+hash = ")[^"]+(";)',
    rf'\g<1>{os.environ["X86_HASH"]}\2', text, count=1)
if count != 1:
    raise SystemExit("failed to update x86_64 hash")
text, count = re.subn(
    r'(aarch64-linux = \{\n\s+target = "aarch64-unknown-linux-gnu";\n\s+hash = ")[^"]+(";)',
    rf'\g<1>{os.environ["ARM_HASH"]}\2', text, count=1)
if count != 1:
    raise SystemExit("failed to update aarch64 hash")

open(path, "w", encoding="utf-8").write(text)
PY

echo "portable-network-archive: ${version}"
