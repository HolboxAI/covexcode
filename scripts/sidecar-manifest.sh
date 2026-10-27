#!/usr/bin/env bash
# Generate sidecar-manifest.json for the IDE.
#
# The IDE pins this manifest (by release tag and, optionally, by hash) and
# downloads the matching artifact. Keeping the version, ACP protocol and
# per-asset SHA-256 in one file means the IDE never has to guess which binary
# belongs to an IDE build.
set -euo pipefail

version="$(grep -m1 '^version = ' Cargo.toml | sed -E 's/version = "(.*)"/\1/')"
assets_dir="${1:-release-assets}"

python3 - "$version" "$assets_dir" <<'PY'
import hashlib
import json
import os
import sys

version, assets_dir = sys.argv[1], sys.argv[2]

mapping = {
    "covexcode-macos-aarch64": ("darwin-arm64", True),
    "covexcode-windows-x86_64.exe": ("win32-x64", True),
    "covexcode-linux-x86_64": ("linux-x64", False),
    "covexcode-linux-aarch64": ("linux-arm64", False),
    "covexcode-linux-x86_64-musl": ("linux-x64-musl", True),
    "covexcode-linux-aarch64-musl": ("linux-arm64-musl", True),
}

artifacts = {}
missing = []
for asset, (target, required) in mapping.items():
    path = os.path.join(assets_dir, asset)
    if not os.path.exists(path):
        if required:
            missing.append(asset)
        continue
    with open(path, "rb") as fh:
        digest = hashlib.sha256(fh.read()).hexdigest()
    artifacts[target] = {
        "asset": asset,
        "sha256": digest,
        "size": os.path.getsize(path),
    }

if missing:
    raise SystemExit(
        "sidecar manifest is incomplete; missing required assets: "
        + ", ".join(missing)
    )

manifest = {
    "schemaVersion": 1,
    "agentVersion": version,
    "protocolVersion": 1,
    "artifacts": artifacts,
}

out = os.path.join(assets_dir, "sidecar-manifest.json")
with open(out, "w", encoding="utf-8") as fh:
    json.dump(manifest, fh, indent=2, sort_keys=True)
    fh.write("\n")
print(f"wrote {out} with {len(artifacts)} artifacts for {version}")
PY
