#!/usr/bin/env python
"""Write the registry snapshot recorded in ecosystem.lock into metacrafter/registry/bundled/.

Run after recording a new registry `version` and `git_sha` in ecosystem.lock. The snapshot
is downloaded from the registry website, or read from --from (a directory that
`metacrafter registry fetch --to DIR` wrote). It is written only if its hashes match its
manifest and its version and git sha match ecosystem.lock.

    python scripts/bundle_registry_snapshot.py
    python scripts/bundle_registry_snapshot.py --from /tmp/registry
"""
import argparse
import gzip
import json
import sys
import tempfile
from pathlib import Path

import yaml

from metacrafter.registry import adapter

LOCK = Path(__file__).resolve().parent.parent / "ecosystem.lock"


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Bundle the registry snapshot recorded in ecosystem.lock.")
    parser.add_argument("--from", dest="source", help="Snapshot directory (default: download from the lock base URL)")
    args = parser.parse_args(argv)

    lock = yaml.safe_load(LOCK.read_text("utf-8"))["metacrafter-registry"]
    with tempfile.TemporaryDirectory() as tmp:
        if args.source:
            source = Path(args.source)
        else:
            print(f"Downloading {lock['base_url']}/api/v1/")
            source = adapter.fetch_snapshot(lock["base_url"], Path(tmp) / "snapshot")
        try:
            files = adapter.read_bundled_snapshot(source)
        except (OSError, ValueError) as error:
            print(f"Snapshot {source} is not usable: {error}", file=sys.stderr)
            return 1

    manifest = json.loads(files["index.json"])
    found = (str(manifest.get("version")), str(manifest.get("git_sha")))
    wanted = (str(lock["version"]), str(lock["git_sha"]))
    if found != wanted:
        print(
            f"Snapshot is registry {found[0]} ({found[1]}), ecosystem.lock records {wanted[0]} ({wanted[1]}). "
            "Record the new version in ecosystem.lock first (see docs/docs/development/ecosystem.md).",
            file=sys.stderr,
        )
        return 1
    if str(manifest.get("base_url") or "").rstrip("/") != str(lock["base_url"]).rstrip("/"):
        print(f"Snapshot base_url {manifest.get('base_url')} is not {lock['base_url']}", file=sys.stderr)
        return 1

    target = adapter.BUNDLED_DIR
    target.mkdir(parents=True, exist_ok=True)
    for old in target.iterdir():
        old.unlink()
    for name, body in files.items():
        if name.endswith(".jsonl"):
            (target / f"{name}.gz").write_bytes(gzip.compress(body, compresslevel=9, mtime=0))
        else:
            (target / name).write_bytes(body)
    size = sum(p.stat().st_size for p in target.iterdir())
    print(f"Wrote registry {found[0]} ({found[1][:7]}) to {target} ({size} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
