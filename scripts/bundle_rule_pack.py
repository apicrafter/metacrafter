#!/usr/bin/env python
"""Write the pinned metacrafter-rules release archive into metacrafter/rules/bundled/.

Run after changing DEFAULT_RELEASE and DEFAULT_SHA256 in metacrafter/rules/pack.py (and
ecosystem.lock). The archive is downloaded from GitHub, or read from --from, and written
only if it matches the pinned SHA-256. Bundled archives of other releases are removed.

    python scripts/bundle_rule_pack.py
    python scripts/bundle_rule_pack.py --from metacrafter-rules-v0.0.3.tar.gz
"""
import argparse
import hashlib
import sys
from pathlib import Path

from metacrafter.rules import pack


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Bundle the pinned metacrafter-rules release archive.")
    parser.add_argument("--from", dest="source", help="Local copy of the release archive (default: download)")
    args = parser.parse_args(argv)

    tag, digest = pack.DEFAULT_RELEASE, pack.DEFAULT_SHA256
    if args.source:
        data = Path(args.source).read_bytes()
    else:
        url = pack.archive_url(tag)
        print(f"Downloading {url}")
        data = pack._download(url, pack.DOWNLOAD_TIMEOUT)
    actual = hashlib.sha256(data).hexdigest()
    if actual != digest:
        print(f"SHA-256 mismatch for {tag}: expected {digest}, got {actual}", file=sys.stderr)
        return 1

    target = pack.bundled_archive(tag)
    target.parent.mkdir(parents=True, exist_ok=True)
    for old in target.parent.glob("*.tar.gz"):
        if old != target:
            old.unlink()
            print(f"Removed {old}")
    target.write_bytes(data)
    print(f"Wrote {target} ({len(data)} bytes, sha256 {digest})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
