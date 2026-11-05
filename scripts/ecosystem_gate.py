#!/usr/bin/env python3
"""Run the ecosystem checks against a registry snapshot and decide whether they block.

The checks block (exit 1 on failure) only while the snapshot's registry version and
git sha equal the ones recorded in ecosystem.lock. When the registry has moved on, the
results are reported as warnings and the script exits 0; the weekly workflow tracks drift.

Usage: python scripts/ecosystem_gate.py SNAPSHOT_DIR [--always-block]
"""
import json
import subprocess
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent


def run(args):
    print("$ " + " ".join(args), flush=True)
    return subprocess.run(args).returncode


def missing_checksums(snapshot: Path):
    from metacrafter.engine.checksum_algorithms import ALGORITHMS

    names = set()
    for line in (snapshot / "datatypes.jsonl").read_text("utf-8").splitlines():
        if line.strip():
            name = json.loads(line).get("checksum")
            if name:
                names.add(name)
    return sorted(names - set(ALGORITHMS))


def main():
    snapshot = Path(sys.argv[1])
    always_block = "--always-block" in sys.argv
    lock = yaml.safe_load((ROOT / "ecosystem.lock").read_text())["metacrafter-registry"]
    manifest = json.loads((snapshot / "index.json").read_text("utf-8"))
    pinned = (lock.get("version"), lock.get("git_sha"))
    live = (manifest.get("version"), manifest.get("git_sha"))
    blocking = always_block or live == pinned
    print(f"registry snapshot {live[0]} {str(live[1])[:7]}; ecosystem.lock {pinned[0]} {str(pinned[1])[:7]}; "
          f"{'blocking' if blocking else 'report only (registry moved on)'}")
    failures = []
    if run(["metacrafter", "rules", "check", "--registry", "--strict", "--registry-path", str(snapshot),
            "--registry-offline", "--allow-unknown", str(ROOT / "tests/ecosystem/known_unresolved_rule_keys.yaml")]):
        failures.append("rule keys")
    if run(["metacrafter", "rules", "test", "--registry-examples", "--registry-path", str(snapshot),
            "--thresholds", str(ROOT / "tests/ecosystem/rule_thresholds.yaml"), "-o", "ecosystem-results.json"]):
        failures.append("registry examples")
    missing = missing_checksums(snapshot)
    if missing:
        print("Registry checksum algorithms missing in metacrafter/engine/checksum_algorithms.py: " + ", ".join(missing))
        failures.append("checksum algorithms")
    if not failures:
        print("Ecosystem checks passed")
        return 0
    message = "Ecosystem checks failed: " + ", ".join(failures)
    if blocking:
        print(f"::error::{message}")
        return 1
    print(f"::warning::{message} (registry {live[0]} differs from ecosystem.lock; see the weekly drift issue)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
