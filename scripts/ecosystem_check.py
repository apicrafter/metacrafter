#!/usr/bin/env python3
"""Cross-repository alignment check for the Metacrafter ecosystem.

Validates the contract between metacrafter-rules and metacrafter-registry:

  1. Rulekey uniqueness across all metacrafter-rules YAML files.
  2. Rule datatype ``key`` values vs registry ``id`` values (both directions):
       * rule keys with no registry entry (WARNING by default, ERROR with
         --strict-orphans)
       * registry datatypes with no rule (WARNING, informational)
  3. PPR rule compilation via Metacrafter's RulesProcessor (if importable).

Sibling repository locations are resolved from environment variables, falling
back to sibling directories next to this repo:
    METACRAFTER_RULES_DIR     (default: ../metacrafter-rules)
    METACRAFTER_REGISTRY_DIR  (default: ../metacrafter-registry)

Exit code is non-zero when a hard error is found (rulekey collisions, failed
compilation, or orphan rule keys when --strict-orphans is set).

Usage:
    python scripts/ecosystem_check.py [--strict-orphans]
"""

import argparse
import json
import os
import sys
from collections import defaultdict
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent


def resolve_dir(env_var, default_rel):
    value = os.environ.get(env_var)
    if value:
        return Path(value)
    return (REPO_ROOT.parent / default_rel).resolve()


def load_rule_keys_and_rulekeys(rules_dir: Path):
    """Return (datatype_keys, rulekey_map) from all rules YAML files."""
    datatype_keys = set()
    rulekeys = defaultdict(list)
    for path in sorted(rules_dir.rglob("*.yaml")):
        if path.name.endswith(".backup"):
            continue
        try:
            with open(path, "r", encoding="utf-8") as handle:
                data = yaml.safe_load(handle)
        except yaml.YAMLError:
            continue
        if not isinstance(data, dict):
            continue
        rules = data.get("rules")
        if not isinstance(rules, dict):
            continue
        for rulekey, rule in rules.items():
            rulekeys[rulekey].append(str(path.relative_to(rules_dir)))
            if isinstance(rule, dict) and rule.get("key"):
                datatype_keys.add(str(rule["key"]))
    return datatype_keys, rulekeys


def load_registry_ids(registry_dir: Path):
    """Return the set of datatype/pattern IDs from the built registry JSONL."""
    ids = set()
    jsonl = registry_dir / "data" / "datatypes_latest.jsonl"
    if not jsonl.exists():
        return ids
    with open(jsonl, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            if record.get("id"):
                ids.add(str(record["id"]))
    return ids


def compile_rules(rules_dir: Path):
    try:
        from metacrafter.classify.processor import RulesProcessor
    except ImportError:
        print("WARNING: metacrafter not importable; skipping compilation check.")
        return []
    errors = []
    for path in sorted(rules_dir.rglob("*.yaml")):
        if path.name.endswith(".backup"):
            continue
        processor = RulesProcessor()
        try:
            processor.import_rules_path(str(path.parent), recursive=False)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{path.relative_to(rules_dir)}: {exc}")
    return errors


def main():
    parser = argparse.ArgumentParser(description="Ecosystem alignment check")
    parser.add_argument(
        "--strict-orphans",
        action="store_true",
        help="Treat rule keys missing from the registry as errors (not warnings)",
    )
    parser.add_argument("--no-compile", action="store_true", help="Skip ppr compile")
    args = parser.parse_args()

    rules_dir = resolve_dir("METACRAFTER_RULES_DIR", "metacrafter-rules") / "rules"
    registry_dir = resolve_dir("METACRAFTER_REGISTRY_DIR", "metacrafter-registry")

    if not rules_dir.exists():
        print(f"ERROR: rules directory not found: {rules_dir}", file=sys.stderr)
        return 1

    errors = []
    warnings = []

    datatype_keys, rulekeys = load_rule_keys_and_rulekeys(rules_dir)

    # 1. Rulekey uniqueness
    for rulekey, files in sorted(rulekeys.items()):
        if len(files) > 1:
            errors.append(
                "Duplicate rulekey '%s' in: %s" % (rulekey, ", ".join(files))
            )

    # 2. Rule keys vs registry IDs
    registry_ids = load_registry_ids(registry_dir)
    if not registry_ids:
        warnings.append(
            f"Registry JSONL not found or empty at {registry_dir}; skipping ID alignment."
        )
    else:
        orphan_rule_keys = sorted(datatype_keys - registry_ids)
        registry_without_rules = sorted(registry_ids - datatype_keys)
        for key in orphan_rule_keys:
            msg = "Rule key '%s' has no registry entry" % key
            (errors if args.strict_orphans else warnings).append(msg)
        print(
            "Alignment: %d rule keys, %d registry ids, %d orphan rule keys, "
            "%d registry datatypes without rules"
            % (
                len(datatype_keys),
                len(registry_ids),
                len(orphan_rule_keys),
                len(registry_without_rules),
            )
        )

    # 3. Compilation
    if not args.no_compile:
        errors.extend(compile_rules(rules_dir))

    if warnings:
        print("\n%d warning(s):" % len(warnings))
        for w in warnings:
            print("  - %s" % w)

    if errors:
        print("\n%d error(s):" % len(errors), file=sys.stderr)
        for e in errors:
            print("  - %s" % e, file=sys.stderr)
        return 1

    print("\nEcosystem alignment check passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
