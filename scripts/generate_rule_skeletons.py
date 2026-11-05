#!/usr/bin/env python3
"""Generate skeleton metacrafter rule YAML from registry datatypes.

For every registry datatype that has a ``regexp`` (and, unless
``--include-existing`` is given, no matching rule key yet in metacrafter-rules),
this emits a skeleton rule block:

  * a ``text`` field-name rule with alias guesses derived from the id/name, and
  * a commented data-rule stub carrying the registry ``regexp`` so an author can
    turn it into a ``ppr`` (PyParsing) or ``func`` (validator) rule.

The output is meant as a starting point for closing the "registry datatypes
without rules" gap; every skeleton must be reviewed before use.

Paths are resolved from ``METACRAFTER_REGISTRY_DIR`` / ``METACRAFTER_RULES_DIR``
environment variables (default: sibling ``../metacrafter-registry`` and
``../metacrafter-rules``) or overridden with CLI flags.

Usage:
    python scripts/generate_rule_skeletons.py [--limit N] [--category geo]
        [--registry PATH] [--rules PATH] [--include-existing] [-o out.yaml]
"""
import argparse
import json
import os
import re
import sys
from pathlib import Path

try:
    import yaml
except ImportError:  # pragma: no cover - yaml is a metacrafter dependency
    yaml = None

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent


def _default_dir(env_var, sibling):
    value = os.environ.get(env_var)
    if value:
        return Path(value)
    return REPO_ROOT.parent / sibling


def load_registry_datatypes(registry_dir):
    jsonl = Path(registry_dir) / "data" / "datatypes_latest.jsonl"
    if not jsonl.exists():
        sys.exit(f"Registry datatypes file not found: {jsonl}")
    datatypes = []
    with open(jsonl, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                datatypes.append(json.loads(line))
    return datatypes


def load_existing_rule_keys(rules_dir):
    """Return the set of existing rule ``key`` values across the rules tree."""
    if yaml is None:
        return set()
    keys = set()
    rules_root = Path(rules_dir) / "rules"
    if not rules_root.exists():
        return keys
    for path in rules_root.rglob("*.yaml"):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
        except Exception:  # noqa: BLE001 - skip malformed files silently
            continue
        if not isinstance(data, dict):
            continue
        rules = data.get("rules", {})
        if isinstance(rules, dict):
            for entry in rules.values():
                if isinstance(entry, dict) and entry.get("key"):
                    keys.add(str(entry["key"]))
    return keys


def guess_field_aliases(datatype_id, name):
    """Produce comma-separated field-name alias guesses for a datatype."""
    aliases = set()
    aliases.add(datatype_id)
    aliases.add(datatype_id.replace("_", ""))
    if name:
        slug = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
        if slug:
            aliases.add(slug)
    # Keep it short and deterministic.
    return ",".join(sorted(a for a in aliases if a))


def render_skeleton(datatype):
    dt_id = datatype["id"]
    name = datatype.get("name", dt_id)
    regexp = datatype.get("regexp", "")
    is_pii = datatype.get("is_pii") in (True, "True", "true")
    aliases = guess_field_aliases(dt_id, name)

    lines = []
    lines.append(f"  {dt_id}field:")
    lines.append(f"    key: {dt_id}")
    if is_pii:
        lines.append("    is_pii: True")
    lines.append(f"    name: {name} by field name")
    lines.append("    match: text")
    lines.append("    type: field")
    lines.append(f"    rule: {aliases}")
    lines.append("    priority: 1")
    # Data-rule stub as comment (regexp is not a native match type).
    lines.append(f"  # TODO data rule for '{dt_id}'. Registry regexp:")
    lines.append(f"  #   {regexp!r}")
    lines.append(f"  # Convert to a `ppr` PyParsing expression or a `func` validator, e.g.:")
    lines.append(f"  # {dt_id}value:")
    lines.append(f"  #   key: {dt_id}")
    lines.append(f"  #   match: func")
    lines.append(f"  #   type: data")
    lines.append(f"  #   rule: metacrafterext.rules.<module>.validate_{dt_id}")
    return "\n".join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--registry", default=None, help="metacrafter-registry directory")
    parser.add_argument("--rules", default=None, help="metacrafter-rules directory")
    parser.add_argument("--category", default=None, help="only datatypes in this category")
    parser.add_argument("--limit", type=int, default=None, help="max skeletons to emit")
    parser.add_argument("--include-existing", action="store_true",
                        help="also emit skeletons for datatypes that already have rules")
    parser.add_argument("-o", "--output", default=None, help="output file (default: stdout)")
    args = parser.parse_args(argv)

    registry_dir = Path(args.registry) if args.registry else _default_dir("METACRAFTER_REGISTRY_DIR", "metacrafter-registry")
    rules_dir = Path(args.rules) if args.rules else _default_dir("METACRAFTER_RULES_DIR", "metacrafter-rules")

    datatypes = load_registry_datatypes(registry_dir)
    existing = set() if args.include_existing else load_existing_rule_keys(rules_dir)

    candidates = []
    for d in datatypes:
        if not d.get("regexp"):
            continue
        if not args.include_existing and d["id"] in existing:
            continue
        if args.category:
            cats = [str(c).lower() for c in (d.get("categories") or [])]
            # categories may be dicts after build; normalize
            cats += [str(c.get("id")).lower() for c in (d.get("categories") or []) if isinstance(c, dict)]
            if args.category.lower() not in cats:
                continue
        candidates.append(d)

    candidates.sort(key=lambda d: d["id"])
    if args.limit:
        candidates = candidates[: args.limit]

    out_lines = [
        "# Auto-generated skeleton rules from registry datatypes.",
        "# REVIEW EVERY ENTRY before use: field aliases are guesses and data",
        "# rules are left as TODO stubs carrying the registry regexp.",
        f"# Source registry: {registry_dir}",
        "name: generated-skeletons",
        "description: Skeleton rules generated from registry datatypes",
        "context: common",
        "lang: en",
        "rules:",
    ]
    for d in candidates:
        out_lines.append(render_skeleton(d))

    output = "\n".join(out_lines) + "\n"
    if args.output:
        Path(args.output).write_text(output, encoding="utf-8")
        print(f"Wrote {len(candidates)} skeleton rule(s) to {args.output}", file=sys.stderr)
    else:
        sys.stdout.write(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
