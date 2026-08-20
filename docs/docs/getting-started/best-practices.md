---
title: "Best practices"
description: "Recommended scan settings, rule hygiene, and production use"
---
# Best practices

## Start narrow, then widen

For a first pass on unknown data, scan a sample with default rules:

```bash
metacrafter scan file data.csv --limit 200 --format full
```

Then restrict to the contexts you care about (`pii`, `finance`, `geo`) and raise
`--confidence` so reports stay actionable.

## Prefer inspectable rules for known types

Use YAML rules for emails, phones, national IDs, and other types with a stable
pattern. Reserve [LLM/hybrid mode](/integrations/llm) for columns that rules
leave unmatched.

## Keep rule keys aligned with the registry

Rule `key` values should match registry datatype `id` values so scan output can
emit `https://registry.apicrafter.io/datatype/<key>`. See
[Registry](/integrations/registry).

## Sample large sources

`--limit` caps values sampled per field (files default to 100, databases to
1000). Increase it only when confidence looks unstable. Use `--batch-size` on
SQL scans to control memory.

## Separate classification from catalog writes

1. Scan to JSON: `metacrafter scan file users.csv --format full --output-format json -o report.json`
2. Review matches and confidence
3. Export with `--min-confidence 50.0` to DataHub / OpenMetadata / Atlas

## Production CLI defaults

- Set `METACRAFTER_PLAIN=1` in CI so tables do not use color
- Pin extras you actually use (`rules`, `llm`, `datahub`) rather than `[all]`
- Point `METACRAFTER_REGISTRY_URL` at a self-hosted registry for air-gapped hosts
- Do not commit API keys; use environment variables

## Custom rules

- Put site-specific rules in their own directory on `rulepath`
- Give each rule a unique `key`
- Test with a tiny CSV that contains known positives and negatives
- Prefer `func` validators for checksums; prefer `ppr` for structured identifiers

## Related docs

- [Custom rules](/integrations/rules)
- [Troubleshooting](/getting-started/troubleshooting)
- [Architecture](/development/architecture)
