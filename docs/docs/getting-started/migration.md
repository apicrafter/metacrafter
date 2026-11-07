---
title: "Migration guide"
description: "Breaking changes in 2.0 and 1.0 and how to adapt"
---
# Migration guide

## 1.0 to 2.0: precise defaults

2.0.0 changes two defaults so that a plain scan reports fewer wrong matches:

- **Minimum confidence 50** (1.0.0: 5). Partial value matches, chance checksum matches and
  field-name-only matches of priority 2 and 3 rules (48 and 36) are no longer reported.
- **Registry detectors `checksum`** (1.0.0: `stable`). Only registry records with a checksum
  get a derived detector. Records with a specific regexp but no checksum need
  `--registry-detectors stable`.

For 1.0.0 results, pass `--confidence 5 --registry-detectors stable`, or set
`registry_detectors: stable` in `.metacrafter` and pass `--confidence 5`.

The default rule pack is metacrafter-rules v0.0.8 (1.0.0: v0.0.2), so labels also change
where rules were fixed. The CHANGELOG lists these changes by rule pack release.

## 0.0.x to 1.0

### Installation

- **Python 3.10 or newer.** Python 3.8 and 3.9 users can stay on 0.0.x.
- **The default rule pack ships with the package.** The package includes the archive of the
  pinned metacrafter-rules GitHub release; on first use, Metacrafter verifies it and unpacks
  it into the cache, without network access. You do not need to install
  `metacrafter-rules`. To use your own checkout, set `rules_dir`. See
  [Installation](/getting-started/installation).
- **The in-repository `rules/` directory is gone.** Rules no longer load from a `rules/`
  folder in the current directory. List your own directories in `rulepath`.
- **Optional dependencies moved to extras.** MongoDB, DuckDB, the HTTP server and binary
  file formats now need `mongodb`, `duckdb`, `server` and `formats`. A command that
  needs a missing extra names it in the error.

### Results

- **Canonical ids.** Matches carry `datatype` (the canonical registry id) and `rule_key`.
  `dataclass` keeps the rule key, so alias keys such as `ssn` now also report
  `datatype: usssn`. URLs point to the canonical record.
- **PII.** PII also comes from the registry. `pii_source` says whether the rule, the
  registry or the rule context set it.
- **Confidence.** Data matches keep their values. Field-name-only matches now score at most
  60 (60 × priority weight) instead of 100. Use `--legacy-scoring` for the old values.
- **Registry detectors.** Registry datatypes without a rule are detected by default
  (`--registry-detectors stable`; 2.0 default: `checksum`). Use `--registry-detectors off`
  for rule-only results.
- **Report version 3.** Saved reports have `report_version: 3` and new fields. Use
  `--output-format legacy` for the pre-1.0 JSON shape.
- **No raw values by default.** JSON, YAML and Markdown output omit dictionary values and
  string min/max values. Use `--include-samples` to keep them.
- **Rule order.** A rule from a later rulepath entry replaces an earlier rule with the
  same id. Before 1.0 the later rule was skipped.

### Rules

- Rule files are validated against the [rule format schema](/integrations/rules). Invalid
  rules are skipped with a diagnostic. Check them with `metacrafter rules validate`.
- `priority: 1` is the strongest priority. Rules are sorted by priority, ascending.
- Check rule keys against the registry with `metacrafter rules check --registry`.
