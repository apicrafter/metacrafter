---
title: "Registry"
description: "Resolve scan results against metacrafter-registry datatypes"
---
# Registry

[metacrafter-registry](https://github.com/apicrafter/metacrafter-registry) is the
canonical catalog of semantic datatypes, published at
[registry.apicrafter.io](https://registry.apicrafter.io). Metacrafter resolves every
match against it:

- **Canonical ids.** A rule key that is a registry alias (for example `ssn`) is reported
  as the canonical id (`usssn`). The original key stays in `rule_key`; `dataclass` also
  keeps the original key for older consumers.
- **Enrichment.** Each match gains a `registry` object with `name`, `status`,
  `categories`, `classification`, `country`, `url` and the `mappings` for Presidio,
  Google DLP, DPV, Purview, Schema.org and Wikidata.
- **PII.** A match is PII when the rule sets `is_pii: true`, when the registry record has
  `is_pii: true`, or when the rule file context is `pii`. `pii_source` says which
  (`rule`, `registry` or `context`, in that order of precedence).
- **Deprecated records.** A match on a deprecated record keeps its id and adds
  `replaced_by`.
- **Provenance.** Every report has a `registry` block: `source`, `base_url`, `version`,
  `git_sha`, `generated_at`.

## Where registry data comes from

Metacrafter tries these sources in order:

1. `registry_path`: a snapshot directory (`datatypes.jsonl`, `aliases.json`,
   `index.json`, as published under `/api/v1/`) or a registry checkout `data/` directory
   (needs the `metacrafter-registry` package);
2. a local cache in `~/.cache/metacrafter/registry/` that is younger than
   `registry_cache_ttl` (default 24 hours) and matches its manifest hashes;
3. a download of `/api/v1/` from the configured base URL. Every file is checked against
   the SHA-256 in `index.json` before use. Skipped when `registry_offline` is set;
4. an older cache, if its hashes still match;
5. the snapshot bundled with the `metacrafter-registry` package, when installed.

If nothing loads, the scan still runs: matches keep their rule keys,
URLs are built from the base URL, and `registry.source` is `unavailable`.

Registry data is public. Metacrafter only downloads it; it never sends scanned values
to the registry.

## Configuration

```yaml
# .metacrafter
registry_url: https://registry.apicrafter.io   # base URL for links and downloads
registry_path: /data/registry-snapshot          # optional local snapshot
registry_offline: true                          # never use the network
registry_cache_ttl: 86400                       # seconds
```

Environment variables override the file: `METACRAFTER_REGISTRY_URL`,
`METACRAFTER_REGISTRY_PATH`, `METACRAFTER_REGISTRY_OFFLINE` (`1`, `true`, `yes`).
Scan commands also accept `--registry-path` and `--registry-offline`.

The optional `metacrafter-registry` package adds value identification, checksums and
crosswalks. It is not on PyPI yet:

```bash
pip install "$(python scripts/ecosystem_ref.py metacrafter-registry)"
```

## Filters

Scan commands can keep only matches that satisfy registry metadata:

```bash
metacrafter scan file users.csv --pii-only --status stable
metacrafter scan file payments.csv --categories finances,pii
```

## Checking rules against the registry

```bash
metacrafter rules check --registry            # summary and tables
metacrafter rules check --registry --strict   # exit 1 when unknown keys exist
metacrafter rules check --output-format json
```

The command lists rule keys that are not registry ids or aliases, keys that resolve
only through an alias, and keys that point to deprecated records.

## LLM index

Hybrid and LLM modes embed registry records. See [LLM classification](/integrations/llm).

## Rule skeletons

`scripts/generate_rule_skeletons.py` emits starter YAML for registry datatypes
that have a regexp but no rule yet:

```bash
METACRAFTER_REGISTRY_DIR=../metacrafter-registry \
METACRAFTER_RULES_DIR=../metacrafter-rules \
python scripts/generate_rule_skeletons.py --category geo --limit 20 -o skeletons.yaml
```

Review every skeleton before using it. See [Custom rules](/integrations/rules).
