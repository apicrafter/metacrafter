# Change: Use metacrafter-registry as the source of truth for datatypes

> **Status (2026-11-05):** the registry package (dependency, bundled snapshot, checkout `data/` directories) is superseded by `update-ecosystem-distribution`: metacrafter reads the registry only through its website.

## Why

`metacrafter-registry` has grown from 630 to 1812 records. It now publishes:
- a versioned v1 API with a SHA-256 manifest;
- a Python package (`metacrafter_registry.Registry`) with `resolve`, `get`, `search`,
  `match`, `identify` and `crosswalk`;
- aliases and the `draft`/`stable`/`deprecated` statuses;
- an authoritative `is_pii` flag on every record;
- crosswalks to Presidio, Google DLP, DPV, Purview, Wikidata, Schema.org and other systems.

Metacrafter uses none of this. In detail:
- **Hard-coded links.** It builds links from a hard-coded template
  (`metacrafter/classify/processor.py:221`). It ignores `registry_url` and
  `METACRAFTER_REGISTRY_URL`, although the documentation says they work.
- **Unresolved keys.** It emits rule keys as datatype ids without resolving them: 14 keys
  are aliases and 32 do not exist in the registry.
- **PII from rule files.** It decides PII only from rule-file contexts.
- **No registry provenance.** Reports carry no registry metadata or version.

The registry project itself asks for this work: `modernize-registry-platform` task 8.7 and
the `ecosystem-alignment` spec.

This change is phase 1 of `devdocs/MODERNIZATION_PLAN.md`. Accepted decision 1:
`metacrafter-registry` becomes a required dependency once it is published on PyPI. Until
then, an HTTP fallback against `/api/v1/` keeps the feature usable.

## What Changes

- Add a `metacrafter.registry` adapter over `metacrafter_registry.Registry`. It loads the
  registry from a local path, cache, network fetch (SHA-256 verified) or the bundled
  snapshot, in that order. Offline operation works without network access.
- Configure the registry with `registry_url`, `registry_path`, `registry_offline` and
  `METACRAFTER_REGISTRY_URL`. Build datatype URLs from the manifest `datatype_url_template`.
- Resolve every rule key to a canonical registry id. Keep the original key as `rule_key`.
  Report `replaced_by` for deprecated records. Warn on unknown keys.
- Enrich each match with registry metadata:
  - `name`, `categories`, `is_pii`, `classification`, `status`, `country`, `url`;
  - selected `mappings`: presidio, google_dlp, dpv, purview, schema_org, wikidata.
- Determine `is_pii` from the registry record, the rule flag (as a boolean) and the rule
  context.
- Add scan filters based on registry metadata: `--categories`, `--pii-only`, `--status`.
- Add `metacrafter rules check --registry`. It reports unknown keys, alias keys and
  deprecated targets.
- Record registry provenance in every report: version, git sha, generated-at, source.
- Keep `metacrafter.registry.client.RegistryClient` as a deprecated shim over the adapter.

## Relationship to other changes

- **Supersedes** `update-registry-resolution` (Codex review W11, Beads `metacrafter-13`).
  Its requirements (single resolver, configured links, cached optional enrichment) are
  covered here.
- **Supersedes** the registry items (section 3) of the July `expand-metacrafter-llm-platform`.
- **Depends on** `refactor-metacrafter-foundation` (Python ≥3.10 matches the registry
  package floor).
- **Depends on** `improve-scan-result-integrity` for the versioned report container. The
  report fields added here are carried by that container.
- **Consumed by** `update-detection-engine`, `add-report-v2` and `update-llm-classification`.

## Impact

- Affected specs: `registry-integration`
- Affected code: new `metacrafter/registry/` adapter; `metacrafter/config.py`;
  `metacrafter/classify/processor.py` (`BASE_URL`, `RuleResult`); `metacrafter/core.py`
  (result assembly); `metacrafter/server/api.py`; CLI `rules` and `scan` options; docs
  `integrations/registry.md`
- Cross-repo prerequisites:
  - `metacrafter-registry` published on PyPI (owner action);
  - runtime dependencies of the registry package reduced (site-build dependencies such as
    Jinja2 moved to a `build` extra).
- Report output gains fields. Existing fields keep their names. `datatype_url` now points
  to the canonical id, which changes 14 alias-based URLs.
