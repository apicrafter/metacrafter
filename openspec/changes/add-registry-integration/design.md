## Context

The registry publishes two interfaces:
- **Python package** `metacrafter_registry`. `Registry.load()` reads a bundled snapshot or a
  checkout. `Registry.fetch()` downloads `/api/v1/` bulk files with a manifest, caches them
  in `~/.cache` and verifies SHA-256.
- **Static site.** It serves the v1 API (`/api/v1/index.json`, `datatypes.jsonl`,
  `aliases.json`, `datatypes/<id>.json`) and legacy files (`/registry.json`,
  `/datatype/<id>.json`).

Note: `/api/v1/datatypes/<alias>.json` returns 404. Aliases must be resolved through
`aliases.json` or the package.

About 76% of records are `draft`; 443 are `stable`. Records may carry `aliases`,
`replaced_by`, `is_pii` and `mappings`.

## Goals / Non-Goals

**Goals:**
- One adapter that every consumer uses: scan output, HTTP API, LLM, exporters, CLI.
- Deterministic, offline-capable operation with explicit provenance.
- Canonical ids and correct URLs in every output.

**Non-Goals:**
- Detection from registry regexps and checksums (`update-detection-engine`).
- The report schema itself (`add-report-v2`).
- Editing registry data. Fixing broken rule keys belongs to `metacrafter-rules`.

## Decisions

### Decision: Adapter API

```python
class RegistryAdapter:
    @classmethod
    def from_config(cls, config) -> "RegistryAdapter": ...
    provenance: RegistryProvenance     # version, git_sha, generated_at, source, base_url
    def resolve(self, key: str) -> Resolution        # canonical_id | None, via_alias, replaced_by
    def describe(self, datatype_id: str) -> DatatypeRef | None
    def url_for(self, datatype_id: str) -> str
    def is_pii(self, datatype_id: str) -> bool | None
    def iter_records(self, *, status=None, categories=None, country=None): ...
```

`DatatypeRef` is a frozen dataclass:
- `id`, `name`, `categories`, `is_pii`, `classification`, `status`, `country`, `url`;
- `mappings`, limited to the selected systems.

The adapter wraps `metacrafter_registry.Registry`. If that package is not importable, it
falls back to a thin HTTP loader for `/api/v1/` with the same manifest check.

### Decision: Source resolution order

1. `registry_path` (config or `--registry-path`): a checkout `data/` dir or a snapshot dir.
2. Cache in `~/.cache/metacrafter-registry`, if present and the manifest hash verifies.
3. If not `registry_offline`: `Registry.fetch(base_url)`, which refreshes the cache.
4. The snapshot bundled in the registry package.
5. Otherwise: no adapter. Matches keep `rule_key`, URLs are built from the configured
   base URL, and `registry.source = "unavailable"` is reported. The scan does not fail.

The base URL comes from `METACRAFTER_REGISTRY_URL`, then `registry_url` in `.metacrafter`,
then `https://registry.apicrafter.io`. Datatype URLs use the manifest
`datatype_url_template`, rebased on the configured base URL.

### Decision: PII determination

`is_pii` is true when any of these hold:
- the registry record has `is_pii: true`;
- the rule has `is_pii: true` (YAML boolean);
- the rule context contains `pii`.

The source is recorded in `pii_source` (`registry`, `rule`, `context`) for auditability. A
registry `is_pii: false` does not override an explicit rule flag. Rule authors may know
the context better, for example a non-PII type used as a personal identifier.

### Decision: Unknown and deprecated keys

- Unknown key: the match is kept with `datatype: null` and `rule_key` set. A warning is
  logged once per key per process. `rules check --registry` lists such keys and exits 1
  with `--strict`.
- Alias key: the match uses the canonical id. `rule_key` keeps the original key.
- Deprecated record with `replaced_by`: the match keeps the deprecated id and adds
  `replaced_by`. The engine does not silently relabel.

## Risks / Trade-offs

- **Registry package not on PyPI.** Mitigation: HTTP fallback plus `registry_path`. The
  dependency is declared once the package is published.
- **Network access during scans.** Mitigation: the cache is used first; `registry_offline`
  disables fetch; nothing is fetched when a fresh cache verifies.
- **Draft records presented as authoritative.** Mitigation: `status` is always included,
  and `--status stable` filters.

## Open Questions

- Should the registry snapshot version be pinned in `ecosystem.lock` for reproducible CI?
  Proposed answer: yes, see `add-ecosystem-contract-tests`.
