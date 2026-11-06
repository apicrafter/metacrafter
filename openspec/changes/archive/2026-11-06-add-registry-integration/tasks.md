## 1. Prerequisites

- [x] 1.1 ~~Confirm `metacrafter-registry` is published on PyPI~~ Superseded by `update-ecosystem-distribution`: the registry is read only through its website
- [x] 1.2 Until then, implement the HTTP fallback loader for `/api/v1/` with manifest SHA-256 checks

## 2. Adapter

- [x] 2.1 Create `metacrafter/registry/adapter.py` with `RegistryAdapter`, `Resolution`, `DatatypeRef`, `RegistryProvenance`
- [x] 2.2 Implement the source resolution order from design.md, including cache verification
- [x] 2.3 Add config fields `registry_path`, `registry_offline`; wire `registry_url` and `METACRAFTER_REGISTRY_URL`
- [x] 2.4 Build datatype URLs from the manifest `datatype_url_template`; remove the hard-coded `BASE_URL`
- [x] 2.5 Turn `RegistryClient` into a deprecated shim over the adapter

## 3. Resolution and enrichment

- [x] 3.1 Resolve rule keys to canonical ids (at report assembly, once per match); store `rule_key` and `datatype`
- [x] 3.2 Add enrichment fields to match results (name, categories, is_pii, classification, status, country, url, mappings)
- [x] 3.3 Implement PII determination with `pii_source`; fix the boolean `is_pii` comparison
- [x] 3.4 Report `replaced_by` for deprecated records
- [x] 3.5 Add registry provenance to SDK, CLI and HTTP API reports

## 4. CLI and filters

- [x] 4.1 Add `--registry-path`, `--registry-offline` to scan commands
- [x] 4.2 Add `--categories`, `--pii-only`, `--status` filters
- [x] 4.3 Add `metacrafter rules check --registry [--strict]`

## 5. Tests and docs

- [x] 5.1 Add a small registry snapshot fixture; all adapter tests run without network
- [x] 5.2 Tests: alias key, unknown key, deprecated key, URL override via env and config, offline with no cache
- [x] 5.3 Tests: PII from registry, rule flag and context; `pii_source` values
- [x] 5.4 Update `docs/docs/integrations/registry.md` and the SDK docs
