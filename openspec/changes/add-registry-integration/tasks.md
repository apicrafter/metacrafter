## 1. Prerequisites

- [ ] 1.1 Confirm `metacrafter-registry` is published on PyPI with a lightweight runtime dependency set (owner action)
- [ ] 1.2 Until then, implement the HTTP fallback loader for `/api/v1/` with manifest SHA-256 checks

## 2. Adapter

- [ ] 2.1 Create `metacrafter/registry/adapter.py` with `RegistryAdapter`, `Resolution`, `DatatypeRef`, `RegistryProvenance`
- [ ] 2.2 Implement the source resolution order from design.md, including cache verification
- [ ] 2.3 Add config fields `registry_path`, `registry_offline`; wire `registry_url` and `METACRAFTER_REGISTRY_URL`
- [ ] 2.4 Build datatype URLs from the manifest `datatype_url_template`; remove the hard-coded `BASE_URL`
- [ ] 2.5 Turn `RegistryClient` into a deprecated shim over the adapter

## 3. Resolution and enrichment

- [ ] 3.1 Resolve rule keys to canonical ids at rule-load time; store `rule_key` and `datatype`
- [ ] 3.2 Add enrichment fields to match results (name, categories, is_pii, classification, status, country, url, mappings)
- [ ] 3.3 Implement PII determination with `pii_source`; fix the boolean `is_pii` comparison
- [ ] 3.4 Report `replaced_by` for deprecated records
- [ ] 3.5 Add registry provenance to SDK, CLI and HTTP API reports

## 4. CLI and filters

- [ ] 4.1 Add `--registry-path`, `--registry-offline` to scan commands
- [ ] 4.2 Add `--categories`, `--pii-only`, `--status` filters
- [ ] 4.3 Add `metacrafter rules check --registry [--strict]`

## 5. Tests and docs

- [ ] 5.1 Add a small registry snapshot fixture; all adapter tests run without network
- [ ] 5.2 Tests: alias key, unknown key, deprecated key, URL override via env and config, offline with no cache
- [ ] 5.3 Tests: PII from registry, rule flag and context; `pii_source` values
- [ ] 5.4 Update `docs/docs/integrations/registry.md` and the SDK docs
