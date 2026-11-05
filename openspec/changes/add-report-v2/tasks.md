## 1. Models and schema

- [ ] 1.1 Define pydantic models `ScanReport`, `SourceReport`, `FieldReport`, `Match`, `Evidence`, `RegistryRef`, `Provenance` in `metacrafter/report/models.py`
- [ ] 1.2 Generate `report-v2.schema.json` from the models and publish it under `docs/static/schema/`
- [ ] 1.3 Build reports from scan results in SDK, CLI and HTTP API through one function
- [ ] 1.4 Implement `--format legacy` as an adapter from v2 to the pre-1.0 structure
- [ ] 1.5 Test that every CLI output (json, jsonl, yaml) validates against the schema

## 2. Exporters

- [ ] 2.1 Move `metacrafter/integrations/` to `metacrafter/exporters/` with import shims
- [ ] 2.2 Read PII, datatype name, categories and URL from `Match.registry`; delete hard-coded datatype lists
- [ ] 2.3 Add registry URL and mappings as catalog custom properties
- [ ] 2.4 Add `metacrafter export labels --system presidio|google_dlp|dpv|purview`
- [ ] 2.5 Add `metacrafter export frictionless` producing a Table Schema with `rdfType`

## 3. Human and agent output

- [ ] 3.1 Add `--format md` with a per-field table and a PII summary
- [ ] 3.2 Exclude raw sample values from Markdown and JSON output unless `--include-samples` is set

## 4. Release

- [ ] 4.1 Write the migration guide (legacy to v2) in docs
- [ ] 4.2 Switch the default format to v2 in 1.0.0; keep legacy for two minor releases
