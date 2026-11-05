## 1. Models and schema

- [x] 1.1 Define pydantic models `ScanReport`, `SourceReport`, `FieldReport`, `Match`, `Evidence`, `RegistryRef`, `Provenance` in `metacrafter/report/models.py`
- [x] 1.2 Generate `report-v2.schema.json` from the models and publish it under `docs/static/schema/`
- [x] 1.3 Build reports from scan results in SDK, CLI and HTTP API through one function
- [x] 1.4 Implement `--format legacy` as an adapter from v2 to the pre-1.0 structure
- [x] 1.5 Test that every CLI output (json, jsonl, yaml) validates against the schema

## 2. Exporters

- [x] 2.1 ~~Move `metacrafter/integrations/` to `metacrafter/exporters/`~~ Dropped: the move is cosmetic and would break module-level patching in tests; new exporters live in `metacrafter/integrations/` (`crosswalk.py`, `frictionless.py`, `labels.py`)
- [x] 2.2 Read PII, datatype name, categories and URL from `Match.registry`; delete hard-coded datatype lists
- [x] 2.3 Add registry URL and mappings as catalog custom properties
- [x] 2.4 Add `metacrafter export labels --system presidio|google_dlp|dpv|purview`
- [x] 2.5 Add `metacrafter export frictionless` producing a Table Schema with `rdfType`

## 3. Human and agent output

- [x] 3.1 Add `--format md` with a per-field table and a PII summary
- [x] 3.2 Exclude raw sample values from Markdown and JSON output unless `--include-samples` is set

## 4. Release

- [x] 4.1 Write the migration guide (legacy to v2) in docs
- [x] 4.2 Switch the default format to version 3 (done in this branch; legacy kept as `--output-format legacy`)
