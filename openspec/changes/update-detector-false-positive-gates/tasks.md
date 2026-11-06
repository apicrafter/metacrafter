## 1. Engine

- [x] 1.1 `engine/background.py`: background token classes and `background_broadness(regexp)` (cached per process)
- [x] 1.2 `registry_detectors.py`: record background broadness, `needs_name_evidence` (generic and no checksum) and field-name keys from the record id and aliases
- [x] 1.3 `processor.py`: field-name evidence for derived detectors; skip generic detectors without it; skip country-specific detectors without it when the scan country differs, or is unknown and the record has no checksum
- [x] 1.4 `processor.py`: without a scan country, drop country-specific field-name matches covered by a universal or stronger match, and country-specific value matches without a validator that repeat a universal match
- [x] 1.5 `--explain` reasons for every skipped detector and dropped match

## 2. Tests and measurements

- [x] 2.1 Tests: background classes; generic detector with and without field-name evidence; alias evidence; stop tokens and country codes in keys; country-specific detector with unknown, other and matching scan country; name-variant suppression; repeated country-specific pattern
- [x] 2.2 Regenerate `tests/benchmark/baseline.json` (nine false-positive-only datatypes removed)
- [x] 2.3 Measure on `armsis` (labelled) and seven other open-data catalogues

## 3. Docs

- [x] 3.1 `docs/docs/integrations/rules.md`, CHANGELOG
