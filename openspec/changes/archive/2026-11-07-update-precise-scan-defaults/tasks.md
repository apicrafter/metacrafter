## 1. Defaults

- [x] 1.1 `MIN_CONFIDENCE_FOR_MATCH` 50 (was 5); CLI help texts
- [x] 1.2 Registry detector mode `checksum`; default of `CrafterCmd`, `RulesProcessor.bind_registry`, `registry_detectors` in `.metacrafter` and `--registry-detectors`
- [x] 1.3 Keep `rules test --registry-examples` on `stable`

## 2. Date patterns

- [x] 2.1 Run qddate on fields whose only matches are field-name matches

## 3. Tests, measurements and docs

- [x] 3.1 Tests for the defaults, the `checksum` mode and date patterns after name-only matches
- [x] 3.2 Re-record `tests/benchmark/baseline.json`
- [x] 3.3 Measure on the `datacrafter-examples` sample files
- [x] 3.4 Docs (options, rules, migration, troubleshooting, best practices, examples), skills, CHANGELOG
