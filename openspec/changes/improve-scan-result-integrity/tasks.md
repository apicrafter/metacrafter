## 1. Confirm compatibility and data contracts

- [ ] 1.1 Capture representative legacy SDK, API, file, SQL and bulk output fixtures.
- [ ] 1.2 Specify canonical report versioning, legacy aliases/readers and stable source identity; coordinate with the typed-results proposal.
- [ ] 1.3 Define field paths, scalar arrays, missing/null/empty values and flattened-path collision behavior.

## 2. Correct statistics and classification evidence

- [ ] 2.1 Replace positional header copies with one named statistics mapping shared by SDK/API/writers.
- [ ] 2.2 Preserve one-character/numeric field keys and declared array behavior through analysis and matching.
- [ ] 2.3 Validate selected fields and finite option ranges; cover empty/zero-size samples without accidental division errors.
- [ ] 2.4 Apply inclusive confidence thresholds and normalize PII booleans/context membership; preserve metadata in rule/LLM match conversion.
- [ ] 2.5 Ensure required-validator/field-guard load failures cannot silently broaden a rule; expose diagnostics.

## 3. Repair report interchange and exports

- [ ] 3.1 Accept legacy fields/data report forms and validate incompatible input instead of treating it as an empty successful export.
- [ ] 3.2 Emit valid aggregate JSON/YAML and one-header source-qualified CSV for bulk scans.
- [ ] 3.3 Honor explicit match PII metadata after confidence filtering.
- [ ] 3.4 Return attempted/succeeded/skipped/failed catalog outcomes; count only confirmed successes and retain per-operation errors.

## 4. Validate and document

- [ ] 4.1 Add golden SDK/API/serialized statistics parity and scan → serialize → exporter round-trip tests.
- [ ] 4.2 Add threshold, field shape, path, PII and failed-write regression fixtures that fail on the reviewed baseline.
- [ ] 4.3 Run the isolated base/optional test jobs, package checks and applicable quality gates.
- [ ] 4.4 Update SDK/API/report/export docs and migration examples; verify legacy consumers.
