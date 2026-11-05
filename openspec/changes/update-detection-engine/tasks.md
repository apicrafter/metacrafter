## 1. Rule format v2

- [ ] 1.1 Write `metacrafter/rules/schema/rule-v2.schema.json` (extends the typed schema from `update-rule-loading-and-packaging`)
- [ ] 1.2 Validate rule files against the schema at load time; v1 files remain accepted
- [ ] 1.3 Add `metacrafter rules validate [PATH]`
- [ ] 1.4 Implement `match: regex` with length pre-check and optional `regex` timeout
- [ ] 1.5 Implement `match: registry` using the registry record regexp and checksum
- [ ] 1.6 Implement `validator: checksum:<algorithm>` via the registry checksum module
- [ ] 1.7 Honour `validator` for all data match types, `fieldrule` on field rules, `minlen`/`maxlen` on text rules
- [ ] 1.8 Honour `deprecated` (skip unless `--include-deprecated`) and `priority`

## 2. Registry-derived detectors

- [ ] 2.1 Build detectors for registry records with regexp and no rule
- [ ] 2.2 Implement gating modes `off|stable|all` and `--registry-detectors`
- [ ] 2.3 Compute or read regexp specificity
- [ ] 2.4 Mark derived matches with `ruletype: registry` and rule id `registry:<id>`

## 3. Scoring

- [ ] 3.1 Implement scoring model v1 and the `evidence` object
- [ ] 3.2 Reuse `metacrafter_registry` scoring if exposed as a function
- [ ] 3.3 Add `--legacy-scoring` and `scoring_model` in output
- [ ] 3.4 Make threshold comparisons inclusive across value and date paths

## 4. Dates, cache and packs

- [ ] 4.1 Map qddate format keys via `crosswalk("qddate", fmt)`; report the pattern record id
- [ ] 4.2 Implement the compiled-ruleset cache with content-hash keys
- [ ] 4.3 Add `--pack <id>` once the registry packs endpoint exists

## 5. Tests and docs

- [ ] 5.1 Unit tests per match type, checksum validator, deprecated and priority handling
- [ ] 5.2 Golden tests: IBAN with valid and invalid check digits; hyphenated ISBN-13
- [ ] 5.3 Test that derived detectors in `stable` mode reject every registry negative example of their record
- [ ] 5.4 Benchmark: load time and scan throughput with and without derived detectors
- [ ] 5.5 Update `docs/docs/integrations/rules.md` with the v2 format and scoring explanation
