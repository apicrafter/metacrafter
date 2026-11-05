## 1. Rule format v2

- [x] 1.1 Write `metacrafter/rules/schema/rule-v2.schema.json` (extends the typed schema from `update-rule-loading-and-packaging`)
- [x] 1.2 Validate rule files against the schema at load time; v1 files remain accepted
- [x] 1.3 Add `metacrafter rules validate [PATH]`
- [x] 1.4 Implement `match: regex` with length pre-check (the optional `regex`-package timeout is not implemented; values are capped at `maxlen`, default 256)
- [x] 1.5 Implement `match: registry` using the registry record regexp and checksum
- [x] 1.6 Implement `validator: checksum:<algorithm>` via the registry checksum module
- [x] 1.7 Honour `validator` for all data match types, `fieldrule` on field rules, `minlen`/`maxlen` on text rules
- [x] 1.8 Honour `deprecated` (skip unless `--include-deprecated`) and `priority`

## 2. Registry-derived detectors

- [x] 2.1 Build detectors for registry records with regexp and no rule
- [x] 2.2 Implement gating modes `off|stable|all` and `--registry-detectors`
- [x] 2.3 Compute or read regexp specificity
- [x] 2.4 Mark derived matches with `ruletype: registry` and rule id `registry:<id>`

## 3. Scoring

- [x] 3.1 Implement scoring model v1 and the `evidence` object
- [x] 3.2 Reuse `metacrafter_registry` scoring if exposed as a function (not exposed as a public function; the specificity measure is reimplemented for the requested records only)
- [x] 3.3 Add `--legacy-scoring` and `scoring_model` in output
- [x] 3.4 Make threshold comparisons inclusive across value and date paths

## 4. Dates, cache and packs

- [x] 4.1 Map qddate format keys via `crosswalk("qddate", fmt)`; report the pattern record id
- [x] 4.2 Implement the rule loading cache (in-process; see design.md)
- [ ] 4.3 Add `--pack <id>` once the registry packs endpoint exists (blocked: `add-datatype-packs` is not implemented in the registry)

## 5. Tests and docs

- [x] 5.1 Unit tests per match type, checksum validator, deprecated and priority handling
- [ ] 5.2 Golden tests: IBAN with valid and invalid check digits (done); hyphenated ISBN-13 (open: depends on the ISBN rule fix in metacrafter-rules)
- [ ] 5.3 Test that derived detectors in `stable` mode reject every registry negative example of their record
- [ ] 5.4 Benchmark: load time and scan throughput with and without derived detectors
- [x] 5.5 Update `docs/docs/integrations/rules.md` with the v2 format and scoring explanation
