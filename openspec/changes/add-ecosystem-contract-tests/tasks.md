## 1. Lock and fixtures

- [x] 1.1 Add `ecosystem.lock` with the registry version, manifest hash and `metacrafter-rules` version
- [x] 1.2 Add a CI step that loads the pinned registry snapshot through the adapter (no live network in unit tests)

## 2. Key resolution gate

- [x] 2.1 Fold `scripts/ecosystem_check.py` checks (key uniqueness, ppr compilation) into `metacrafter rules check`
- [x] 2.2 Run `metacrafter rules check --registry --strict` in CI
- [x] 2.3 Record the unresolved keys with suggested targets in `tests/ecosystem/known_unresolved_rule_keys.yaml` (filing them upstream in metacrafter-rules is a follow-up)

## 3. Example-based rule tests

- [x] 3.1 Implement `metacrafter rules test --registry-examples [--rule ID] [--format json]`
- [x] 3.2 Report per rule: positives tested and matched, negatives tested and accepted
- [x] 3.3 Add `tests/rule_thresholds.yaml` (minimum recall per rule, zero accepted negatives for checksum types)
- [x] 3.4 Run the gate in CI; document how to record accepted exceptions

## 4. Scheduled compatibility

- [x] 4.1 Add `.github/workflows/ecosystem.yml` (weekly and manual) against HEAD of rules and registry
- [x] 4.2 Open or update a single tracking issue on failure

## 5. Benchmark

- [x] 5.1 Create synthetic labelled datasets under `tests/benchmark/` (no real PII)
- [x] 5.2 Extend `metacrafter evaluate` with `--baseline`, `--write-baseline` and `--tolerance` (per-datatype precision, recall and time)
- [x] 5.3 Gate CI on no regression beyond a tolerance stored in the repo
