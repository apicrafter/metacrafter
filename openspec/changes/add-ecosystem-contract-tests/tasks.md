## 1. Lock and fixtures

- [ ] 1.1 Add `ecosystem.lock` with the registry version, manifest hash and `metacrafter-rules` version
- [ ] 1.2 Add a CI step that loads the pinned registry snapshot through the adapter (no live network in unit tests)

## 2. Key resolution gate

- [ ] 2.1 Fold `scripts/ecosystem_check.py` checks (key uniqueness, ppr compilation) into `metacrafter rules check`
- [ ] 2.2 Run `metacrafter rules check --registry --strict` in CI
- [ ] 2.3 File the list of 32 unresolved keys in `metacrafter-rules` with suggested targets (`boolean`, `latitude`, `longitude`, `percentage`, `filesize`, `casin`, ...)

## 3. Example-based rule tests

- [ ] 3.1 Implement `metacrafter rules test --registry-examples [--rule ID] [--format json]`
- [ ] 3.2 Report per rule: positives tested and matched, negatives tested and accepted
- [ ] 3.3 Add `tests/rule_thresholds.yaml` (minimum recall per rule, zero accepted negatives for checksum types)
- [ ] 3.4 Run the gate in CI; document how to record accepted exceptions

## 4. Scheduled compatibility

- [ ] 4.1 Add `.github/workflows/ecosystem.yml` (weekly and manual) against HEAD of rules and registry
- [ ] 4.2 Open or update a single tracking issue on failure

## 5. Benchmark

- [ ] 5.1 Create synthetic labelled datasets under `tests/benchmark/` (no real PII)
- [ ] 5.2 Implement `metacrafter bench` printing per-datatype precision, recall and time
- [ ] 5.3 Gate CI on no regression beyond a tolerance stored in the repo
