# Change: Ecosystem contract tests between metacrafter, rules and registry

## Why

The ecosystem has no enforced contract:
- 32 rule keys in `metacrafter-rules` do not exist in the registry. 31 of them are generic
  field rules named `<token>_<category>`; the 32nd is `ca_sin` versus registry `casin`.
- 14 keys resolve only through aliases.
- Measured against registry examples, current data rules accept 67% of positive examples
  and 11 of 159 negatives.

Nobody notices regressions until a user does.

`scripts/ecosystem_check.py` was written for `add-ecosystem-alignment-workflow` (July
2026). It sits uncommitted in the main checkout and reads the registry YAML directly
instead of using aliases.

The registry already pins `metacrafter-rules` in its own `ecosystem.lock` and runs a weekly
coverage diff. Metacrafter has no matching lock.

This change is phase 5 of `devdocs/MODERNIZATION_PLAN.md`. It runs in parallel with phases
1–4.

## What Changes

- Add a key resolution gate: every rule key must resolve to a registry id or alias. It runs
  in CI through `metacrafter rules check --registry --strict` against the pinned registry
  snapshot.
- Add example-based rule tests: `metacrafter rules test --registry-examples`. It runs every
  data rule and registry-derived detector against the registry examples of its datatype
  and reports recall on positives and acceptance of negatives. A threshold file sets the
  CI gate.
- Add `ecosystem.lock` in metacrafter, pinning the registry version (manifest hash) and the
  `metacrafter-rules` version used by CI and releases.
- Add a scheduled workflow (weekly). It runs the gates against the HEADs of
  `metacrafter-rules` and `metacrafter-registry` and opens or updates one tracking issue
  on failure.
- Add a detection benchmark: a small labelled dataset set (synthetic, no real PII) with
  expected datatypes per field. It reports per-datatype precision, recall and scan time,
  and gates regressions.

## Relationship to other changes

- **Supersedes** the July `add-ecosystem-alignment-workflow`. The useful parts of
  `scripts/ecosystem_check.py` are folded into `rules check`.
- **Extends** `rules test` from `add-report-and-rule-product-tools` (Codex F03): registry
  examples become a test source alongside rule-local examples.
- **Prepares** the evaluation work of Codex F06; the benchmark here is the regression gate,
  not a calibration study.
- **Depends on** `add-registry-integration` (adapter, alias resolution) and
  `update-detection-engine` (derived detectors, evidence).

## Impact

- Affected specs: `ecosystem-contract`
- Affected code: `metacrafter/rules/` (check and test commands), `tests/benchmark/`,
  `ecosystem.lock`, `.github/workflows/ecosystem.yml`
- Cross-repo:
  - `metacrafter-rules` fixes the 32 keys or the registry adds aliases;
  - `metacrafter-rules` adopts the example gate in its own CI and commits its untracked
    `.github/workflows/ci.yml`.
