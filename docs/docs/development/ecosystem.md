---
title: "Ecosystem contract"
description: "How metacrafter, metacrafter-rules and metacrafter-registry are kept compatible"
---
# Ecosystem contract

Metacrafter relies on two sibling projects, neither of which is a Python dependency:

- [metacrafter-rules](https://github.com/apicrafter/metacrafter-rules): the default rule pack,
  downloaded from a GitHub release. The tag and archive SHA-256 are pinned in
  `metacrafter/rules/pack.py` and `ecosystem.lock`.
- [metacrafter-registry](https://github.com/apicrafter/metacrafter-registry): the datatypes
  that rule keys refer to, read from the registry website. `ecosystem.lock` records the
  registry version and git sha that the baselines in `tests/ecosystem/` were made with.

## Checks on every pull request

| Check | Command | Data |
|---|---|---|
| Rule keys resolve to registry ids or aliases | `metacrafter rules check --registry --strict --allow-unknown tests/ecosystem/known_unresolved_rule_keys.yaml` | live registry snapshot |
| Rules on registry examples | `metacrafter rules test --registry-examples --thresholds tests/ecosystem/rule_thresholds.yaml` | live registry snapshot |
| Registry checksum names exist in the vendored algorithms | `scripts/ecosystem_gate.py` | live registry snapshot |
| Detection benchmark | `pytest tests/test_ecosystem_contract.py` (`metacrafter evaluate tests/benchmark/fixture.yaml --baseline tests/benchmark/baseline.json`) | offline registry fixture |

- **Unresolved keys.** `known_unresolved_rule_keys.yaml` lists the keys of the pinned rule pack
  that the registry does not know yet, with a suggested target. A new unresolved key fails CI.
  Fix keys in metacrafter-rules (or add aliases in the registry), then remove them from the list.
- **Registry examples.** Each data rule and registry-derived detector runs on the positive and
  negative examples of its datatype. `rule_thresholds.yaml` stores the minimum recall per rule
  (the current value, so it can only go up) and the rules allowed to accept negative examples
  today. A rule that newly misses a positive or accepts a negative fails CI.
- **Benchmark.** Synthetic labelled data (no real personal data) with per-datatype precision
  and recall. A drop of more than 0.02 below `baseline.json` fails.

`scripts/ecosystem_gate.py` runs the registry checks. They **block** only while the live
registry has the version and git sha recorded in `ecosystem.lock`. After the registry
publishes a new version, the checks still run and report warnings, so registry changes do not
block unrelated pull requests. The weekly workflow opens the drift issue, and updating the
baselines and `ecosystem.lock` makes the gate blocking again.

## Updating baselines

After an intended change (a newer pinned rule pack or registry, or better rules):

```bash
metacrafter registry fetch --to /tmp/registry
metacrafter rules check --registry --registry-path /tmp/registry --output-format json
metacrafter rules test --registry-examples --registry-path /tmp/registry \
  --write-thresholds tests/ecosystem/rule_thresholds.yaml -o /dev/null
metacrafter evaluate tests/benchmark/fixture.yaml --write-baseline tests/benchmark/baseline.json
```

Then record the registry `version` and `git_sha` (from `/tmp/registry/index.json`) in
`ecosystem.lock` and update the registry snapshot shipped in the package
(`metacrafter/registry/bundled/`):

```bash
python scripts/bundle_registry_snapshot.py --from /tmp/registry
```

A test fails while the bundled snapshot does not match `ecosystem.lock`. To move to a new rules release, update `release` and `sha256` in both
`ecosystem.lock` and `metacrafter/rules/pack.py`. Review the diff: thresholds should only
rise, and exceptions should only shrink.

## Weekly check

`.github/workflows/ecosystem.yml` runs the same checks every Monday against the HEAD of
metacrafter-rules (`METACRAFTER_RULES_DIR` pointing at a checkout) and the live registry. When
they fail, it opens or updates one issue labelled `ecosystem-drift`.
