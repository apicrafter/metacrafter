---
title: "Ecosystem contract"
description: "How metacrafter, metacrafter-rules and metacrafter-registry are kept compatible"
---
# Ecosystem contract

Metacrafter depends on two sibling projects:

- [metacrafter-rules](https://github.com/apicrafter/metacrafter-rules): the default rule pack;
- [metacrafter-registry](https://github.com/apicrafter/metacrafter-registry): the datatypes
  that rule keys refer to.

`ecosystem.lock` pins the commits that CI and releases are tested against. Neither package is
on PyPI yet; `python scripts/ecosystem_ref.py <name>` prints the pip requirement for the
pinned commit.

## Checks on every pull request

| Check | Command | Data |
|---|---|---|
| Rule keys resolve to registry ids or aliases | `metacrafter rules check --registry --strict --allow-unknown tests/ecosystem/known_unresolved_rule_keys.yaml` | pinned registry checkout |
| Rules on registry examples | `metacrafter rules test --registry-examples --thresholds tests/ecosystem/rule_thresholds.yaml` | pinned registry checkout |
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

## Updating baselines

After an intended change (a newer pinned rule pack or registry, or better rules):

```bash
metacrafter rules check --registry --registry-path ../metacrafter-registry/data --output-format json
metacrafter rules test --registry-examples --registry-path ../metacrafter-registry/data \
  --write-thresholds tests/ecosystem/rule_thresholds.yaml -o /dev/null
metacrafter evaluate tests/benchmark/fixture.yaml --write-baseline tests/benchmark/baseline.json
```

Review the diff: thresholds should only rise, and exceptions should only shrink.

## Weekly check

`.github/workflows/ecosystem.yml` runs the same checks against the current HEAD of both
repositories every Monday and opens or updates one issue labelled `ecosystem-drift` when they
fail.
