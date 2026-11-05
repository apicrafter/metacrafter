## Context

`RulesProcessor.match_dict()` runs field rules (100 confidence on name hit), then data
rules (share of non-empty values that match, compared with `>` against a threshold), then
qddate when nothing else matched.

The registry package already implements:
- `match(value)`: regexp plus checksum;
- `identify(values, field_name, country)`. Its score is 60 × weighted regexp hit rate ×
  specificity, +25 × checksum pass rate, +25 × field-name similarity, ± a country bonus,
  and −5 for drafts.

## Goals / Non-Goals

**Goals:**
- Use registry regexps and checksums without copying them into YAML.
- One confidence scale across rules, registry detectors and LLM results.
- Explainable results (`evidence`) that support calibration later.

**Non-Goals:**
- Calibrated probabilities. Scores remain heuristic until an evaluation set exists (Codex F06).
- Replacing pyparsing rules. `ppr` stays supported.
- Sampling strategy (Codex `update-source-scanning-and-sampling`).

## Decisions

### Decision: Rule format v2

New keys and values, all optional unless stated:

| Key | Values | Meaning |
|---|---|---|
| `match` | `text`, `ppr`, `func`, `regex`, `registry` | `regex`: `rule` is an anchored Python pattern. `registry`: `rule` is a registry id. |
| `validator` | dotted path, or `checksum:<algorithm>` | Applied to every candidate value for all data match types |
| `priority` | 1–3 | Field-evidence weight. 1 is strongest. |
| `deprecated` | bool | Rule loads but does not run unless `--include-deprecated` |
| `is_pii` | bool | Typed boolean; strings rejected by schema |
| `format_version` | `2` | File-level marker; absent means v1 (still accepted) |

The schema is stored in `metacrafter/rules/schema/rule-v2.schema.json` and published with
the docs. `metacrafter-rules` vendors the same file in CI.

### Decision: Registry match type

`match: registry` with `rule: iban`:
- compiles the registry `regexp` for `iban`;
- applies `checksum` (`iban_mod97`) automatically unless the rule sets `validator`;
- takes `key` from the registry id. A rule `key` that differs is a schema error.

The rule may add `fieldrule`, `minlen`/`maxlen`, `priority`, `imprecise` and
`country_code`. This keeps detection logic in the registry and heuristics in rules.

### Decision: Registry-derived detectors

At load time, for each registry record that has a `regexp` and no rule targeting its
canonical id:

| Mode | Included records |
|---|---|
| `off` | none |
| `stable` (default) | `status: stable` and (`checksum` present or specificity ≥ 0.8) |
| `all` | all with regexp; non-stable or low-specificity ones are marked `imprecise` |

Specificity is the registry's broadness inverse. If the API does not publish it, it is
computed locally the same way as `Registry._broadness()`. Derived detectors get rule ids
`registry:<id>` and `ruletype: registry`.

### Decision: Scoring model v1

For a candidate datatype on one field:

```
value_score  = hit_rate × specificity                  (0..1)
checksum     = checksum_pass_rate if checksum else n/a  (0..1)
name_score   = max(field-rule hit by priority, registry name similarity)   (0..1)
score = 60·value_score + 25·checksum + 25·name_score + country_bonus − draft_penalty
clamped to 0..100
```

- Field-rule hits map by priority: 1 → 1.0, 2 → 0.8, 3 → 0.6.
- A field-only match (no data evidence) is capped at 60.
- The threshold comparison is inclusive.

`evidence` records:
- `hit_rate`, `checksum_rate`, `specificity`, `name_score`, `country_bonus`,
  `draft_penalty`;
- `sampled`, `matched`, `empty`;
- `scoring_model: "v1"`.

If `metacrafter_registry` exposes the scoring as a function, call it instead of duplicating
the formula.

### Decision: Regex safety

Registry CI already rejects possessive quantifiers and empty matches. In addition:
- values longer than the rule `maxlen` (or 256 when unset) are skipped before matching;
- patterns are compiled once;
- if the optional `regex` package is installed, matching uses its timeout (100 ms per value).

### Decision: Compiled ruleset cache

The cache key is a SHA-256 over the sorted rule file paths and contents, the registry
version and the detector mode. Cached data excludes Python callables: `func` rules and
validators are re-imported. The cache lives in `~/.cache/metacrafter/rules/`.

## Risks / Trade-offs

- **Score changes break existing confidence thresholds in user pipelines.** Mitigation:
  `--legacy-scoring` for one minor release; `scoring_model` in the report.
- **Derived detectors raise false positives.** Mitigation: stable-only default; negative
  examples checked in `add-ecosystem-contract-tests` before a record qualifies.
- **Performance with about 1000 more detectors.** Mitigation: length and type pre-filtering;
  benchmark gate in the contract-tests change.
