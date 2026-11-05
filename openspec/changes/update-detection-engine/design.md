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
validated_rate = values that match the pattern and pass the validator / considered values
value          = validated_rate x specificity          (specificity = 1 for curated rules)
confidence     = 100 x value + 10 x name_weight + country_bonus - draft_penalty
                 clamped to 0..100
field-only     = 60 x priority_weight                  (1 -> 1.0, 2 -> 0.8, 3 -> 0.6)
```

- `name_weight` is the priority weight of a field rule for the same datatype that matched
  this field (0 when none did); it is a bonus, never a penalty, because most column names
  are arbitrary.
- `country_bonus` is +5 / -10 for registry-derived detectors when the scan country is known.
- `draft_penalty` is 5 for draft registry records.
- Validators and checksums filter value evidence (as in pre-1.0 releases); `checksum_rate`
  in `evidence` reports the share of pattern hits that passed.
- For curated data rules without a field bonus this equals the pre-1.0 share of matching
  values, so existing thresholds keep working. The visible change is that field-name-only
  matches score at most 60 instead of 100.
- Threshold comparisons are inclusive.

`evidence` records `hit_rate`, `checksum_rate`, `specificity`, `name_score`,
`country_bonus`, `draft_penalty`, `sampled_count`, `considered_count`, `matched_count`,
`empty_count` and `scoring_model: "v1"`.

### Decision: Regex safety

Registry CI already rejects possessive quantifiers and empty matches. In addition:
- values longer than the rule `maxlen` (or 256 when unset) are skipped before matching;
- patterns are compiled once;
- if the optional `regex` package is installed, matching uses its timeout (100 ms per value).

### Decision: Rule loading cache

Profiling showed that building unused pyparsing `oneOf` parsers for text rules and parsing
YAML dominated load time (4.6 s for the default pack). Text rules now match by set
membership without a parser, YAML uses the libyaml loader when available, parsed files are
cached in-process by (path, mtime, size), and compiled pattern expressions are cached by
source text. Loading the default pack with registry detectors takes about 0.4 s, so a
cross-process disk cache is not needed.

## Risks / Trade-offs

- **Score changes break existing confidence thresholds in user pipelines.** Mitigation:
  `--legacy-scoring` for one minor release; `scoring_model` in the report.
- **Derived detectors raise false positives.** Mitigation: stable-only default; negative
  examples checked in `add-ecosystem-contract-tests` before a record qualifies.
- **Performance with about 1000 more detectors.** Mitigation: length and type pre-filtering;
  benchmark gate in the contract-tests change.
