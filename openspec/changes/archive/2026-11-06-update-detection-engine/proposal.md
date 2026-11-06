# Change: Detection engine v2 on top of registry patterns

## Why

The engine supports three match types (`text`, `ppr`, `func`). Confidence is only the share
of matching values. As a result, only 502 of 1812 registry records (27.7%) can be detected.
The registry already holds the material to close most of the gap:
- 1297 Python regexps;
- 49 checksum algorithms used by 85 records;
- 1363 records with examples, including 825 negative examples.

Of the 1037 datatypes without a rule:
- 663 have both a regexp and examples;
- 53 have a checksum.

Measured against registry examples, current data rules accept 67% of positive examples
(385/572). They also accept 11 of 159 negative examples, in 10 rules.

Several rule attributes are silently ignored or only partly applied:
- `priority` (666 rules);
- `deprecated`;
- `validator` outside `ppr` data rules;
- `fieldrule` on field rules;
- `minlen`/`maxlen` on `text` rules.

The date detector (qddate) is not linked to registry date-format records.

This change is phase 2 of `devdocs/MODERNIZATION_PLAN.md`. Accepted decision 3: detectors
derived from the registry are enabled by default for `stable` records only.

## What Changes

- Rule format v2, published as a JSON Schema shared with `metacrafter-rules`:
  - new `match: regex` (anchored Python `re.fullmatch`);
  - new `match: registry` (`rule: <datatype id>`). It uses the registry record's regexp and
    checksum; the rule adds constraints only.
  - new validator form `validator: checksum:<algorithm>`, backed by the registry checksum
    implementations;
  - `priority`, `deprecated`, `is_pii` (boolean) honoured;
  - `validator` honoured on every match type;
  - `fieldrule` honoured on field rules; `minlen`/`maxlen` honoured on `text` rules.
- Registry-derived detectors (`--registry-detectors off|stable|all`, default `stable`). For
  registry records with a regexp and no rule, data detectors are built at load time. Gating
  uses status, checksum presence and regexp specificity.
- A unified confidence model (0–100) with an `evidence` breakdown:
  - value hit rate weighted by pattern specificity;
  - checksum pass rate;
  - field-name evidence;
  - rule priority;
  - country context;
  - a draft penalty.

  The model is aligned with `Registry.identify()` scoring.
- Date matches map the qddate format key through `crosswalk("qddate", fmt)` to the registry
  pattern record, for example `date_dmy`.
- A compiled-ruleset cache keyed by rulepath content hash and registry version.
- `--pack <id>` selects datatypes from a registry pack once `add-datatype-packs` lands in
  the registry.

## Relationship to other changes

- **Depends on** `update-rule-loading-and-packaging` (Codex W06): typed rule schema,
  inclusive thresholds, deterministic precedence and strict diagnostics. This change extends
  that schema to v2.
- **Depends on** `add-registry-integration` for resolution, records and provenance.
- **Feeds** `add-report-v2` (the `evidence` object) and `add-ecosystem-contract-tests`
  (example-based rule tests).
- Partly realizes the July `add-ecosystem-product-features` item "Registry-driven skeleton
  rule generation". Detectors are derived at runtime instead of generated as YAML.

## Impact

- Affected specs: `rule-format`, `detection-engine`
- Affected code: `metacrafter/classify/processor.py` (split into `metacrafter/rules/` and
  `metacrafter/engine/`), `metacrafter/core.py` (date path), CLI `scan` options
- Cross-repo:
  - `metacrafter-rules` adopts the v2 schema and may convert rules to
    `match: registry`/`checksum:`;
  - `metacrafter-registry` should publish regexp broadness in the API, which this change
    can otherwise compute locally;
  - `metacrafter-registry` implements `add-datatype-packs` for `--pack`.
- **Behaviour change:** confidence values change for existing rules. Reports state the
  scoring model version. The `--legacy-scoring` flag keeps the old share-only scores for one
  minor release.
