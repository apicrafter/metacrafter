---
title: "Custom rules"
description: "YAML rules, match engines, validators, registry detectors and scoring"
---
# Custom rules

Metacrafter loads rules from every directory in the effective `rulepath`, in order:

1. The default pack: the pinned [metacrafter-rules](https://github.com/apicrafter/metacrafter-rules)
   GitHub release, downloaded once and verified by SHA-256 (or `rules_dir`, an installed
   `metacrafterext`, or the cache; see [Installation](/getting-started/installation)).
   Skipped when `auto_rules: false`.
2. `rulepath` entries from `.metacrafter`.
3. `--rulepath` flags.

A later rule with the same rule id replaces the earlier one (logged at INFO). Rules are
never loaded implicitly from a `rules/` folder in the current directory.

## Rule file format

Rule files are YAML. Format version 2 is described by a JSON Schema published at
[`/schema/rule-v2.schema.json`](pathname:///schema/rule-v2.schema.json) and shipped in the package
(`metacrafter/rules/schema/rule-v2.schema.json`). Files without `format_version` are
version 1; they use the same structure and stay valid.

```yaml
format_version: 2
name: Payments
description: Payment identifiers
context: finances        # dot-separated contexts, e.g. pii.ru
lang: common
country_code: [gb, de]   # optional
rules:
  ibanbyregistry:
    key: iban            # registry datatype id or alias
    name: IBAN by registry pattern
    type: data
    match: registry
    rule: iban
```

Validate files before use:

```bash
metacrafter rules validate ./custom_rules
```

Invalid rules are skipped with a diagnostic, never loaded as unchecked patterns.

### Rule keys

| Key | Meaning |
|---|---|
| `key` | Registry datatype id or alias. Check with `metacrafter rules check --registry`. |
| `type` | `field` (column names) or `data` (values) |
| `match` | `text`, `ppr`, `func`, `regex` or `registry` (see below) |
| `rule` | Pattern, keyword list, function path or registry id |
| `validator` | Dotted path to `(value) -> bool`, or `checksum:<algorithm>` |
| `priority` | 1 (strongest) to 3; weights field-name evidence |
| `minlen`, `maxlen` | Value length bounds (honoured for every match type) |
| `fieldrule`, `fieldrulematch` | Field-name guard (`text` or `ppr`) for data and field rules |
| `is_pii` | Boolean; marks matches as PII |
| `imprecise` | Skipped unless `--include-imprecise` |
| `deprecated` | Skipped unless `--include-deprecated` |

## Match engines

| `match` | Behaviour |
|---------|-----------|
| `text` | Case-insensitive match against comma-separated tokens |
| `ppr` | PyParsing expression in a restricted namespace |
| `func` | Importable Python function `(value) -> bool` |
| `regex` | Python regular expression; the whole value must match |
| `registry` | Uses the regexp and checksum of the registry record named in `rule` |

### Registry patterns

`match: registry` keeps detection logic in the registry and heuristics in the rule:

```yaml
ibanbyregistry:
  key: iban
  type: data
  match: registry
  rule: iban
  fieldrule: iban,account
  fieldrulematch: text
```

The checksum of the record (here `iban_mod97`) is applied automatically. The rule `key`
must resolve to the same record as `rule`.

### Regular expressions

```yaml
docnumber:
  key: doc_number
  type: data
  match: regex
  rule: '[A-Z]{2}\d{6}'
  maxlen: 8
```

Values longer than `maxlen` (default 256 for regex rules) are not matched.

### Checksums

```yaml
cardnumber:
  key: credit_card_number
  type: data
  match: regex
  rule: '\d{16}'
  validator: checksum:luhn
```

Checksum algorithms are the registry's own (49 algorithms, for example `luhn`, `iban_mod97`,
`gtin`, `ru_inn`), vendored into metacrafter so they work without the registry package. An
unknown algorithm name disables the rule with a diagnostic.

### Function

```yaml
runpabyfunc:
  key: runpa
  name: Russian legal act / law
  maxlen: 500
  minlen: 3
  priority: 1
  match: func
  type: data
  rule: metacrafterext.rules.ru.gov.is_ru_law
```

Any importable path works (`mypackage.validators.is_widget_id`) as long as the module
is on `PYTHONPATH`. Function rules are trusted executable plugins. The `metacrafterext`
validators of the default pack are imported from the verified release archive (or from
`rules_dir`).

### Exact text

```yaml
midname:
  key: person_midname
  name: Person midname by known field name
  rule: midname,secondname,middlename,mid_name,middle_name
  type: field
  match: text
```

### PyParsing

```yaml
rukadastr:
  key: rukadastr
  name: Russian land territory cadastral identifier
  rule: Word(nums, min=1, max=2) + Literal(':').suppress() + Word(nums, min=1, max=2) + Literal(':').suppress() + Word(nums, min=6, max=7) + Literal(':').suppress() + Word(nums, min=1, max=6)
  maxlen: 20
  minlen: 12
  priority: 1
  match: ppr
  type: data
```

`ppr` rules run with PyParsing primitives only (`Word`, `Literal`, `nums`, `alphas`, …)
and bounded repetition (`expr * 3`, `expr * (0, 6)`, `expr[0, ...]`). `import`, `eval`
and attribute access outside a few parser methods are rejected.

## Registry-derived detectors

Many registry datatypes have a regexp but no rule. Metacrafter builds detectors for them
at load time:

| `--registry-detectors` | Detectors added |
|---|---|
| `off` | none |
| `stable` (default) | stable records with a checksum, or with a regexp that matches at most 20% of other records' examples |
| `all` | every record with a regexp; those outside the `stable` criteria are imprecise |

Derived matches have rule ids `registry:<id>` and `ruletype: registry`. Set the default
in `.metacrafter` with `registry_detectors: off|stable|all`.

## Confidence scoring

Confidence is 0–100 (`scoring_model: v1`):

- **Value evidence**: the share of non-empty values that match the pattern and pass the
  validator or checksum, times the pattern specificity (1 for curated rules).
- **Field-name bonus**: up to +10 when a field rule for the same datatype also matched.
- **Country**: +5 / −10 for registry detectors when `--country-codes` is set.
- **Draft penalty**: −5 for draft registry records.
- **Field-name-only matches** score 60 × priority weight (1 → 60, 2 → 48, 3 → 36).

Each match has an `evidence` object with `hit_rate`, `checksum_rate`, `specificity`,
`name_score`, counts and `scoring_model`. `--legacy-scoring` restores the pre-1.0 scores
(share of matching values; 100 for field-name matches).

Confidence is a heuristic, not a calibrated probability.

## Inspect

```bash
metacrafter rules list --rulepath ./custom_rules
metacrafter rules check --registry
metacrafter rules validate ./custom_rules
```

See [rules](/commands/rules) and [Architecture](/development/architecture).
