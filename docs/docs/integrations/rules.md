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
| `validator` | Dotted path to `(value) -> bool`, or `checksum:<algorithm>`. On a field rule it checks the values: the name match is kept when at least half of the non-empty sampled values pass; a field without values keeps it |
| `needs_values` | `true` on a field rule: the name match is dropped when the field has no non-empty values in the sample or in the records up to the 1000th (then the validator checks those). For datatypes the name does not prove, such as Unix milliseconds in a `created_at` field. Metacrafter 2.1 and earlier ignore it |
| `priority` | 1 (strongest) to 3; weights field-name evidence |
| `minlen`, `maxlen` | Value length bounds (honoured for every match type) |
| `fieldrule`, `fieldrulematch` | Field-name guard (`text` or `ppr`) for data and field rules |
| `fieldregex` | Field-name guard by Python regular expression, searched in the last part of the field name without regard to case (`date\|datum\|_at$`). With `fieldrule` too, either guard lets the rule run. Metacrafter 2.1 and earlier ignore it: a value rule that must not run unguarded there also needs a `fieldrule` |
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

### Field-name guards

A value rule whose values alone are ambiguous can ask for a field name. `fieldrule` lists the
names (`fieldrulematch: text`) or gives a PyParsing expression (`fieldrulematch: ppr`);
`fieldregex` gives a regular expression:

```yaml
unixtimemsbydate:
  key: unixtime_ms
  type: data
  match: regex
  rule: '-?\d{11,13}'
  fieldregex: 'date|datum|data|fecha|_at$|time'
```

13-digit numbers in `TicketReportedOnDate` or `Data założenia` are then Unix milliseconds,
and in `code` they are not looked at. The keys of a nested object whose keys are data (the
words of an inverted index: many distinct keys across records, lists or numbers as values)
pass no field-name guard and match no field-name rule.

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
| `checksum` (default) | stable records with a checksum metacrafter validates (bank cards, Belgian IBAN and VAT, Italian codice fiscale, …) |
| `stable` | stable records with a checksum, or with a regexp that matches at most 20% of other records' examples |
| `all` | every record with a regexp; those outside the `stable` criteria are imprecise |

Derived matches have rule ids `registry:<id>` and `ruletype: registry`. Set the default
in `.metacrafter` with `registry_detectors: off|checksum|stable|all`. Detectors without a
checksum find more datatypes but also report format look-alikes, such as `cficode` on a
column of six-letter status codes; use them for exploratory scans.

Registry *patterns* (`kind: pattern`) are representations of a datatype, such as
`iso639_2` (three-letter codes) for `language` or `countrycode_alpha2` for `country`. A
pattern detector reports the datatype in the pattern's `semantic_type` and the pattern id
in `pattern`; the datatype id counts as field-name evidence, so a column `language` with
`eng` reports `language` with `pattern: iso639_2`.

Values alone do not identify every datatype, so a detector reports a match only when the
evidence supports it:

- **Generic patterns need the field name.** Each regexp is tested against classes of
  ordinary tokens: integers of 1 to 6 digits (one class per length), plain words,
  title-case words, short acronyms, two-letter codes (`ok`, `NY`) and letter-and-digit
  codes (`WGS84`, `MP3`, `A320`). A regexp without a checksum that accepts at least
  30% of one class (for example `\d{1,4}`, `\d{4}` or `[A-Za-z]{2,63}`) matches only when
  the field name names the datatype or its broader `parent`. Without this a column of
  counters would be reported as flight numbers and a column of words as top-level domains.
- **Field-name evidence** comes from a field rule for the same datatype, or from the
  registry id and aliases: their tokens, without the record's country codes and common
  tokens such as `code`, `number` or `id`, must appear in the field name. `am_postal_code`
  needs `postal` (`postal_code`, `postalCode`); `tld` needs `tld`, or the alias
  `domain_zone`. The id of the record's `parent` counts at half weight: a field `language`
  supports a code type whose parent is `language` (name score 0.5, bonus +5).
- **Country-specific datatypes** (registry `country`) are skipped when `--country-codes`
  names other countries, and when no country is set and the record has no checksum, unless
  the field name names the type together with its country (`am_postal_code`,
  `postal_code_am`; `inn` has no country code in its id, so `inn` is enough). A field
  `driver_license` does not lift this for `am_driver_license`.

`--explain` lists the skipped detectors with the reason.

Field rules also try the field name without a language suffix: when `abstract_en` or
`title_ru` matches no field rule, `abstract` or `title` is tried. The language keys of a
nested object work the same way: `title.ru` and `title.en` (from `{"title": {"ru": …, "en": …}}`)
try `title`. Only common language codes
count as suffixes (`en`, `ru`, `fr`, `de`, `es`, `hy`, …); codes that are also common words or
abbreviations, such as `id`, `no` or `ms`, do not, so `user_id` stays `user_id`.

`--langs` and `--country-codes` select language- and country-specific rule files. Rules
with `lang: common` and rules with `country_code: xx` (or no country) apply with any filter,
so `--country-codes ru` still detects emails, URLs and dates.

Curated rules follow two similar rules when `--country-codes` is not set:

- a field-name match from a country-specific rule file (`country_code` other than `xx`)
  is dropped when a universal rule matched the same field with at least its priority, or
  any rule matched with a higher priority: a field `city` reports `city`, not also `egcity`
  and `trcity`;
- a value match from a country-specific rule without a validator and without field-name
  evidence is dropped when a universal rule matched the same values with at least the
  same confidence: GUIDs report `uuid`, not also `rufias`;
- a registry detector match without field-name evidence is dropped when a value rule whose
  field rule names the field matched it too: Czech RÚIAN address point codes (`AdresniBod`)
  pass the IČO check digit, and the RÚIAN rule names them;
- a value match without field-name evidence is dropped when a value rule with a field guard
  (`fieldrule`), which runs only on fields named for it, matched at least as many of the
  values: NTEE codes in `NTEE_CD` also have the shape of ICD-10 codes, OKPD2 codes in `okpd2`
  that of IPv4 addresses. Matches beside a value rule that a field rule backs (without a
  guard) are kept.

Values also veto field-name matches: a field rule with a `validator` keeps its match only
when at least half of the non-empty sampled values pass it (`orgname` needs names, not the
codes 1, 2, 5 or URLs), and a column of JSON booleans gets `boolean` only (a `status` of
`true`/`false` is not a `category`).

## Confidence scoring

Confidence is 0–100 (`scoring_model: v1`):

- **Value evidence**: the share of non-empty values that match the pattern and pass the
  validator or checksum, times the pattern specificity (1 for curated rules).
- **Field-name bonus**: up to +10 when a field rule for the same datatype also matched
  (for registry detectors, also when the field name names the datatype).
- **Country**: +5 / −10 for registry detectors when `--country-codes` is set.
- **Draft penalty**: −5 for draft registry records.
- **Field-name-only matches** score 60 × priority weight (1 → 60, 2 → 48, 3 → 36),
  moved by how well the values fit the datatype (`evidence.value_fit`). The checks of the
  datatype are its value rules (imprecise ones included), the regexps of its registry record
  and pattern records with their checksums, and the field rule's `validator`. Each check has
  a pass rate on up to 200 distinct values and a background broadness `b` (the share of
  ordinary tokens, numbers or words, that it passes):
  - `support` = the best `rate × (1 − b)`: a specific check that passes the values adds up
    to +30 (a field `email` of e-mail addresses: 90);
  - `misfit` = the share of values that fail every shape check × the largest `b` of those
    checks: values that even a broad check rejects take up to −10 (word positions `58`,
    `226` under the key `year`: 50). Dictionary rules (`match: text`) only add support.

  A datatype without checks (`name`, `category`, `id`) keeps 60 × priority weight. So
  `--confidence 60` drops name matches whose values do not fit, and `--confidence 70`
  keeps the name matches whose values do.

Each match has an `evidence` object with `hit_rate`, `checksum_rate`, `specificity`,
`name_score`, counts and `scoring_model`; a field-name match with checks of its datatype
also has `value_fit` (`support`, `misfit`, the best `check` with its `rate` and `broadness`,
`shape_rate`, `considered_count`). `--legacy-scoring` restores the pre-1.0 scores
(share of matching values; 100 for field-name matches).

Confidence is a heuristic, not a calibrated probability.

Scans report matches with a confidence of at least 50 (`--confidence`, default 50). This
drops partial value matches and chance checksum matches: about one value in ten passes a
mod-10 or mod-11 check by chance, so a column of Russian INNs also scores 6–12 for ISBN-10,
NHS and NPI numbers. It also drops field-name-only matches of priority 2 and 3 rules.
`--confidence 5` reports them.

## Inspect

```bash
metacrafter rules list --rulepath ./custom_rules
metacrafter rules check --registry
metacrafter rules validate ./custom_rules
```

See [rules](/commands/rules) and [Architecture](/development/architecture).
