## MODIFIED Requirements

### Requirement: Unified Confidence Scoring

The engine SHALL compute a confidence score from 0 to 100 for every candidate datatype. The
score of a value match SHALL combine:
- the share of values that match the pattern and pass the rule validator or checksum,
  weighted by pattern specificity (1 for curated rules);
- a field-name bonus when a field rule for the same datatype matched, weighted by rule priority;
- a country bonus or penalty for registry-derived detectors when the scan country is known;
- a penalty for draft registry records.

The score of a field-name match SHALL be `60 x priority weight` (priority 1, 2, 3: weight 1.0,
0.8, 0.6), plus up to 30 for the support and minus up to 10 for the misfit of the values (see
Value Fit of Field-Name Matches), clamped to 0..100. A field-name match whose datatype has no
value checks SHALL score `60 x priority weight`.

Each match SHALL include an `evidence` object with these components, the sampled and matched
counts, and the scoring model version. Threshold comparisons SHALL be inclusive.

#### Scenario: Checksum filters value evidence

- **WHEN** a field's values all match an IBAN regexp and half of them pass the checksum
- **THEN** the IBAN match reports a confidence based on the validated values only
- **AND** its `evidence` shows `hit_rate` 1.0 and `checksum_rate` 0.5

#### Scenario: Field-name match without value checks

- **WHEN** a field name matches a priority 1 field rule of a datatype that has no value rule,
  no registry regexp and no validator (`name`)
- **THEN** the match scores 60
- **AND** its `evidence` has no `value_fit`

#### Scenario: Values that fit raise a field-name match

- **WHEN** a field `email` matches a priority 1 field rule for `email`
- **AND** its values are e-mail addresses that a specific check of `email` passes
- **THEN** the match scores more than 60 and at most 90

#### Scenario: Values that do not fit lower a field-name match

- **WHEN** a key `year` matches a priority 1 field rule for `year`
- **AND** its values (`58`, `226`, `0`) fail even the broadest shape check of `year`
- **THEN** the match scores less than 60 and at least 50
- **AND** it is reported at the default threshold and dropped at `--confidence 60`

#### Scenario: Legacy scoring

- **WHEN** a scan runs with `--legacy-scoring`
- **THEN** data-rule scores equal the share of matching non-empty values
- **AND** field-name matches score 100
- **AND** the report states the legacy scoring model

## ADDED Requirements

### Requirement: Value Fit of Field-Name Matches

For a field-name match of datatype T, the engine SHALL measure how well the field's values fit
T with the checks of T: its value rules (imprecise ones included), the regexps of its registry
record and of the registry pattern records whose `semantic_type` is T (each with its checksum),
and the validator of the field rule. The checks SHALL run on up to 200 distinct non-empty
values. Each check SHALL have a broadness: the largest share of a class of ordinary tokens
(integers by length, words, title-case words, acronyms, two-letter codes, letter-and-digit
codes) that it accepts.

- `support` SHALL be the largest `pass rate x (1 - broadness)` of the checks.
- `misfit` SHALL be the share of values that fail every shape check (all checks except
  dictionary rules, `match: text`) times the broadness of the broadest shape check.

The match `evidence` SHALL include `value_fit` with `considered_count`, `checks`, `support`,
`misfit`, the deciding `check`, its `rate` and `broadness`, and, with shape checks,
`shape_rate` and `shape_broadness`.

#### Scenario: Broad check that passes proves little

- **WHEN** the only check of `year` is `\d{4}` and every value is a four-digit number
- **THEN** `support` is the pass rate times `1 - broadness` of `\d{4}`, near 0
- **AND** the match scores about 60

#### Scenario: Dictionary misses do not count against a name

- **WHEN** the only check of `city` is a dictionary rule of city names
- **AND** none of the values is in the dictionary
- **THEN** `misfit` is 0 and the match scores 60
