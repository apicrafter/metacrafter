## ADDED Requirements

### Requirement: Registry-Derived Detectors

The engine SHALL build data detectors from registry datatype records that have a regexp and no
data rule targeting their canonical id. The `--registry-detectors` option SHALL accept `off`, `stable` and `all`,
with `stable` as the default:
- `stable` SHALL include only records with `status: stable` that have a checksum or a
  regexp specificity of at least 0.8;
- `all` SHALL include every record with a regexp, and SHALL mark records outside the
  `stable` criteria as imprecise.

#### Scenario: Stable detector finds an uncovered type

- **WHEN** a stable registry record with a checksum has no rule
- **AND** a column contains values that match its regexp and pass its checksum
- **THEN** the field reports a match with rule id `registry:<id>` and `ruletype: registry`

#### Scenario: Draft records excluded by default

- **WHEN** a registry record has `status: draft` and no rule
- **AND** the scan runs with default options
- **THEN** no detector is built for that record

#### Scenario: Detectors disabled

- **WHEN** a scan runs with `--registry-detectors off`
- **THEN** only rule-based detectors run

### Requirement: Unified Confidence Scoring

The engine SHALL compute a confidence score from 0 to 100 for every candidate datatype. The
score SHALL combine:
- the share of values that match the pattern and pass the rule validator or checksum,
  weighted by pattern specificity (1 for curated rules);
- a field-name bonus when a field rule for the same datatype matched, weighted by rule priority;
- a country bonus or penalty for registry-derived detectors when the scan country is known;
- a penalty for draft registry records.

Each match SHALL include an `evidence` object with these components, the sampled and matched
counts, and the scoring model version. Threshold comparisons SHALL be inclusive.

#### Scenario: Checksum filters value evidence

- **WHEN** a field's values all match an IBAN regexp and half of them pass the checksum
- **THEN** the IBAN match reports a confidence based on the validated values only
- **AND** its `evidence` shows `hit_rate` 1.0 and `checksum_rate` 0.5

#### Scenario: Field-name-only match is capped

- **WHEN** a field name matches a field rule and no value evidence exists
- **THEN** the match score does not exceed 60

#### Scenario: Legacy scoring

- **WHEN** a scan runs with `--legacy-scoring`
- **THEN** data-rule scores equal the share of matching non-empty values
- **AND** the report states the legacy scoring model

### Requirement: Date Format Crosswalk

When a date or time pattern matches, the engine SHALL resolve the qddate format key through
the registry `qddate` crosswalk. The match SHALL report the matching registry pattern
record id together with the format key.

#### Scenario: Day-month-year dates

- **WHEN** a column contains values like `31.12.2025`
- **THEN** the match reports the datetime datatype, the qddate format key and the
  registry pattern record that maps to that key

### Requirement: Rule Loading Cache

The engine SHALL cache parsed rule files and compiled pattern expressions within a process,
keyed by file path, modification time and size (for files) and by expression text (for
patterns). A changed rule file SHALL invalidate its cached entry.

#### Scenario: Cache invalidation

- **WHEN** a rule file in the rulepath changes
- **THEN** the next load parses the file again instead of using the cached document
