# detection-engine Specification

## Purpose
How rules and registry patterns are matched against field names and values, and how confidence and evidence are computed.
## Requirements
### Requirement: Registry-Derived Detectors

The engine SHALL build data detectors from registry datatype records that have a regexp and no
data rule targeting their canonical id. The `--registry-detectors` option SHALL accept `off`, `stable` and `all`,
with `stable` as the default:
- `stable` SHALL include only records with `status: stable` that have a checksum or a
  regexp specificity of at least 0.8;
- `all` SHALL include every record with a regexp, and SHALL mark records outside the
  `stable` criteria as imprecise.

For each detector the engine SHALL compute a background broadness: the largest share of
one class of ordinary tokens that the regexp fully matches. The classes SHALL be integers
of each length from 1 to 6 digits, plain words, title-case words and short acronyms.

A detector SHALL have field-name evidence when a field rule for the same datatype matched
the field, or when the field name contains every token of the record id or of one of its
aliases, ignoring the record's country codes and common tokens such as `code`, `number`
and `id`. A token of four or more characters MAY match inside the field name with
separators removed.

Without field-name evidence a detector SHALL NOT report a match when:
- its record has no checksum and its background broadness is at least 0.3; or
- its record is country-specific and the scan countries do not include the record's
  country; or
- its record is country-specific, no scan country is set and the record has no checksum.

With `--explain` each skipped detector SHALL be listed with its reason.

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

#### Scenario: Generic pattern on a counter column

- **WHEN** a registry record's regexp accepts any number of one to four digits and the record has no checksum
- **AND** a column `layers_count` contains small integers
- **THEN** the column reports no match for that record
- **AND** with `--explain` the rejected candidates give the reason "Generic pattern without field-name evidence"

#### Scenario: Generic pattern with a matching field name

- **WHEN** the same detector runs on a column `flight_number`
- **THEN** the column reports the match with a field-name score of 1

#### Scenario: Country-specific type without a scan country

- **WHEN** a registry record is specific to one country and has no checksum
- **AND** no scan country is set and the field name does not name the type
- **THEN** the column reports no match for that record

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

### Requirement: Country-Specific Duplicates

When no scan country is set, the engine SHALL drop:
- a field-name match from a rule file with a country other than `xx` when a rule file
  without a country (or with `xx`) matched the field with at least the same priority
  weight, or any rule matched the field with a higher priority weight;
- a value match from a country-specific rule that has no validator, is not a `func` rule
  and has no field-name evidence, when a rule without a country matched the same field
  with at least the same confidence.

With `--explain` each dropped match SHALL be listed with its reason.

#### Scenario: City field

- **WHEN** a field `city` matches a universal `city` rule with priority 1 and an Egyptian `egcity` rule with priority 3
- **AND** no scan country is set
- **THEN** the field reports `city` only

#### Scenario: GUID column

- **WHEN** a column of GUIDs matches a universal `uuid` value rule and a Russian `rufias` value rule with the same pattern
- **AND** no scan country is set and the field name does not name `rufias`
- **THEN** the column reports `uuid` only

#### Scenario: Scan country set

- **WHEN** the scan runs with `--country-codes ru`
- **THEN** matches from Russian rules are not dropped as country-specific duplicates

