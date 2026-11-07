# detection-engine Specification

## Purpose
How rules and registry patterns are matched against field names and values, and how confidence and evidence are computed.
## Requirements
### Requirement: Registry-Derived Detectors

The engine SHALL build data detectors from registry datatype records that have a regexp and no
data rule targeting their canonical id. The `--registry-detectors` option and the
`registry_detectors` setting SHALL accept `off`, `checksum`, `stable` and `all`, with
`checksum` as the default:
- `checksum` SHALL include only records with `status: stable` that have a checksum the
  engine can validate;
- `stable` SHALL include only records with `status: stable` that have a checksum or a
  regexp specificity of at least 0.8;
- `all` SHALL include every record with a regexp, and SHALL mark records outside the
  `stable` criteria as imprecise.

For each detector the engine SHALL compute a background broadness: the largest share of
one class of ordinary tokens that the regexp fully matches. The classes SHALL be integers
of each length from 1 to 6 digits, plain words, title-case words and short acronyms.

A detector SHALL have field-name evidence of weight 1 when a field rule for the same
datatype matched the field, or when the field name contains every token of the record id
or of one of its aliases, ignoring the record's country codes and common tokens such as
`code`, `number` and `id`. It SHALL have field-name evidence of weight 0.5 when the field
name contains every token of the record's `parent` id. A token of four or more characters
MAY match inside the field name with separators removed. The weight SHALL scale the
field-name bonus.

A detector SHALL NOT report a match when:
- its record has no checksum, its background broadness is at least 0.3 and it has no
  field-name evidence; or
- its record is country-specific, the field name does not contain the tokens of the
  record id or an alias with the country codes kept, no field rule for the datatype
  matched, and either the scan countries do not include the record's country or no scan
  country is set and the record has no checksum.

With `--explain` each skipped detector SHALL be listed with its reason.

#### Scenario: Stable detector finds an uncovered type

- **WHEN** a stable registry record with a checksum has no rule
- **AND** a column contains values that match its regexp and pass its checksum
- **AND** the scan runs with default options
- **THEN** the field reports a match with rule id `registry:<id>` and `ruletype: registry`

#### Scenario: Detector without a checksum needs the stable mode

- **WHEN** a stable registry record without a checksum has a regexp specificity of at least 0.8 and no rule
- **THEN** a scan with default options builds no detector for that record
- **AND** a scan with `--registry-detectors stable` builds one

#### Scenario: Draft records excluded by default

- **WHEN** a registry record has `status: draft` and no rule
- **AND** the scan runs with default options or with `--registry-detectors stable`
- **THEN** no detector is built for that record

#### Scenario: Detectors disabled

- **WHEN** a scan runs with `--registry-detectors off`
- **THEN** only rule-based detectors run

#### Scenario: Generic pattern on a counter column

- **WHEN** a scan runs with `--registry-detectors stable`
- **AND** a registry record's regexp accepts any number of one to four digits and the record has no checksum
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

#### Scenario: Parent name supports a generic code type

- **WHEN** a scan runs with `--registry-detectors stable`
- **AND** a registry record with a generic regexp has `parent: language`
- **AND** a column `language` contains three-letter codes
- **THEN** the column reports the record with a field-name score of 0.5

#### Scenario: Broader field name does not lift the country check

- **WHEN** a registry record `am_driver_license` has `country: [AM]` and `parent: driver_license`
- **AND** no scan country is set and the column is named `driver_license`
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

When a field has no match or only field-name matches, the engine SHALL try date and time
patterns (qddate) on its string values, unless date parsing is disabled. When a date or time
pattern matches, the engine SHALL resolve the qddate format key through the registry
`qddate` crosswalk. The match SHALL report the matching registry pattern record id together
with the format key.

#### Scenario: Day-month-year dates

- **WHEN** a column contains values like `31.12.2025`
- **THEN** the match reports the datetime datatype, the qddate format key and the
  registry pattern record that maps to that key

#### Scenario: Dates in a field with a date-like name

- **WHEN** a column `created` matches a field rule for a date datatype and no value rule
- **AND** its values are ISO dates such as `2025-05-15`
- **THEN** the field reports the field-name match and a qddate `datetime` match with value evidence

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

### Requirement: Pattern-Record Detectors

The engine SHALL build registry-derived detectors from pattern records (`kind: pattern`) that have
a regexp and a `semantic_type`, under the same `--registry-detectors` rules as datatype records,
unless a data rule targets the pattern id. A pattern detector SHALL report the canonical
`semantic_type` as the datatype and the pattern id in the match's `pattern`. The `semantic_type`
id SHALL count as field-name evidence of weight 1.

Background broadness SHALL also include a class of two-letter codes and a class of
letter-and-digit codes.

#### Scenario: Language codes

- **WHEN** a stable pattern `iso639_2` with `semantic_type: language` has a generic three-letter regexp
- **AND** a column `language` contains `eng` and `rus`
- **THEN** the column reports datatype `language` with `pattern: iso639_2`

#### Scenario: Generic pattern without the datatype name

- **WHEN** the same detector runs on a column `workspace` with three-letter words
- **THEN** the column reports no match for it

#### Scenario: Letter-and-digit codes

- **WHEN** a column contains `WGS84` and `EA1` and its name does not name `flightnumber`
- **THEN** the column reports no `flightnumber` match

### Requirement: Language Suffixes in Field Names

When a field name matches no field rule, the engine SHALL try the field name without a
trailing language code separated by `_`, `-` or `.`. Only codes from an allowlist of ISO
639-1 codes SHALL count; codes that are also common words or abbreviations, such as `id`,
`no`, `is` and `ms`, SHALL NOT.

#### Scenario: English abstract

- **WHEN** a column `abstract_en` contains text and a field rule matches `abstract`
- **THEN** the column reports that rule's datatype

#### Scenario: Language key of a nested object

- **WHEN** records hold an object `title` keyed by language (`{"title": {"ru": ..., "en": ...}}`)
  and a field rule matches `title`
- **THEN** the fields `title.ru` and `title.en` report that rule's datatype

#### Scenario: Identifier column

- **WHEN** a column is named `user_id`
- **THEN** the engine does not try the name `user`

