## MODIFIED Requirements

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

#### Scenario: Parent name supports a generic code type

- **WHEN** a registry record with a generic regexp has `parent: language`
- **AND** a column `language` contains three-letter codes
- **THEN** the column reports the record with a field-name score of 0.5

#### Scenario: Broader field name does not lift the country check

- **WHEN** a registry record `am_driver_license` has `country: [AM]` and `parent: driver_license`
- **AND** no scan country is set and the column is named `driver_license`
- **THEN** the column reports no match for that record

## ADDED Requirements

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
