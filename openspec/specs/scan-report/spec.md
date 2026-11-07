# scan-report Specification

## Purpose
The report produced by every scan (version 3), its schema and its legacy and Markdown renderings.
## Requirements
### Requirement: Registry-Enriched Report Version 3

Scan reports SHALL follow the report version 3 model:
- **Report-level fields:** `report_version: 3` (a superset of the version 2 interchange
  format from `improve-scan-result-integrity`), the Metacrafter version, rule pack version,
  registry provenance and scoring model version.
- **Per source:** a list of fields.
- **Per field:** statistics, matches and a PII summary.
- **Per match:** canonical `datatype`, `rule_key`, `rule_id`, `ruletype`, `confidence`,
  `evidence`, `is_pii`, `pii_source` and a `registry` reference with name, categories,
  status, URL and mappings.

A PII value match (`ruletype` `data` or `registry`) without field-name evidence, whose datatype
has no match with field-name evidence on the field, SHALL be reported with `is_pii: false`,
`pii_source: null` and `pii_outranked_by` set to the outranking datatype when another value match
on the same field has field-name evidence, is not PII, is a different datatype that is not its
registry parent or child, and has the same or a higher confidence. The match SHALL stay in the
field's matches, and `registry.is_pii` SHALL keep the datatype's flag. Registry filters
(`--pii-only`) SHALL see the demoted flag.

#### Scenario: Report from a file scan

- **WHEN** a user runs `metacrafter scan file users.csv --format json`
- **THEN** the output has `report_version: 3`
- **AND** each match carries `datatype`, `confidence`, `evidence` and `registry.url`

#### Scenario: PII summary per field

- **WHEN** a field has at least one match with `is_pii: true`
- **THEN** the field's PII summary is true and lists the PII datatypes found

#### Scenario: Value-only PII match outranked by a named datatype

- **WHEN** a field `okpd2` holds OKPD2 codes such as `21.20.10.226`, and `okpd` matches them with
  field-name evidence at confidence 100 while `ipv4` matches them by value only at 100
- **THEN** `okpd` is the first match and `ipv4` follows with `is_pii: false` and
  `pii_outranked_by: okpd`
- **AND** the field's PII summary is false

#### Scenario: Field-name-only match keeps value PII

- **WHEN** a field `login` holds email addresses and `login` matches only by field name
- **THEN** the `email` match keeps `is_pii: true` and the field's PII summary is true

### Requirement: Published Report Schema

The JSON Schema of report version 3 SHALL be generated from the report models and published with
the documentation. Every JSON, JSON Lines and YAML report produced by the CLI, SDK and HTTP
API SHALL validate against it.

#### Scenario: Schema validation in CI

- **WHEN** the test suite produces reports for the file, SQL and bulk fixtures
- **THEN** every report validates against the published schema

### Requirement: Legacy Report Format

Metacrafter SHALL provide `--output-format legacy`, producing the pre-1.0 report structure from a
version 3 report, for at least two minor releases after 1.0.0.

#### Scenario: Legacy consumer

- **WHEN** a user runs a scan with `--output-format legacy`
- **THEN** the output keeps the pre-1.0 keys, including `datatype_url`

### Requirement: Markdown Report

Metacrafter SHALL provide `--output-format md`. It SHALL produce a Markdown report with one table
per source listing field, datatype, confidence, PII flag and registry link, followed by a
PII summary. Raw sample values SHALL NOT appear unless `--include-samples` is set.

#### Scenario: Markdown without samples

- **WHEN** a user runs `metacrafter scan file users.csv --output-format md`
- **THEN** the output contains a field table with registry links
- **AND** contains no values from the scanned file

### Requirement: Field Groups

Each single-source scan report SHALL include `field_groups`: groups of sibling fields (the same
parent path) whose names, split on case changes and separators, share structure. A `prefix` group
SHALL have at least two fields starting with the same word of three or more letters that is not a
flag word, at least one of them longer than the word, and SHALL list the remaining part of each
name. A `suffix` group SHALL have at least two multi-word fields ending with the same word. A
`language` group SHALL hold a field and its language-suffix variants; prefix and suffix groups SHALL
compare the names without language suffixes.

#### Scenario: Object encoded in names

- **WHEN** a table has fields `place`, `placeURL`, `placeLocation` and `placeType`
- **THEN** `field_groups` has a prefix group `place` with parts `""`, `location`, `type`, `url`

#### Scenario: Language variants

- **WHEN** a table has fields `title`, `title_en` and `title_ru`
- **THEN** `field_groups` has a language group `title` with parts `""`, `en`, `ru`

