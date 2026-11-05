## ADDED Requirements

### Requirement: Registry-Enriched Report v2

Scan reports SHALL follow the report v2 model:
- **Report-level fields:** `report_format: 2`, the Metacrafter version, rule pack version,
  registry provenance and scoring model version.
- **Per source:** a list of fields.
- **Per field:** statistics, matches and a PII summary.
- **Per match:** canonical `datatype`, `rule_key`, `rule_id`, `ruletype`, `confidence`,
  `evidence`, `is_pii`, `pii_source` and a `registry` reference with name, categories,
  status, URL and mappings.

#### Scenario: Report from a file scan

- **WHEN** a user runs `metacrafter scan file users.csv --format json`
- **THEN** the output has `report_format: 2`
- **AND** each match carries `datatype`, `confidence`, `evidence` and `registry.url`

#### Scenario: PII summary per field

- **WHEN** a field has at least one match with `is_pii: true`
- **THEN** the field's PII summary is true and lists the PII datatypes found

### Requirement: Published Report Schema

The JSON Schema of report v2 SHALL be generated from the report models and published with
the documentation. Every JSON, JSON Lines and YAML report produced by the CLI, SDK and HTTP
API SHALL validate against it.

#### Scenario: Schema validation in CI

- **WHEN** the test suite produces reports for the file, SQL and bulk fixtures
- **THEN** every report validates against the published schema

### Requirement: Legacy Report Format

Metacrafter SHALL provide `--format legacy`, producing the pre-1.0 report structure from a
v2 report, for at least two minor releases after 1.0.0.

#### Scenario: Legacy consumer

- **WHEN** a user runs a scan with `--format legacy`
- **THEN** the output keeps the pre-1.0 keys, including `datatype_url`

### Requirement: Markdown Report

Metacrafter SHALL provide `--format md`. It SHALL produce a Markdown report with one table
per source listing field, datatype, confidence, PII flag and registry link, followed by a
PII summary. Raw sample values SHALL NOT appear unless `--include-samples` is set.

#### Scenario: Markdown without samples

- **WHEN** a user runs `metacrafter scan file users.csv --format md`
- **THEN** the output contains a field table with registry links
- **AND** contains no values from the scanned file
