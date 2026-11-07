## MODIFIED Requirements

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
