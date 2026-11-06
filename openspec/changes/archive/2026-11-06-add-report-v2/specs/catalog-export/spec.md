## ADDED Requirements

### Requirement: Registry-Driven Catalog Labels

Catalog exporters (DataHub, OpenMetadata, Atlas) SHALL derive PII classification, datatype
labels and glossary terms only from the report's match data and registry references. They
SHALL NOT contain lists of datatype ids. Each exported datatype label SHALL carry the
registry URL.

#### Scenario: PII type unknown to the old list

- **WHEN** a report contains a match for `ru_snils` with `is_pii: true`
- **AND** the user exports the report to DataHub
- **THEN** the field receives the PII tag
- **AND** the datatype glossary term links to the `ru_snils` registry URL

### Requirement: Crosswalk Label Export

Metacrafter SHALL provide `metacrafter export labels --system <system>`, where system is one
of `presidio`, `google_dlp`, `dpv` or `purview`. The command SHALL map each match to the
target system's label using the registry `mappings`. It SHALL list matches without a
mapping separately.

#### Scenario: Presidio entities

- **WHEN** a report contains matches for `email` and `iban`
- **AND** the user runs `metacrafter export labels report.json --system presidio`
- **THEN** the output maps the fields to `EMAIL_ADDRESS` and `IBAN_CODE`

#### Scenario: No mapping

- **WHEN** a matched datatype has no mapping for the requested system
- **THEN** the field is listed under unmapped matches with its datatype id

### Requirement: Frictionless Schema Export

Metacrafter SHALL provide `metacrafter export frictionless`. It SHALL write a Frictionless
Table Schema per source. Each field SHALL be annotated with `rdfType` from the datatype's
Schema.org or Wikidata mapping, when present, and with the registry URL.

#### Scenario: Annotated schema

- **WHEN** a report contains an `email` match on field `contact`
- **THEN** the exported schema field `contact` has an `rdfType` and a registry URL annotation
