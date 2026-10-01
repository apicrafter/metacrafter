## ADDED Requirements

### Requirement: Consistent named field statistics

The SDK, HTTP API and serialized reports SHALL map each Analyzer statistic to the same named field and SHALL preserve numeric ranges, character flags and dictionary values without positional mislabeling.

#### Scenario: Numeric range survives serialization
- **WHEN** a synthetic amount field contains numeric values 1 and 2
- **THEN** its named statistics report minval 1 and maxval 2 in SDK, API and serialized output
- **AND** dictvalues contains only the documented dictionary-value representation, never the numeric minimum by positional error

### Requirement: Complete documented field coverage

Scanning SHALL preserve one-character and numeric field names and SHALL handle scalar arrays and nested paths according to a documented shared traversal contract. Missing selected fields and path collisions SHALL produce explicit validation outcomes rather than silent omissions or internal errors.

#### Scenario: Short field name is retained
- **WHEN** a record contains the valid field name x
- **THEN** the full report includes x and its statistics even when no semantic rule matches

#### Scenario: Missing selected field is diagnosed
- **WHEN** a scan explicitly selects a field absent from the input
- **THEN** the interface returns or raises its documented validation outcome
- **AND** an internal KeyError is not exposed as the scan result

### Requirement: Inclusive bounded confidence thresholds

Public confidence options SHALL accept only finite values in the documented range. A match at the configured minimum SHALL satisfy that threshold. Empty samples and invalid limits SHALL not cause division errors.

#### Scenario: Perfect match at the maximum threshold
- **WHEN** all eligible sampled values match a value rule and the minimum confidence is 100 percent
- **THEN** the rule match is retained with confidence 100 percent

#### Scenario: Empty denominator is handled
- **WHEN** no eligible values remain after sample and empty-value policies
- **THEN** classification produces the documented empty/unknown outcome without division by zero

### Requirement: Preserved explicit PII metadata

Rule loading and report conversion SHALL normalize boolean PII metadata and SHALL preserve authoritative PII flags across classification and serialization. Required validators or field guards that cannot load SHALL not silently increase a rule's accepted scope.

#### Scenario: YAML boolean PII is preserved
- **WHEN** a rule in a non-PII context declares is_pii as YAML boolean true and matches
- **THEN** its serialized match is marked as PII

#### Scenario: Required validator is unavailable
- **WHEN** a rule references a required validator that cannot be imported
- **THEN** strict loading reports a failure or disables the affected rule
- **AND** the rule is not silently treated as fully validated

### Requirement: Versioned report interchange

Serialized reports SHALL declare a versioned interchange contract that exporters can consume directly. Readers SHALL accept documented legacy data/fields forms or reject incompatible input explicitly; they SHALL not report an incompatible populated report as an empty successful export.

#### Scenario: Saved scan report is exportable
- **WHEN** SDK field results are saved using the supported JSON writer and read by a catalog exporter
- **THEN** the exporter sees the same field identities, matches, confidence and PII metadata

### Requirement: Parseable source-qualified bulk reports

Bulk writers SHALL produce one valid JSON value, a valid YAML document, or a single-header CSV with source identity. Explicit JSONL mode MAY emit separate line-delimited records. Failed and skipped sources SHALL remain visible in aggregate outcomes.

#### Scenario: Multiple sources produce valid JSON
- **WHEN** two sources are scanned with JSON aggregate output
- **THEN** the complete output parses with a standard JSON parser
- **AND** both source identities and their outcomes are represented
