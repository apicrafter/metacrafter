## ADDED Requirements

### Requirement: Registry-Derived Detectors

The engine SHALL build data detectors from registry records that have a regexp and no rule
targeting them. The `--registry-detectors` option SHALL accept `off`, `stable` and `all`,
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
- value hit rate weighted by pattern specificity;
- checksum pass rate;
- field-name evidence;
- rule priority;
- country context;
- a penalty for draft records.

Each match SHALL include an `evidence` object with these components, the sampled and matched
counts, and the scoring model version. Threshold comparisons SHALL be inclusive.

#### Scenario: Checksum raises confidence

- **WHEN** two fields both match an IBAN regexp in all values
- **AND** only the first field's values pass the checksum
- **THEN** the first field's IBAN score is higher than the second's
- **AND** both matches show `checksum_rate` in `evidence`

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

### Requirement: Compiled Ruleset Cache

The engine SHALL cache compiled rulesets keyed by a hash of rule file contents, the
registry version and the detector mode. A changed rule file or registry version SHALL
invalidate the cache.

#### Scenario: Cache invalidation

- **WHEN** a rule file in the rulepath changes
- **THEN** the next scan recompiles the ruleset instead of using the cached one
