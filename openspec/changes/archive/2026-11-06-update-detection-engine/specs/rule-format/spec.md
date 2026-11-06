## ADDED Requirements

### Requirement: Rule Format Schema v2

Metacrafter SHALL publish a JSON Schema for rule files (format version 2) and SHALL
validate every rule file against it at load time. Files without `format_version` SHALL be
treated as version 1 and remain accepted.

#### Scenario: Invalid rule reported

- **WHEN** a rule file sets `is_pii: "yes"` (a string)
- **THEN** loading reports a schema error with the file, rule id and field
- **AND** that rule is not loaded

#### Scenario: Validate command

- **WHEN** a user runs `metacrafter rules validate ./myrules`
- **THEN** each schema violation is listed
- **AND** the command exits non-zero if any violation exists

### Requirement: Regex Match Type

Rules with `match: regex` SHALL match a value when the Python pattern in `rule` fully
matches it. Values longer than the rule `maxlen`, or 256 characters when unset, SHALL NOT
be matched.

#### Scenario: Regex data rule

- **WHEN** a data rule has `match: regex` and `rule: '[A-Z]{2}\d{6}'`
- **AND** a column contains `AB123456`
- **THEN** the value counts as a hit

### Requirement: Registry Match Type

Rules with `match: registry` SHALL take the pattern and checksum from the registry record
named in `rule`. They SHALL use that record's canonical id as the datatype. Constraints
from the rule (`fieldrule`, length bounds, priority, imprecise, country) SHALL be applied
in addition.

#### Scenario: IBAN via registry

- **WHEN** a rule has `match: registry` and `rule: iban`
- **AND** a column contains `GB82WEST12345698765432` and `GB82WEST12345698765433`
- **THEN** the first value counts as a hit
- **AND** the second value fails the `iban_mod97` checksum and does not count

#### Scenario: Mismatched key

- **WHEN** a `match: registry` rule sets `key` to a value different from the resolved id of
  `rule`
- **THEN** loading reports a schema error for that rule

### Requirement: Checksum Validators

A rule `validator` of the form `checksum:<algorithm>` SHALL validate values with the
registry implementation of that algorithm. An unknown algorithm SHALL be a load error.

#### Scenario: Luhn validator

- **WHEN** a rule sets `validator: checksum:luhn`
- **THEN** only values that pass the Luhn check count as hits

### Requirement: Honoured Rule Attributes

The engine SHALL honour these rule attributes:
- `priority` in scoring;
- `deprecated` by skipping the rule unless explicitly included;
- `validator` on every data match type;
- `fieldrule` on field rules;
- `minlen`/`maxlen` on `text` rules.

#### Scenario: Deprecated rule skipped

- **WHEN** a rule sets `deprecated: true`
- **THEN** it does not run during a default scan
- **AND** it runs when `--include-deprecated` is passed

#### Scenario: Validator on a text rule

- **WHEN** a `text` data rule sets a `validator`
- **THEN** a value counts as a hit only if it is in the keyword set and passes the validator
