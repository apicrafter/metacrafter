## ADDED Requirements

### Requirement: Report comparison

The CLI SHALL compare legacy or versioned reports deterministically, report field/datatype/PII and numeric statistics changes, flag incompatible provenance, and support a configurable failure exit code.

#### Scenario: Datatype added

- **WHEN** a second report adds a datatype match
- **THEN** report diff emits the addition in JSON and can fail the pipeline

### Requirement: PII inventory

The CLI SHALL consolidate explicit PII matches across sources with source identity, confidence, evidence, severity and aggregate completeness; sampled counts SHALL not be presented as population counts.

#### Scenario: Custom PII type

- **WHEN** a report contains a custom datatype marked is_pii
- **THEN** the inventory includes that datatype and its source

### Requirement: Lint tests explanations and drafts

The CLI SHALL lint rule structure and imports, test supplied positive and negative examples, explain accepted rule evidence without raw samples by default, and generate review-required rule drafts that do not invent validators.

#### Scenario: Draft from leading-zero samples

- **WHEN** the user drafts a rule from string identifiers beginning with zero
- **THEN** the draft preserves strings and remains disabled pending review

### Requirement: Report policy checks

The CLI SHALL evaluate saved reports against prohibited PII, required labels, confidence and completeness policies, emit machine-readable violations, and use documented exit codes without rescanning.

#### Scenario: Incomplete report prohibited

- **WHEN** a completeness policy is evaluated against a partial scan
- **THEN** check reports the completeness violation and exits nonzero

### Requirement: Reproducible evaluation

The CLI SHALL evaluate a labeled fixture with per-datatype precision, recall, false positives, false negatives, abstentions and timing while identifying classification mode and rule version.

#### Scenario: False positive

- **WHEN** a fixture labels a predicted datatype as incorrect
- **THEN** evaluation counts a false positive without treating confidence as calibrated probability

### Requirement: dbt artifact adapter

An optional offline adapter SHALL validate dbt manifest metadata, map declared column types to report fields, and produce a dry-run plan preserving existing metadata.

#### Scenario: Existing column metadata

- **WHEN** a dbt manifest column already contains metadata
- **THEN** the generated plan retains it and proposes additive classification metadata
