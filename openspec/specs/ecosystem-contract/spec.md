# ecosystem-contract Specification

## Purpose
Automated checks that keep metacrafter, the metacrafter-rules pack and metacrafter-registry compatible.
## Requirements
### Requirement: Rule Key Resolution Gate

CI SHALL fail when a loaded rule key resolves to neither a registry id nor a registry alias
in the pinned registry snapshot, unless the key is listed in the repository's file of known
unresolved keys. That file SHALL give a note for each key; entries are removed when fixed.

#### Scenario: New rule with an unknown key

- **WHEN** a rule pack update introduces a rule with key `foo_bar` that is not in the registry
- **THEN** the CI key resolution gate fails and names the rule id, file and key

### Requirement: Registry Example Rule Tests

Metacrafter SHALL provide `metacrafter rules test --registry-examples`. It SHALL run each
data rule and registry-derived detector against the registry examples of its datatype. It
SHALL report, per rule, how many positive examples it matched and how many negative
examples it accepted. CI SHALL enforce the thresholds stored in the repository.

#### Scenario: Checksum type accepts a negative example

- **WHEN** a rule for a datatype with a checksum accepts a registry example marked
  `valid: false`
- **AND** the rule is not listed as an exception
- **THEN** the rule test reports the example
- **AND** the CI gate fails

#### Scenario: Recall below threshold

- **WHEN** a rule matches fewer positive examples than its stored minimum recall
- **THEN** the CI gate fails and names the rule and the missed examples

### Requirement: Ecosystem Lock

The repository SHALL contain `ecosystem.lock`. It SHALL pin the registry version with its
manifest hash and the `metacrafter-rules` version. CI and release builds SHALL use these
pinned versions.

#### Scenario: Reproducible CI

- **WHEN** CI runs twice on the same commit
- **THEN** both runs use the same registry snapshot and rule pack

### Requirement: Scheduled Compatibility Check

A scheduled workflow SHALL run the key resolution gate and the example rule tests against
the current HEAD of `metacrafter-rules` and `metacrafter-registry`. On failure, it SHALL
open or update a single tracking issue.

#### Scenario: Registry renames a datatype

- **WHEN** the registry HEAD removes an id without adding an alias
- **AND** a rule still uses that id
- **THEN** the weekly run fails and updates the tracking issue with the affected rules

### Requirement: Detection Benchmark

The repository SHALL contain synthetic labelled datasets. `metacrafter evaluate` SHALL report
per-datatype precision, recall and scan time, and SHALL compare them with a stored baseline
(`--baseline`). CI SHALL fail on a regression beyond the stored tolerance.

#### Scenario: Precision regression

- **WHEN** a change lowers precision for a benchmark datatype beyond the tolerance
- **THEN** the benchmark gate fails and lists the datatype with old and new values

