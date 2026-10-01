## ADDED Requirements

### Requirement: Authoritative PII export

Catalog exporters SHALL honor explicit PII metadata on included matches after confidence filtering, including datatypes outside built-in name heuristics. Existing catalog metadata SHALL be preserved according to the documented merge policy.

#### Scenario: Custom PII datatype is tagged
- **WHEN** an included national_id match has is_pii true
- **THEN** the exporter plans the configured PII tag or classification even though the datatype is outside its legacy hard-coded list

### Requirement: Confirmed operation outcomes

Catalog exporters SHALL distinguish attempted, succeeded, skipped and failed operations. Success counters SHALL increase only for confirmed successful writes; failures SHALL appear in structured operation errors and aggregate completeness/status.

#### Scenario: Remote write fails
- **WHEN** a classification write returns HTTP 500 or the client raises a transport error
- **THEN** confirmed-success counts do not increase for that operation
- **AND** the failure identifies the source/field and operation without exposing secrets
