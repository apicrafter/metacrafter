## ADDED Requirements

### Requirement: Validated transparent classification

LLM results SHALL use registered datatype IDs, finite confidence in range, configured confidence and selected nested fields; reports SHALL record requested and effective modes and explicit fallback policy.

#### Scenario: Unknown model label

- **WHEN** a provider returns an unknown datatype or non-finite confidence
- **THEN** the result is rejected with a diagnostic rather than emitted as a classification

### Requirement: Sample privacy and secrets

LLM classification SHALL offer names-only, redacted and explicitly selected raw sample policies, bounded sample counts and provider timeouts; logs SHALL redact credentials and API keys.

#### Scenario: Names-only policy

- **WHEN** a cloud provider classifies a field under the default names-only policy
- **THEN** no raw values are sent to embeddings or generation

### Requirement: Local embeddings and verified indexes

Retrieval SHALL support a local embedding provider, fingerprint registry and embedding configuration, reject incompatible indexes, and rebuild through a replacement collection without zero-vector fallbacks.

#### Scenario: Embedding failure during rebuild

- **WHEN** an embedding request fails while replacing an existing index
- **THEN** the usable index remains intact and the failure is reported

### Requirement: Complete filter metadata

Retrieval SHALL retain datatype PII metadata and match all supplied language, category and country filter values.

#### Scenario: Multiple language filters

- **WHEN** a candidate matches the second requested language
- **THEN** the candidate is eligible and keeps its PII metadata
