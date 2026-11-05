## ADDED Requirements

### Requirement: Configured optional enrichment

Registry resolution SHALL use explicit configuration then environment then defaults consistently, cache bounded responses, validate HTTP status and response shape, and preserve standalone scanning when unavailable.

#### Scenario: Custom registry URL

- **WHEN** a user configures a registry URL and scans without network access
- **THEN** result links use the configured base URL and classification succeeds

### Requirement: Documented release behavior

User documentation SHALL describe actual installed rule resources, report versions, API options, sampling and optional integrations without claiming unverified external compatibility.

#### Scenario: Minimal installation

- **WHEN** a user follows the core installation instructions
- **THEN** the documented rule scan works without installing catalog or LLM extras
