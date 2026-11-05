## ADDED Requirements

### Requirement: Normal imports and shared service

The library SHALL expose CrafterCmd through normal Python imports, retain existing public import paths, share statistics and report helpers, and avoid replacing application logging handlers.

#### Scenario: SDK import compatibility

- **WHEN** an application imports CrafterCmd from metacrafter and metacrafter.core
- **THEN** both names resolve to the same class without dynamic module execution

### Requirement: HTTP scan parity

The HTTP service SHALL initialize its scanning dependency once, validate record shape and scan options, honor confidence, field selection and statistics options, enforce bounded payloads, and expose health and readiness endpoints.

#### Scenario: Invalid request

- **WHEN** the client sends a scalar payload or invalid limit
- **THEN** the server returns a structured 400 response without running classification

### Requirement: Operational authentication

The service SHALL support optional bearer authentication from server configuration and sanitized error responses.

#### Scenario: Authentication enabled

- **WHEN** a scan request omits the configured bearer token
- **THEN** the service rejects the request while health remains available
