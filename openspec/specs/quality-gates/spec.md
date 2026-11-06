# quality-gates Specification

## Purpose
Hermetic tests and the CI gates that every change must pass.
## Requirements
### Requirement: Independent deterministic tests

Tests SHALL isolate home configuration and external services, collect registry tests, and explicitly skip unavailable optional integrations without swallowing assertion failures.

#### Scenario: Developer home configuration

- **WHEN** the test runner executes on a machine with a custom .metacrafter file
- **THEN** test behavior is independent of that file

### Requirement: Enforced supported builds

CI SHALL enforce critical lint, focused formatting and type checks, a measured coverage floor, and core wheel installation checks on supported Python versions.

#### Scenario: Wheel omits defaults

- **WHEN** a built wheel lacks rule resources
- **THEN** the installation smoke gate fails

