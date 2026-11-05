## ADDED Requirements

### Requirement: Packaged defaults

The package SHALL include built-in YAML rules in wheels and source distributions, load them independently of the current directory, and preserve explicit custom-only rule loading.

#### Scenario: Installed outside a checkout

- **WHEN** a minimal installation scans a file from another directory
- **THEN** built-in classifications are available without optional rules or LLM packages

### Requirement: Deterministic validated rules

The loader SHALL validate rule structure, normalize boolean PII metadata, apply deterministic priority and source precedence, expose invalid-rule diagnostics, and disable rules whose required validator or field guard cannot compile.

#### Scenario: Required validator unavailable

- **WHEN** a rule references an unavailable required validator
- **THEN** the rule is disabled with a diagnostic and never becomes an unchecked pattern

### Requirement: Restricted pattern expressions

Pattern compilation SHALL accept only an allowlist of PyParsing constructors, constants and parser methods through validated syntax; Python function plugins SHALL remain explicitly trusted executable extensions.

#### Scenario: Literal contains reserved text

- **WHEN** a valid pattern includes Literal("open")
- **THEN** the pattern compiles while import and arbitrary attribute access are rejected
