# rule-loading Specification

## Purpose
How rule files are found, ordered, validated and compiled.
## Requirements
### Requirement: Extended Rules Directory API

The `metacrafter-rules` package SHALL expose a stable function
`metacrafterext.rules.rules_dir()` returning the absolute path to its bundled `rules/`
directory.

#### Scenario: Portable path resolution

- **WHEN** metacrafter imports `metacrafterext.rules.rules_dir`
- **THEN** the returned path points to the packaged rules tree regardless of install mode
  (editable, wheel, or sdist)

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

### Requirement: Deterministic Rulepath Order

The effective rulepath SHALL be composed in this order: the default pack (unless
`auto_rules: false`), then paths from `.metacrafter` `rulepath`, then CLI `--rulepath`
values. Later entries SHALL override earlier ones for the same rule id.

#### Scenario: User rule overrides the default pack

- **WHEN** a user rulepath defines a rule with the same id as a rule in the default pack
- **THEN** the user rule is used
- **AND** the override is reported once at INFO level with both file paths

#### Scenario: Default pack disabled

- **WHEN** `.metacrafter` sets `auto_rules: false` and lists a `rulepath`
- **THEN** only rules from the listed paths are loaded

#### Scenario: No rules resolved

- **WHEN** `auto_rules: false` is set and no `rulepath` is configured
- **THEN** the scan fails with an error stating that no rule directories are configured

