## ADDED Requirements

### Requirement: Rules Optional Extra

The `metacrafter` package SHALL declare an optional dependency group `rules` installable
via `pip install 'metacrafter[rules]'` that pulls in the `metacrafter-rules` package.

#### Scenario: Full ruleset install

- **WHEN** a user runs `pip install 'metacrafter[rules]'`
- **THEN** both `metacrafter` and `metacrafter-rules` are installed
- **AND** `import metacrafterext.rules` succeeds

#### Scenario: Minimal install unchanged

- **WHEN** a user runs `pip install metacrafter` without extras
- **THEN** `metacrafter-rules` is not installed
- **AND** rule-based scanning still works with built-in rules only

#### Scenario: All extra includes rules

- **WHEN** a user runs `pip install 'metacrafter[all]'`
- **THEN** `metacrafter-rules` is installed as a dependency

### Requirement: Extended Rules Wheel Contents

The `metacrafter-rules` package SHALL ship its `rules/` YAML directory inside the
installable wheel so `rules_dir()` resolves to a directory containing rule files
without a git checkout.

#### Scenario: Rules available after pip install

- **WHEN** `metacrafter-rules` is installed from PyPI or a wheel
- **THEN** `metacrafterext.rules.rules_dir()` returns a path that exists on disk
- **AND** that directory contains at least one `.yaml` rule file
