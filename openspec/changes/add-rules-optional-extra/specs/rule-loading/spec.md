## ADDED Requirements

### Requirement: Extended Rules Directory API

The `metacrafter-rules` package SHALL expose a stable function
`metacrafterext.rules.rules_dir()` returning the absolute path to its bundled `rules/`
directory.

#### Scenario: Portable path resolution

- **WHEN** metacrafter imports `metacrafterext.rules.rules_dir`
- **THEN** the returned path points to the packaged rules tree regardless of install mode
  (editable, wheel, or sdist)

### Requirement: Automatic Extended Rulepath

When `metacrafter-rules` is installed and auto-discovery is enabled, Metacrafter SHALL
append the extended rules directory to the effective rulepath without manual configuration.

#### Scenario: Auto-append on install

- **WHEN** `metacrafter-rules` is importable
- **AND** `auto_rules` is true (default)
- **AND** the user has not set `auto_rules: false` in `.metacrafter`
- **THEN** `ConfigLoader.get_rulepath()` includes the built-in `rules/` path first
- **AND** includes the path from `metacrafterext.rules.rules_dir()` second
- **AND** a scan loads rules from both directories

#### Scenario: Auto-append disabled

- **WHEN** `.metacrafter` contains `auto_rules: false`
- **THEN** the extended rules directory is not auto-appended
- **AND** only explicitly configured `rulepath` entries are used (plus built-in `rules/` if listed)

#### Scenario: Extended package not installed

- **WHEN** `metacrafter-rules` is not installed
- **THEN** auto-discovery is a no-op
- **AND** the effective rulepath equals built-in defaults or user-configured paths only

#### Scenario: User rulepath composes with auto-discovery

- **WHEN** a user passes `--rulepath ./custom_rules`
- **THEN** custom paths are loaded in addition to built-in and auto-discovered paths
- **AND** duplicate rule `key` values follow existing skip-with-warning behavior
