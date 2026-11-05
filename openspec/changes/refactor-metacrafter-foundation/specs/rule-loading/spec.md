## ADDED Requirements

### Requirement: Package-Resolved Default Rule Pack

Metacrafter SHALL resolve its default rule pack from the installed `metacrafter-rules`
package through `metacrafterext.rules.rules_dir()`. Metacrafter SHALL NOT resolve default
rules relative to the current working directory.

#### Scenario: Scan from an unrelated directory

- **WHEN** a user runs `metacrafter scan file data.csv` from a directory that contains no
  `rules/` folder and no `.metacrafter` file
- **THEN** rules from the `metacrafter-rules` pack are loaded
- **AND** no duplicate rule key warning is logged

#### Scenario: Local rules folder is not picked up implicitly

- **WHEN** the current working directory contains a `rules/` folder
- **AND** neither `.metacrafter` nor `--rulepath` references it
- **THEN** rules from that folder are not loaded

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
