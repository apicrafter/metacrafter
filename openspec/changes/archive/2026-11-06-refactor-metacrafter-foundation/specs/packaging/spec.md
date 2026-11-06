## ADDED Requirements

### Requirement: Supported Python Runtime

The `metacrafter` package SHALL declare `requires-python = ">=3.10"` and SHALL be tested
in CI on every supported CPython minor version from 3.10 to the latest stable release.

#### Scenario: Install on supported Python

- **WHEN** a user runs `pip install metacrafter` on Python 3.10 or newer
- **THEN** the installation succeeds
- **AND** `metacrafter --help` exits with code 0

#### Scenario: Install on unsupported Python

- **WHEN** a user runs `pip install metacrafter` on Python 3.9
- **THEN** pip does not select a 0.1.0 or later release

### Requirement: Minimal Core Dependencies

The core installation SHALL include only the packages needed to scan files and SQL
databases with rule-based classification. MongoDB, DuckDB-native, HTTP server, MCP, LLM
and catalog dependencies SHALL be provided through named extras.

#### Scenario: Core install scans a file

- **WHEN** a user installs `metacrafter` without extras
- **AND** runs `metacrafter scan file users.csv`
- **THEN** the scan completes using the default rule pack

#### Scenario: Missing extra is reported

- **WHEN** a user without the `mongodb` extra runs `metacrafter scan mongodb ...`
- **THEN** the command exits with a non-zero code
- **AND** the error message names the missing extra and the install command
  `pip install 'metacrafter[mongodb]'`

### Requirement: Required Rules Package

The `metacrafter` package SHALL depend on `metacrafter-rules` as a required dependency
and SHALL NOT ship its own copy of rule YAML files.

#### Scenario: Rules available after core install

- **WHEN** a user installs `metacrafter` without extras
- **THEN** `metacrafter-rules` is installed
- **AND** `import metacrafterext.rules` succeeds

#### Scenario: Compatibility extra

- **WHEN** a user runs `pip install 'metacrafter[rules]'`
- **THEN** the installation succeeds without installing additional packages
