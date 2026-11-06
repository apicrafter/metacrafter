## ADDED Requirements

### Requirement: Rule Pack from GitHub Releases

Metacrafter SHALL obtain its default rule pack from a GitHub release of
`apicrafter/metacrafter-rules`. The release tag and the SHA-256 of its archive SHALL be
pinned in the metacrafter package. The archive SHALL be verified before it is unpacked, and
SHALL be unpacked without writing outside the cache directory.

#### Scenario: First scan downloads the pinned release

- **WHEN** a user runs `metacrafter scan file data.csv` with an empty cache and network access
- **THEN** the pinned release archive is downloaded and its SHA-256 is verified
- **AND** the rules from its `rules/` directory are loaded

#### Scenario: Corrupted archive

- **WHEN** the downloaded archive does not match the pinned SHA-256
- **THEN** nothing is unpacked and the error names the expected and actual hashes

#### Scenario: Offline without a cached pack

- **WHEN** `rules_offline` is set and no rule pack is cached or configured
- **THEN** the scan fails with an error that explains `metacrafter rules fetch` and `rules_dir`

### Requirement: Rule Pack Source Order

The default rule pack SHALL be resolved from:
1. a configured `rules_dir`;
2. an installed `metacrafterext` package;
3. the cache;
4. a download of the configured release.

Another release SHALL be used only when both `rules_release` and `rules_sha256` are set.

#### Scenario: Local checkout

- **WHEN** `rules_dir` points to a metacrafter-rules checkout
- **THEN** its rules and validators are used and nothing is downloaded

#### Scenario: Unpinned release refused

- **WHEN** `rules_release` is set without `rules_sha256`
- **THEN** the rule pack is not downloaded and the error asks for the hash

### Requirement: Registry Access Through the Website

Metacrafter SHALL NOT depend on the `metacrafter_registry` Python package. Registry data
SHALL come from the published `/api/v1/` files, a verified cache of them, or a local
snapshot directory with the same files. Checksum validation, value identification, search
and the qddate crosswalk SHALL work without the registry package.

#### Scenario: Checksum without the registry package

- **WHEN** a rule uses `validator: checksum:iban_mod97` and `metacrafter_registry` is not installed
- **THEN** the validator works

#### Scenario: Offline snapshot

- **WHEN** a user runs `metacrafter registry fetch --to ./registry`
- **AND** later scans with `registry_path: ./registry` and `registry_offline: true`
- **THEN** matches are resolved and enriched without network access

### Requirement: Ecosystem Gate Against the Live Registry

The CI ecosystem gate SHALL use the registry website. It SHALL fail the build when the
live registry version equals the version recorded in `ecosystem.lock` and a check fails.
When the live registry has a different version, the gate SHALL report results without
failing.

#### Scenario: Registry moved on

- **WHEN** the registry publishes a new version after the baselines were recorded
- **THEN** pull request checks pass with a warning
- **AND** the weekly workflow opens or updates the drift issue
