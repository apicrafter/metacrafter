## MODIFIED Requirements

### Requirement: Rule Pack from GitHub Releases

Metacrafter SHALL obtain its default rule pack from a GitHub release of
`apicrafter/metacrafter-rules`. The release tag and the SHA-256 of its archive SHALL be
pinned in the metacrafter package. The package SHALL include a copy of the pinned release
archive. Every archive, bundled or downloaded, SHALL be verified before it is unpacked, and
SHALL be unpacked without writing outside the cache directory.

#### Scenario: First scan uses the bundled release

- **WHEN** a user runs `metacrafter scan file data.csv` with an empty cache
- **THEN** the bundled archive of the pinned release is verified against the pinned SHA-256
  and unpacked into the cache
- **AND** nothing is downloaded
- **AND** the rules from its `rules/` directory are loaded

#### Scenario: First scan downloads the pinned release

- **WHEN** a user runs `metacrafter scan file data.csv` with an empty cache and network access
- **AND** the installation has no bundled archive of the pinned release
- **THEN** the pinned release archive is downloaded and its SHA-256 is verified
- **AND** the rules from its `rules/` directory are loaded

#### Scenario: Corrupted archive

- **WHEN** the downloaded archive does not match the pinned SHA-256
- **THEN** nothing is unpacked and the error names the expected and actual hashes

#### Scenario: Corrupted bundled archive

- **WHEN** the bundled archive does not match the pinned SHA-256
- **THEN** nothing is unpacked from it, a warning names the expected and actual hashes
- **AND** the pack is downloaded unless `rules_offline` is set

#### Scenario: Offline without a cached pack

- **WHEN** `rules_offline` is set, no rule pack is cached or configured, and the configured
  release is not the bundled one
- **THEN** the scan fails with an error that explains `metacrafter rules fetch` and `rules_dir`

#### Scenario: Offline with the bundled release

- **WHEN** `rules_offline` is set and no rule pack is cached
- **AND** the configured release is the pinned release
- **THEN** the bundled archive is used and the scan completes

### Requirement: Rule Pack Source Order

The default rule pack SHALL be resolved from:
1. a configured `rules_dir`;
2. an installed `metacrafterext` package;
3. the cache;
4. the bundled archive, when the configured release and hash are the pinned ones;
5. a download of the configured release.

Another release SHALL be used only when both `rules_release` and `rules_sha256` are set.

#### Scenario: Local checkout

- **WHEN** `rules_dir` points to a metacrafter-rules checkout
- **THEN** its rules and validators are used and nothing is downloaded

#### Scenario: Unpinned release refused

- **WHEN** `rules_release` is set without `rules_sha256`
- **THEN** the rule pack is not downloaded and the error asks for the hash

#### Scenario: Forced fetch

- **WHEN** a user runs `metacrafter rules fetch --force`
- **THEN** the pinned release is downloaded from GitHub and verified, even when a cached or
  bundled copy exists
