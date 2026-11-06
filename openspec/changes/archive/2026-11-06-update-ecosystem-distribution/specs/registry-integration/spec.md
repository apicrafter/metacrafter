## MODIFIED Requirements

### Requirement: Registry Source Resolution

Metacrafter SHALL load registry data through one adapter, using only the files the registry
website publishes under `/api/v1/`. The adapter SHALL try sources in this order:
1. a configured local snapshot directory;
2. a verified cache that is younger than the configured time to live;
3. a download from the configured base URL;
4. a verified older cache.

Fetched data SHALL be verified against the SHA-256 values in the registry manifest before
use.

#### Scenario: Offline scan with a local snapshot

- **WHEN** `registry_path` points to a registry snapshot
- **AND** `registry_offline: true` is set
- **THEN** the scan completes without network access
- **AND** the report states the snapshot as the registry source

#### Scenario: Corrupted download is rejected

- **WHEN** a fetched bulk file does not match its manifest SHA-256
- **THEN** the file is not used
- **AND** the adapter falls back to the next source
- **AND** a warning names the file and the expected hash

#### Scenario: Registry unavailable

- **WHEN** no registry source can be loaded
- **THEN** the scan still completes with rule results
- **AND** the report states `registry.source` as `unavailable`
