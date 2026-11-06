## MODIFIED Requirements

### Requirement: Registry Source Resolution

Metacrafter SHALL load registry data through one adapter, using only the files the registry
website publishes under `/api/v1/`. The adapter SHALL try sources in this order:
1. a configured local snapshot directory;
2. a verified cache that is younger than the configured time to live;
3. when no verified cache exists and the base URL is the default
   `https://registry.apicrafter.io`, the snapshot bundled in the package, copied into the
   cache;
4. a download from the configured base URL;
5. a verified older cache.

Fetched and bundled data SHALL be verified against the SHA-256 values in the registry
manifest before use. A cache created from the bundled snapshot SHALL count as checked at
the time it was created.

#### Scenario: Offline scan with a local snapshot

- **WHEN** `registry_path` points to a registry snapshot
- **AND** `registry_offline: true` is set
- **THEN** the scan completes without network access
- **AND** the report states the snapshot as the registry source

#### Scenario: First scan uses the bundled snapshot

- **WHEN** a user runs `metacrafter scan file data.csv` with an empty cache
- **AND** the registry base URL is the default
- **THEN** the bundled snapshot is verified and copied into the cache
- **AND** nothing is downloaded
- **AND** the report states `registry.source` as `bundled` and the bundled registry version

#### Scenario: Offline with the bundled snapshot

- **WHEN** `registry_offline: true` is set, no cache exists and the base URL is the default
- **THEN** the scan resolves datatypes from the bundled snapshot

#### Scenario: Self-hosted registry ignores the bundled snapshot

- **WHEN** `METACRAFTER_REGISTRY_URL=http://localhost:8089` is set and no cache exists
- **THEN** the bundled snapshot is not used
- **AND** registry data is downloaded from `http://localhost:8089`

#### Scenario: Corrupted download is rejected

- **WHEN** a fetched bulk file does not match its manifest SHA-256
- **THEN** the file is not used
- **AND** the adapter falls back to the next source
- **AND** a warning names the file and the expected hash

#### Scenario: Registry unavailable

- **WHEN** no registry source can be loaded
- **THEN** the scan still completes with rule results
- **AND** the report states `registry.source` as `unavailable`

## ADDED Requirements

### Requirement: Bounded Registry Download

Registry requests SHALL accept gzip-compressed responses. A registry download during a scan
SHALL stop after `registry_fetch_timeout` seconds in total (default 30). When the download
fails or stops, the adapter SHALL use the next source and SHALL NOT try the network again
before `registry_cache_ttl` has passed. `metacrafter registry fetch` SHALL have no overall
time limit.

#### Scenario: Compressed download

- **WHEN** the registry serves `datatypes.jsonl` with `Content-Encoding: gzip`
- **THEN** the adapter decompresses it and verifies the decompressed bytes against the
  manifest SHA-256

#### Scenario: Slow refresh

- **WHEN** the cache is older than `registry_cache_ttl` and the download takes longer than
  `registry_fetch_timeout`
- **THEN** the scan stops the download and uses the older cache
- **AND** a warning names the base URL and the time limit
- **AND** the next scan within `registry_cache_ttl` uses the cache without network access

### Requirement: Bundled Registry Snapshot Version

The bundled registry snapshot SHALL have the registry `version` and `git_sha` recorded in
`ecosystem.lock`.

#### Scenario: Registry baseline bump

- **WHEN** a maintainer records a new registry version in `ecosystem.lock`
- **AND** does not run `scripts/bundle_registry_snapshot.py`
- **THEN** the test suite fails and names both versions
