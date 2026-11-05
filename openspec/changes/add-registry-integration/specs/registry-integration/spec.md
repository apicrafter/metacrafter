## ADDED Requirements

### Requirement: Registry Source Resolution

Metacrafter SHALL load registry data through one adapter. The adapter SHALL try sources in
this order: a configured local path, a verified cache, a network fetch from the configured
base URL, and the snapshot bundled with the registry package. Fetched data SHALL be
verified against the SHA-256 values in the registry manifest before use.

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

### Requirement: Configurable Registry Base URL

Datatype URLs in every output SHALL be built from the registry manifest URL template,
rebased on the configured base URL. The base URL SHALL be taken from
`METACRAFTER_REGISTRY_URL`, then `registry_url` in `.metacrafter`, then the default
`https://registry.apicrafter.io`.

#### Scenario: Self-hosted registry

- **WHEN** `METACRAFTER_REGISTRY_URL=http://localhost:8089` is set
- **THEN** every datatype URL in SDK, CLI and HTTP API output starts with
  `http://localhost:8089`

### Requirement: Canonical Datatype Resolution

Metacrafter SHALL resolve every rule key to a canonical registry id. Each match SHALL
include the canonical id as `datatype` and the original key as `rule_key`.

#### Scenario: Alias key

- **WHEN** a rule with key `ssn` matches
- **THEN** the match reports `datatype` as `usssn` and `rule_key` as `ssn`
- **AND** the URL points to the `usssn` record

#### Scenario: Unknown key

- **WHEN** a rule key matches no registry id or alias
- **THEN** the match reports `datatype` as null and keeps `rule_key`
- **AND** one warning per key is logged

#### Scenario: Deprecated record

- **WHEN** a match resolves to a record with `status: deprecated` and `replaced_by: X`
- **THEN** the match keeps the deprecated id and reports `replaced_by` as `X`

### Requirement: Registry Match Enrichment

Each resolved match SHALL carry registry metadata:
- `name`, `categories`, `is_pii`, `classification`, `status`, `country`, `url`;
- the `mappings` for presidio, google_dlp, dpv, purview, schema_org and wikidata, when present.

#### Scenario: Enriched email match

- **WHEN** a field matches datatype `email`
- **THEN** the match includes the registry name, categories, `status` and `url` for `email`
- **AND** includes the Presidio and Google DLP mapping values when the record has them

### Requirement: Registry-Based PII Determination

A match SHALL be marked `is_pii` when the registry record has `is_pii: true`, when the rule
sets `is_pii: true`, or when the rule context includes `pii`. The match SHALL report which
source set the flag in `pii_source`.

#### Scenario: PII from the registry

- **WHEN** a rule without a `pii` context matches a registry record with `is_pii: true`
- **THEN** the match has `is_pii: true` and `pii_source: registry`

#### Scenario: Boolean rule flag

- **WHEN** a rule YAML sets `is_pii: true` as a boolean
- **THEN** the match has `is_pii: true` and `pii_source: rule`

### Requirement: Registry Metadata Filters

Scan commands and the SDK SHALL accept filters on registry metadata: categories, PII only,
and record status. Filters SHALL apply to which matches are reported.

#### Scenario: Stable PII only

- **WHEN** a user runs `metacrafter scan file users.csv --pii-only --status stable`
- **THEN** the report lists only matches whose datatype is PII and has `status: stable`

### Requirement: Registry Provenance

Every scan report SHALL include the registry version, git sha, generation timestamp,
source kind and base URL used to resolve its matches.

#### Scenario: Reproducible report

- **WHEN** two scans run against the same registry snapshot
- **THEN** both reports carry identical registry provenance values

### Requirement: Registry Rule Check Command

Metacrafter SHALL provide `metacrafter rules check --registry`. The command SHALL list
rule keys that are unknown, that resolve only through an alias, or that resolve to a
deprecated record. With `--strict`, it SHALL exit non-zero when unknown keys exist.

#### Scenario: Strict check fails on unknown keys

- **WHEN** the loaded rules contain a key that is neither a registry id nor an alias
- **AND** the user runs `metacrafter rules check --registry --strict`
- **THEN** the command lists the key with its rule id and file
- **AND** exits with code 1
