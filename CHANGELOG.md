# Changelog

All notable changes to Metacrafter will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Performance

- **Bundled registry snapshot.** The package ships the registry snapshot recorded in
  `ecosystem.lock` (`metacrafter/registry/bundled/`, about 550 KB). With the default
  registry URL, a cold cache is seeded from it, so the first scan downloads nothing. A
  first `metacrafter scan file` with an empty registry cache took 97–123 s and now takes
  1.4–2.0 s (a warm scan takes 1.4–1.7 s). `registry_offline` works on a new machine
  without `metacrafter registry fetch`. Update it with `scripts/bundle_registry_snapshot.py`.
- **Smaller, bounded registry downloads.** Registry requests accept gzip (3.1 MB to 561 KB
  for `datatypes.jsonl`). A refresh during a scan stops after `registry_fetch_timeout`
  seconds in total (new setting, default 30) and falls back to the older cache; a failed
  refresh is retried after `registry_cache_ttl`, not on every scan. `metacrafter registry
  fetch` has no overall time limit.

## [1.0.0] - 2026-11-06

Registry-driven modernization (see `devdocs/MODERNIZATION_PLAN.md`, `openspec/ROADMAP.md`)
together with the fixes from the 2026-10-01 repository review. Migration guide:
`docs/docs/getting-started/migration.md`.

### Distribution

Neither metacrafter-rules nor metacrafter-registry is a Python dependency:

- **Rule pack from GitHub releases.** The default rule pack is downloaded on first use from
  the pinned metacrafter-rules release (`v0.0.2`), verified by SHA-256 and cached.
  `metacrafter-rules` is no longer a Python dependency. New command: `metacrafter rules fetch`.
  New settings: `rules_dir`, `rules_release` + `rules_sha256`, `rules_offline`.
- **Registry through its website.** Metacrafter no longer uses the `metacrafter-registry`
  package:
  - the registry's checksum algorithms are vendored;
  - value identification, search and the qddate crosswalk run on the downloaded `/api/v1/` data;
  - `registry_path` must be a snapshot directory, which `metacrafter registry fetch --to DIR` writes;
  - the `registry` extra is removed.
- **CI.** The ecosystem gate uses the live registry. It blocks while the registry version
  matches `ecosystem.lock` and reports otherwise.

### Breaking

- Python 3.10 or newer.
- The in-repository `rules/` directory is removed; rules no longer load from `./rules`.
  The default rule pack comes from metacrafter-rules (see Distribution above).
- Optional dependencies moved to extras: `mongodb`, `duckdb`, `server` (now FastAPI +
  uvicorn instead of Flask), `formats` (Parquet, ORC, Excel, XML, compression), `mcp`,
  `llm` (Anthropic SDK), `llm-chroma` (deprecated vector index). Missing extras raise
  `MissingExtraError` naming the extra.
- Reports are version 3. Use `--output-format legacy` for the pre-1.0 JSON shape.
- JSON, YAML and Markdown output omit raw values unless `--include-samples` is set.
- Field-name-only matches score at most 60 (`--legacy-scoring` restores 100).
- A later rulepath entry now overrides an earlier rule with the same id.
- `priority: 1` is the strongest rule priority (rules sort ascending).
- LLM providers share one OpenAI-compatible HTTP client; the OpenAI SDK is no longer used
  for chat. `llm_registry_path` is deprecated.

### Added

- Registry adapter: offline-capable loading with SHA-256 verification, canonical ids for
  alias keys, registry metadata on every match (`registry`, `datatype`, `rule_key`,
  `pii_source`, `replaced_by`), registry provenance in reports, configurable registry URL,
  filters `--categories`, `--pii-only`, `--status`.
- Rule format version 2 with a JSON Schema; `match: regex`, `match: registry`,
  `validator: checksum:<algorithm>`; `deprecated` rules; `metacrafter rules validate`.
- Registry-derived detectors for datatypes without rules (`--registry-detectors`).
- Scoring model v1 with an `evidence` object; qddate formats mapped to registry patterns.
- `metacrafter rules check --registry`, `rules test --registry-examples`, `evaluate --baseline`.
- `--output-format md`; `metacrafter export labels --system presidio|google_dlp|dpv|purview|...`;
  `metacrafter export frictionless`.
- FastAPI HTTP API: `POST /v2/scan`, `POST /v2/classify`, `/health`, `/ready`, OpenAPI.
- MCP server (`metacrafter mcp`) with scoped, read-only tools; agent skills in `skills/`;
  `llms.txt` for the documentation site.
- Registry-constrained LLM classification with PII-aware disclosure and an Anthropic provider.
- From the repository review: shared scan service, report interchange and bulk output,
  bounded head/reservoir sampling, schema-aware SQL quoting, export outcome accounting,
  report diff, PII inventory, policy checks, rule lint/test/draft, `scan explain`,
  dbt dry-run export, secret redaction, hermetic tests.
- Ecosystem contract tests in CI (pinned registry and rules) and a weekly compatibility check.

### Fixed

- Boolean `is_pii` in rules was ignored.
- Catalog exporters used a hard-coded list of five PII datatypes.
- `metacrafter scan file` crashed when `--llm-min-confidence` was not given.
- Valid PyParsing rules with tuple or indexed repetition were rejected.
- Loading the default rule pack took 4.6 s (now about 0.4 s).

## [0.0.5] - 2026-07-08

### Added
- **LLM-Based Classification**: New LLM-powered classification using Retrieval-Augmented Generation (RAG)
  - Support for multiple LLM providers: OpenAI, OpenRouter, Ollama, LM Studio, and Perplexity
  - Three classification modes: rules-only (default), LLM-only, and hybrid (rules + LLM fallback)
  - Vector-based similarity search using ChromaDB for retrieving relevant registry entries
  - Automatic index building from registry on first use
  - Configurable confidence thresholds for LLM results
  - CLI options: `--classification-mode`, `--llm-provider`, `--llm-model`, `--llm-api-key`, `--llm-base-url`, etc.
  - Configuration support via `.metacrafter` config file
  - Optional dependencies: `openai`, `chromadb`, `requests`
  - See [README](README.md#llm-based-classification) for usage examples
- **Apache Atlas Integration**: New export command to push Metacrafter scan results to Apache Atlas metadata catalog
  - Export PII labels, datatypes, and confidence scores as classifications and custom attributes
  - CLI command: `metacrafter export atlas`
  - Configuration support via `.metacrafter` config file or environment variables
  - Optional dependency: `requests`
  - See [Implementation Guide](devdocs/ISSUE_ATLAS_IMPLEMENTATION.md)
- **DataHub Integration**: New export command to push Metacrafter scan results to DataHub metadata catalog
  - Export PII labels, datatypes, and confidence scores as tags, glossary terms, and custom properties
  - CLI command: `metacrafter export datahub`
  - Configuration support via `.metacrafter` config file or environment variables
  - Optional dependency: `acryl-datahub[datahub-rest]`
  - See [Issue #24](https://github.com/apicrafter/metacrafter/issues/24) and [Implementation Guide](devdocs/ISSUE_24_IMPLEMENTATION.md)
- **OpenMetadata Integration**: New export command to push Metacrafter scan results to OpenMetadata metadata catalog
  - Export PII labels, datatypes, and confidence scores as tags, glossary terms, and custom properties
  - CLI command: `metacrafter export openmetadata`
  - Configuration support via `.metacrafter` config file or environment variables
  - Optional dependency: `openmetadata-ingestion`
  - See [Implementation Guide](devdocs/ISSUE_OPENMETADATA_IMPLEMENTATION.md)
- **Rules Inspection Commands**: New commands for inspecting and managing classification rules
  - `metacrafter rules list`: List all loaded rules with metadata (ID, name, type, match method, language, country, contexts)
    - Supports multiple output formats: table (default), JSON, YAML, CSV
    - Filterable by rule path and country codes
  - `metacrafter rules stats`: Display aggregate statistics about loaded rules
    - Shows counts of field rules, data rules, languages, contexts, country codes, and date/time patterns

### Changed
- Updated configuration system to support LLM, DataHub, Apache Atlas, and OpenMetadata settings
- Enhanced CLI with new `export` command group, `rules` command group, and LLM classification options
- Extended `MetacrafterConfig` with LLM-related fields and validation
- Improved error handling for missing optional dependencies (graceful degradation)

## [0.0.4] - Previous Release

### Added
- Support for multiple file formats (CSV, JSON, JSONL, XML, Parquet, Avro, ORC, Excel, BSON, Pickle)
- Support for compressed files (gzip, bzip2, xz, lz4, zstandard, Brotli, Snappy, ZIP)
- Database scanning for SQL databases (via SQLAlchemy) and MongoDB
- Rule-based classification system with 111+ rules
- Date detection with 312+ patterns
- Context and language filtering
- Built-in API server
- Statistics and field analysis

### Changed
- Improved error handling and logging
- Enhanced output formats (table, JSON, YAML, CSV)

## [0.0.3] - Earlier Release

Initial public release with core functionality.

---

## Notes

- **LLM Classification**: The LLM classification feature requires optional dependencies: `openai`, `chromadb`, and `requests`. Install them with `pip install openai chromadb requests` if you plan to use LLM-based classification. The feature gracefully degrades if dependencies are missing.
- **Apache Atlas Integration**: The Apache Atlas integration requires the optional `requests` package. Install it separately if you plan to use this feature.
- **DataHub Integration**: The DataHub integration requires the optional `acryl-datahub[datahub-rest]` package. Install it separately if you plan to use this feature.
- **OpenMetadata Integration**: The OpenMetadata integration requires the optional `openmetadata-ingestion` package. Install it separately if you plan to use this feature.
- **Configuration**: LLM, DataHub, Apache Atlas, and OpenMetadata settings can be configured in `.metacrafter` config file or via environment variables.

