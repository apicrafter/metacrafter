# Changelog

All notable changes to Metacrafter will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

