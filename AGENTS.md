<!-- OPENSPEC:START -->
# OpenSpec Instructions

These instructions are for AI assistants working in this project.

Always open `@/openspec/AGENTS.md` when the request:
- Mentions planning or proposals (words like proposal, spec, change, plan)
- Introduces new capabilities, breaking changes, architecture shifts, or big performance/security work
- Sounds ambiguous and you need the authoritative spec before coding

Use `@/openspec/AGENTS.md` to learn:
- How to create and apply change proposals
- Spec format and conventions
- Project structure and guidelines

Keep this managed block so 'openspec update' can refresh the instructions.

<!-- OPENSPEC:END -->

# AGENTS.md - Metacrafter

This document provides guidance for AI agents working with the Metacrafter codebase.

## Overview

Metacrafter is a Python command-line tool and library for labeling table fields and data files. It uses rule-based classification to identify:
- Personal Identifiable Information (PII)
- Person names, surnames, midnames
- Basic identifiers (UUID/GUID, email, phone, etc.)
- Country/language-specific identifiers
- Dates and times
- Various semantic data types

## Repository Structure

```
metacrafter/
├── metacrafter/            # Main package
│   ├── scan_service.py     # CrafterCmd: shared scan service (CLI, SDK, HTTP, MCP)
│   ├── core/               # Compatibility exports (metacrafter.core.CrafterCmd, Typer apps)
│   ├── config.py           # .metacrafter loader; rulepath, registry, LLM and MCP settings
│   ├── registry/           # RegistryAdapter (sources, canonical ids, enrichment), rule key checks
│   ├── rules/              # Rule format JSON Schema (v2) and validation
│   ├── classify/           # RulesProcessor, Analyzer (statistics), LLM classifiers and providers
│   ├── engine/             # Scoring model, checksum validators, registry-derived detectors
│   ├── report/             # Report version 3 models/schema, legacy and Markdown output
│   ├── sources.py          # SQL, DuckDB, MongoDB adapters
│   ├── integrations/       # DataHub, OpenMetadata, Atlas, crosswalk labels, Frictionless
│   ├── interfaces/         # FastAPI HTTP API, MCP server
│   ├── product_tools.py    # report diff/pii, check, rules lint/test/draft, evaluate, dbt
│   ├── ecosystem.py        # Registry-example rule tests, thresholds, benchmark comparison
│   └── cli/                # Typer commands
├── skills/                 # Agent skills (metacrafter-scan, metacrafter-rule-author)
├── tests/                  # pytest; fixtures/registry_snapshot is the offline registry
│   ├── ecosystem/          # Known unresolved rule keys, rule thresholds
│   └── benchmark/          # Synthetic labelled data and baseline
├── openspec/               # Change proposals and ROADMAP.md
├── devdocs/                # Plans and reviews (MODERNIZATION_PLAN.md)
├── docs/                   # Docusaurus site (content in docs/docs/)
├── ecosystem.lock          # Pinned rules release and the registry version baselines use
└── pyproject.toml
```

Rules are not a Python dependency: the default pack is the pinned GitHub release of
metacrafter-rules (`metacrafter/rules/pack.py`). A copy of its archive ships in
`metacrafter/rules/bundled/` (refresh with `scripts/bundle_rule_pack.py` after a release
bump); it is verified by SHA-256 and unpacked into `~/.cache/metacrafter/rules/` on first use,
without network access. Other releases (`rules_release`) are downloaded from GitHub. Datatypes come from the registry website
(`/api/v1/`) through `metacrafter.registry.RegistryAdapter`; the registry's Python package is
not used. The package ships the registry snapshot recorded in `ecosystem.lock`
(`metacrafter/registry/bundled/`, written by `scripts/bundle_registry_snapshot.py`); a cold
cache is seeded from it, so the first scan does not download the registry.

## Key Components

### 1. Scan service (`metacrafter/scan_service.py`)

`CrafterCmd` is the entry point for every operation: `scan_file()`, `scan_db()`,
`scan_mongodb()`, `scan_data()`, `scan_bulk()`. It loads rules, binds the registry
(registry-derived detectors), runs matching, enriches matches from the registry and builds
report version 3.

### 2. Rules Processor (`metacrafter/classify/processor.py`)

Loads YAML rules (validated against `metacrafter/rules/schema/rule-v2.schema.json`), compiles
`text`, `ppr`, `func`, `regex` and `registry` rules, and scores matches with evidence
(`metacrafter/engine/scoring.py`). Later rulepath entries override earlier rule ids.

### 3. Registry adapter (`metacrafter/registry/adapter.py`)

Loads registry data (local path, verified cache, `/api/v1/` download, registry package
snapshot), resolves aliases to canonical ids, adds registry metadata and PII to matches, and
builds datatype URLs.

### 4. Configuration (`metacrafter/config.py`)

`ConfigLoader` reads `.metacrafter` from the current directory or home directory:
`rulepath`, `auto_rules`, `registry_url`, `registry_path`, `registry_offline`,
`registry_cache_ttl`, `registry_fetch_timeout`, `registry_detectors`, LLM settings and the
`mcp` section.

## Rule System

Rules are YAML files that define how to identify data types. Three match types:

1. **text** - Exact text matching (for field names)
   ```yaml
   midname:
     key: person_midname
     match: text
     type: field
     rule: midname,secondname,middlename
   ```

2. **ppr** - PyParsing pattern matching (for data values)
   ```yaml
   rukadastr:
     key: rukadastr
     match: ppr
     type: data
     rule: Word(nums, min=1, max=2) + Literal(':')...
   ```

3. **func** - Python function validation
   ```yaml
   runpabyfunc:
     key: runpa
     match: func
     type: data
     rule: metacrafterext.rules.ru.gov.is_ru_law
   ```

## Supported File Formats

Metacrafter uses `iterabledata` package for file format support:

**Text formats:** CSV, TSV, delimited TXT (GTFS), JSON, JSONL, XML, PC-Axis
**Geodata:** GeoJSON, TopoJSON, KML/KMZ, GML, zipped Shapefile, GeoPackage
**Binary formats:** BSON, Parquet, Avro, ORC, Excel (XLS/XLSX), ODS, SPSS (.sav/.zsav), Stata (.dta),
SAS (.sas7bdat/.xpt), dBase (.dbf), R data (.rda/.rds), SQLite (.db/.sqlite), iCalendar (.ics), Pickle

`metacrafter/file_inputs.py` reads the start of each file before iterabledata opens it and
fills in encoding, delimiter, header offset, JSON record key, PC-Axis encoding and the GML
fallback (`prepare_input`). It reads SPSS, Stata and SAS (pyreadstat dict output), GeoPackage
(sqlite3), iCalendar and KML/KMZ itself, picks the data table of a SQLite file and the code page
of a dBase file, names XML
column lists after their header element, and `select_sheets` resolves `scan file --sheet`.
**Compression:** gzip, bzip2, xz, lz4, zstandard, Brotli, Snappy, ZIP

Format detection is automatic based on file extension.

## Database Support

- **SQL databases:** Any database supported by SQLAlchemy (PostgreSQL, MySQL, SQLite, SQL Server, Oracle, DuckDB, etc.)
- **NoSQL:** MongoDB (via pymongo)

## Common Tasks

### Adding a New Rule

1. Create or edit a YAML file in a directory on your `rulepath` (rules for the default pack go to metacrafter-rules)
2. Define rule with appropriate match type (text/ppr/func)
3. Set metadata: key, name, type (field/data), priority, contexts, langs, country
4. Test with `metacrafter scan file test.csv`

### Extending Rule Validation Functions

1. Create Python module in appropriate location
2. Define function that accepts string/value and returns bool
3. Reference in rule YAML: `rule: package.module.function_name`
4. Ensure function is importable (may need to add to package)

### Adding Database Support

1. Ensure SQLAlchemy driver is available
2. Use connection string format: `dialect+driver://user:pass@host:port/db`
3. For new NoSQL databases, add an adapter in `metacrafter/sources.py` and a method on `CrafterCmd`

### Working with Registry Integration

The registry client (`metacrafter/registry/client.py`) connects to metacrafter-registry to:
- Fetch datatype metadata
- Resolve datatype URLs
- Get rule metadata

Registry URL defaults to `https://registry.apicrafter.io` but can be configured.

## CLI Usage Patterns

### Basic File Scan
```bash
metacrafter scan file data.csv --format full -o results.json
```

### Database Scan
```bash
metacrafter scan sql "postgresql://user:pass@localhost/db" --format full
```

### PII Detection
```bash
metacrafter scan file users.csv --contexts pii --langs en --confidence 20.0
```

### Server Mode
```bash
metacrafter server run --host 127.0.0.1 --port 10399
```

## Python API Usage

```python
from metacrafter.core import CrafterCmd

cmd = CrafterCmd()
report = cmd.scan_data(
    items=[{"email": "test@example.com"}],
    contexts="pii",
    langs="en",
    confidence=20.0
)
```

## Important Files

- `metacrafter/scan_service.py` - CrafterCmd scan service
- `metacrafter/classify/processor.py` - Rule loading and matching
- `metacrafter/registry/adapter.py` - Registry integration
- `metacrafter/report/models.py` - Report version 3
- `metacrafter/interfaces/` - HTTP API and MCP server
- `openspec/ROADMAP.md` - Change status and execution plan

## Dependencies

Key dependencies:
- `pyparsing` - Rule pattern matching
- `iterabledata` - File format support
- `sqlalchemy` - Database connectivity
- `pymongo` - MongoDB support
- `qddate` - Date/time pattern detection
- `typer` - CLI framework
- `pydantic` - Data validation
- `phonenumbers` - Phone number validation

## Testing

Tests are in `tests/` directory. Run with:
```bash
python setup.py test
# or
pytest tests/
```

## Error Handling

Custom exceptions in `metacrafter/exceptions.py`:
- `MetacrafterError` - Base exception
- `ConfigurationError` - Config file issues
- `RuleCompilationError` - Rule parsing/compilation failures
- `FileProcessingError` - File I/O issues
- `DatabaseError` - Database connection/query issues
- `ValidationError` - Data validation failures

## Contributing Guidelines

1. Follow existing code style
2. Add tests for new features
3. Update documentation (README.md) for user-facing changes
4. Ensure backward compatibility when possible
5. Use type hints where appropriate
6. Handle errors gracefully with appropriate exceptions

## Registry Integration

Metacrafter integrates with `metacrafter-registry` to:
- Link detected datatypes to registry entries
- Provide datatype URLs in output
- Fetch rule metadata

Registry is optional - Metacrafter works standalone but provides richer metadata when registry is available.


## Finishing a Work Session

1. **Track remaining work in GitHub Issues** (`gh issue create --repo apicrafter/metacrafter`);
   work for metacrafter-rules or metacrafter-registry goes to those repositories. Close issues
   that the change finishes (`Closes #N` in the PR).
2. **Run quality gates** when code changed: `pytest tests/`, the CI lint selection
   (`ruff check metacrafter tests scripts --select E9,F63,F7,F82`), `openspec validate --strict`
   for OpenSpec changes.
3. **Commit to a branch and push it**; changes reach `main` through a pull request. Do not push
   to `main` directly.
4. **Hand off**: say what is done, what is not verified and which issues remain.
