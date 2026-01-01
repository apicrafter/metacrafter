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
├── metacrafter/          # Main package
│   ├── __init__.py       # Package initialization, exports exceptions
│   ├── __main__.py       # CLI entry point
│   ├── core.py           # Main CLI command handler (CrafterCmd)
│   ├── config.py         # Configuration file loader (.metacrafter)
│   ├── exceptions.py     # Custom exception classes
│   ├── classify/         # Core classification engine
│   │   ├── processor.py  # RulesProcessor - loads and applies rules
│   │   ├── stats.py      # Analyzer - field statistics and analysis
│   │   └── utils.py      # Utility functions
│   ├── core/             # Core validation utilities
│   │   └── validators.py # Validation functions
│   ├── registry/         # Registry client integration
│   │   └── client.py     # Client for metacrafter-registry
│   └── server/           # API server components
│       ├── api.py        # API endpoints
│       └── manager.py    # Server management
├── rules/                # Default rule files (YAML)
│   ├── basic/           # Basic identifier rules
│   ├── common/          # Common rules (dates, internet, etc.)
│   ├── pii/             # PII detection rules
│   ├── en/              # English-specific rules
│   ├── ru/              # Russian-specific rules
│   └── fr/              # French-specific rules
├── tests/               # Test suite
├── scripts/             # Utility scripts
└── setup.py            # Package setup
```

## Key Components

### 1. Core Engine (`metacrafter/core.py`)

The `CrafterCmd` class is the main entry point for all operations:
- `scan_file()` - Scan data files (CSV, JSON, Parquet, etc.)
- `scan_db()` - Scan SQL databases
- `scan_mongodb()` - Scan MongoDB databases
- `scan_data()` - Scan in-memory data (list of dicts)
- `scan_bulk()` - Scan multiple files in a directory

### 2. Rules Processor (`metacrafter/classify/processor.py`)

`RulesProcessor` handles:
- Loading YAML rule files from configured paths
- Compiling rules (text, PyParsing, function-based)
- Applying rules to field names and data values
- Filtering by context, language, country codes
- Confidence scoring

### 3. Statistics Analyzer (`metacrafter/classify/stats.py`)

`Analyzer` computes field statistics:
- Data type detection (str, int, float, dict, etc.)
- Uniqueness metrics
- Length statistics (min, max, avg)
- Character analysis (digits, alphas, special chars)
- Dictionary value detection

### 4. Configuration (`metacrafter/config.py`)

`ConfigLoader` reads `.metacrafter` YAML config files from:
- Current working directory
- User home directory (`~/.metacrafter`)

Configuration options:
- `rulepath`: List of directories containing rule YAML files

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
     rule: metacrafter.rules.ru.gov.is_ru_law
   ```

## Supported File Formats

Metacrafter uses `iterabledata` package for file format support:

**Text formats:** CSV, TSV, JSON, JSONL, XML
**Binary formats:** BSON, Parquet, Avro, ORC, Excel (XLS/XLSX), Pickle
**Compression:** gzip, bzip2, xz, lz4, zstandard, Brotli, Snappy, ZIP

Format detection is automatic based on file extension.

## Database Support

- **SQL databases:** Any database supported by SQLAlchemy (PostgreSQL, MySQL, SQLite, SQL Server, Oracle, DuckDB, etc.)
- **NoSQL:** MongoDB (via pymongo)

## Common Tasks

### Adding a New Rule

1. Create or edit a YAML file in `rules/` directory (or custom rulepath)
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
3. For new NoSQL databases, extend `scan_mongodb()` pattern in `core.py`

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

- `metacrafter/core.py` - Main CLI handler (2246 lines)
- `metacrafter/classify/processor.py` - Rule processing engine
- `metacrafter/classify/stats.py` - Statistics computation
- `metacrafter/config.py` - Configuration management
- `metacrafter/server/api.py` - API server endpoints

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

