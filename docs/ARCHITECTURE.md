# Metacrafter Architecture Documentation

## Table of Contents

1. [Overview](#overview)
2. [High-Level Architecture](#high-level-architecture)
3. [Core Components](#core-components)
4. [Data Flow](#data-flow)
5. [Rule System](#rule-system)
6. [File Format Support](#file-format-support)
7. [Database Support](#database-support)
8. [API Server Architecture](#api-server-architecture)
9. [Configuration System](#configuration-system)
10. [Extension Points](#extension-points)
11. [Performance Considerations](#performance-considerations)

## Overview

Metacrafter is a rule-based data classification engine designed to automatically label and classify fields in structured data sources. It identifies data types such as email addresses, phone numbers, person names, PII (Personally Identifiable Information), dates, identifiers (UUIDs, GUIDs), and many other semantic types.

### Key Features

- **Rule-Based Classification**: Extensible YAML-based rule system with 111+ labeling rules
- **Multi-Format Support**: Handles CSV, JSON, JSONL, XML, Parquet, Avro, ORC, Excel, BSON, and more
- **Database Integration**: Supports SQL databases (via SQLAlchemy) and MongoDB
- **Language & Context Awareness**: Rules can be filtered by language and context (e.g., PII detection)
- **Date Pattern Detection**: 312+ date detection patterns using the `qddate` library
- **REST API Server**: Built-in Flask-based API server for remote classification
- **Registry Integration**: Links to semantic data types registry at `registry.apicrafter.io`

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      CLI Interface (Typer)                   │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │ scan commands│  │ rules commands│  │server commands│      │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘       │
└─────────┼─────────────────┼─────────────────┼───────────────┘
          │                 │                 │
          └─────────────────┼─────────────────┘
                            │
          ┌─────────────────▼─────────────────┐
          │        CrafterCmd (Core)           │
          │  - Orchestrates scanning           │
          │  - Handles I/O and formatting      │
          │  - Manages remote/local execution  │
          └─────────────────┬─────────────────┘
                            │
        ┌───────────────────┼───────────────────┐
        │                   │                   │
┌───────▼────────┐  ┌───────▼────────┐  ┌───────▼────────┐
│ RulesProcessor │  │    Analyzer    │  │  DateParser     │
│  - Rule loading│  │  - Statistics  │  │  - Date patterns│
│  - Matching     │  │  - Field stats │  │  - Pattern match│
│  - Filtering    │  │  - Type detect │  │                 │
└───────┬────────┘  └───────┬────────┘  └─────────────────┘
        │                   │
        │         ┌─────────▼─────────┐
        │         │   Data Sources     │
        │         │  - Files (iterable)│
        │         │  - SQL databases   │
        │         │  - MongoDB         │
        │         └────────────────────┘
        │
┌───────▼───────────────────────────────────────┐
│           Rule System (YAML)                  │
│  - Field rules (match field names)            │
│  - Data rules (match data values)             │
│  - PyParsing, text, function matchers         │
└───────────────────────────────────────────────┘
```

## Core Components

### 1. CLI Layer (`metacrafter/core.py`)

The CLI layer is built using **Typer** and provides three main command groups:

- **`scan`**: Commands for scanning files, databases, and directories
- **`rules`**: Commands for inspecting and managing rules
- **`server`**: Commands for running the API server

**Entry Point**: `metacrafter/__main__.py` → `core.app()`

### 2. CrafterCmd Class (`metacrafter/core.py`)

The central orchestrator class that coordinates all operations:

```python
class CrafterCmd:
    - prepare()              # Load rules and initialize processors
    - scan_file()           # Scan data files
    - scan_db()             # Scan SQL databases
    - scan_mongodb()        # Scan MongoDB databases
    - scan_data()           # Scan in-memory data structures
    - scan_bulk()           # Scan multiple files in directory
    - rules_list()          # List all loaded rules
    - rules_dumpstats()     # Show rule statistics
```

**Key Responsibilities**:
- Configuration management (rule paths, country codes, etc.)
- Format conversion (table, JSON, YAML, CSV)
- Progress reporting (via `tqdm`)
- Remote API integration
- Output formatting and writing

### 3. RulesProcessor (`metacrafter/classify/processor.py`)

The core classification engine that loads, compiles, and applies rules:

```python
class RulesProcessor:
    - import_rules_path()   # Load rules from directory
    - import_rules()        # Load rules from YAML file
    - match_dict()          # Classify fields in dictionary/list
    - match_field()         # Match field name rules
    - match_data()          # Match data value rules
```

**Key Features**:
- **Rule Types**: Field rules (match column names) and Data rules (match values)
- **Match Engines**: 
  - `text`: Exact text matching (case-insensitive)
  - `ppr`: PyParsing pattern matching
  - `func`: Python function-based matching
- **Filtering**: Language, context, and country code filtering
- **Confidence Scoring**: Calculates match confidence based on sample data

**Rule Compilation**:
- PyParsing rules are compiled with security restrictions (no dangerous code)
- Function rules are dynamically imported from modules
- Text rules are converted to optimized sets for O(1) lookup

### 4. Analyzer (`metacrafter/classify/stats.py`)

Generates field-level statistics for data analysis:

```python
class Analyzer:
    - analyze()             # Analyze data and generate statistics
    - guess_datatype()      # Infer data type from value
```

**Statistics Generated**:
- Field type (`str`, `int`, `float`, `bool`, `date`, etc.)
- Length statistics (min, max, average)
- Uniqueness metrics (unique count, uniqueness percentage)
- Dictionary detection (low-cardinality fields)
- Character analysis (digits, alphabetic, special characters)
- Empty value detection

### 5. Date Parser (`qddate` integration)

Uses the `qddate` library for date pattern detection:
- 312+ date/time patterns
- Supports English and Russian date formats
- Pattern matching with confidence scoring
- Format extraction (e.g., "YYYY-MM-DD", "DD/MM/YYYY")

### 6. Configuration System (`metacrafter/config.py`)

Manages configuration loading and validation:

```python
class ConfigLoader:
    - load_config()         # Load from .metacrafter file
    - get_rulepath()        # Get validated rule paths
```

**Configuration Sources** (in order of precedence):
1. Command-line arguments (`--rulepath`)
2. Local `.metacrafter` file (current directory)
3. Home directory `~/.metacrafter` file
4. Default: `["rules"]`

**Configuration Format**:
```yaml
rulepath:
  - ./rules
  - ./custom_rules
  - /path/to/additional/rules
```

## Data Flow

### File Scanning Flow

```
1. User invokes: metacrafter scan file data.csv
   │
   ▼
2. CLI parses arguments → CrafterCmd instance created
   │
   ▼
3. CrafterCmd.prepare()
   ├─→ ConfigLoader.get_rulepath()
   ├─→ RulesProcessor.import_rules_path() (loads all YAML rules)
   └─→ DateParser initialization
   │
   ▼
4. CrafterCmd.scan_file()
   ├─→ Detect file format (via iterabledata)
   ├─→ Open file with appropriate reader
   │
   ▼
5. For each batch of records:
   ├─→ Analyzer.analyze() → Generate field statistics
   └─→ RulesProcessor.match_dict() → Classify fields
      ├─→ Match field names (field rules)
      ├─→ Match data values (data rules)
      └─→ Match date patterns (qddate)
   │
   ▼
6. Aggregate results → Calculate confidence scores
   │
   ▼
7. Format output (table/JSON/YAML/CSV) → Write to file/stdout
```

### Database Scanning Flow

```
1. User invokes: metacrafter scan sql "postgresql://..."
   │
   ▼
2. CrafterCmd.scan_db()
   ├─→ SQLAlchemy connection → List tables/schemas
   │
   ▼
3. For each table:
   ├─→ Query rows in batches (batch_size)
   ├─→ Convert rows to dict format
   │
   ▼
4. Process like file scanning:
   ├─→ Analyzer.analyze()
   └─→ RulesProcessor.match_dict()
   │
   ▼
5. Aggregate per-table results
   │
   ▼
6. Format multi-table output → Write results
```

### Rule Matching Flow

```
1. RulesProcessor.match_dict(items, datastats, ...)
   │
   ▼
2. For each field in data:
   │
   ├─→ Field Name Matching (if field rules enabled)
   │   ├─→ Check field_rules list
   │   ├─→ Apply text/ppr/func matchers
   │   └─→ If match → Add RuleResult
   │
   ├─→ Data Value Matching (if data rules enabled)
   │   ├─→ Sample values (up to limit)
   │   ├─→ For each data rule:
   │   │   ├─→ Check length constraints (minlen/maxlen)
   │   │   ├─→ Apply match function (text/ppr/func)
   │   │   ├─→ Run validator if present
   │   │   └─→ Calculate match percentage
   │   └─→ If confidence > threshold → Add RuleResult
   │
   └─→ Date Pattern Matching (if enabled)
       ├─→ For each value → qddate.match()
       └─→ If pattern found → Add RuleResult with format
   │
   ▼
3. Calculate confidence scores:
   ├─→ Match percentage = (matches / total_samples) * 100
   ├─→ Apply imprecise rule penalties
   └─→ Filter by confidence threshold
   │
   ▼
4. Return TableScanResult with ColumnMatchResult per field
```

## Rule System

### Rule File Structure

Rules are organized in YAML files with the following structure:

```yaml
name: common                    # Rule group name
description: Common data types  # Description
context: common                # Context filter (common, pii, etc.)
lang: common                   # Language code (en, ru, fr, common, etc.)
country_code: us,ca            # Optional: ISO country codes (comma-separated)
rules:
  rule_id:                     # Unique rule identifier
    key: datatype_key          # Data type identifier (e.g., "email", "phone")
    name: Human readable name  # Display name
    type: field|data           # Rule type: field (match name) or data (match value)
    match: text|ppr|func       # Match engine type
    rule: <rule_definition>    # Rule definition (depends on match type)
    priority: 1                # Priority (higher = more important)
    minlen: 3                  # Minimum value length
    maxlen: 100                # Maximum value length
    imprecise: 0|1             # Whether rule is imprecise (default: 0)
    is_pii: true|false         # Whether this is PII data
    validator: module.func     # Optional: validation function
    fieldrule: <rule>          # Optional: additional field name rule
    fieldrulematch: text|ppr   # Match type for fieldrule
```

### Rule Types

#### 1. Field Rules (`type: field`)

Match based on column/field names:

```yaml
email:
  key: email
  name: Email by known field name
  type: field
  match: text
  rule: email,e_mail,email_address,emailAddress
```

**Use Cases**: Identifying columns by common naming conventions

#### 2. Data Rules (`type: data`)

Match based on data values:

**Text Matching**:
```yaml
mimetypebyvalue:
  key: mimetype
  name: Mimetype by value
  type: data
  match: text
  rule: application/pdf,application/json,image/jpeg
```

**PyParsing Matching**:
```yaml
ru_kadastr:
  key: rukadastr
  name: Russian cadastral number
  type: data
  match: ppr
  rule: Word(nums, min=1, max=2) + Literal(':').suppress() + Word(nums, min=1, max=2) + ...
```

**Function Matching**:
```yaml
runpabyfunc:
  key: runpa
  name: Russian legal act / law
  type: data
  match: func
  rule: metacrafter.rules.ru.gov.is_ru_law
```

### Rule Organization

Rules are organized in directories by language and context:

```
rules/
├── basic/              # Basic identifiers (UUID, crypto, etc.)
├── common/             # Common data types (email, phone, URLs)
├── en/                 # English-specific rules
├── fr/                 # French-specific rules
├── ru/                 # Russian-specific rules
├── pii/                # PII detection rules
│   ├── pii.yaml
│   ├── fr/
│   └── ru/
└── ...
```

### Rule Filtering

Rules can be filtered at load time or runtime:

- **Language Filtering**: `--langs en,ru` → Only load rules for English and Russian
- **Context Filtering**: `--contexts pii` → Only load PII-related rules
- **Country Filtering**: `--country-codes us,ca` → Only load US/Canada rules

### Rule Compilation Security

PyParsing rules are compiled with security restrictions:
- No `import`, `exec`, `eval`, `compile` statements
- No access to `__builtins__` except safe functions (`len`, `str`, `int`, etc.)
- Restricted namespace with only PyParsing classes
- Cached compilation results (`@lru_cache`)

## File Format Support

Metacrafter uses the **`iterabledata`** package for file format support, providing automatic format detection and unified iteration interface.

### Supported Formats

**Text Formats**:
- CSV, TSV (with auto-delimiter detection)
- JSON (array of objects)
- JSONL/NDJSON (newline-delimited JSON)
- XML (with configurable tag name)

**Binary Formats**:
- Parquet (Apache Parquet)
- Avro (Apache Avro)
- ORC (Apache ORC)
- Excel (`.xls`, `.xlsx`)
- BSON (Binary JSON)
- Pickle (`.pickle`, `.pkl`)

**Compression Codecs** (auto-detected):
- gzip (`.gz`)
- bzip2 (`.bz2`)
- xz (`.xz`)
- lz4 (`.lz4`)
- zstandard (`.zst`)
- Brotli (`.br`)
- Snappy
- ZIP (`.zip`)

### Format Detection

Format detection is automatic based on file extension:
- `data.csv.gz` → Detected as gzip-compressed CSV
- `data.jsonl.bz2` → Detected as bzip2-compressed JSONL
- Compression and underlying format are both detected

### File Reading Flow

```python
# In CrafterCmd.scan_file()
from iterable.helpers.detect import open_iterable

with open_iterable(filename, **options) as reader:
    for record in reader:
        # Process each record as dict
        process_record(record)
```

## Database Support

### SQL Databases (SQLAlchemy)

Metacrafter supports any database with SQLAlchemy support:

- **PostgreSQL**: `postgresql+psycopg2://...`
- **MySQL/MariaDB**: `mysql+pymysql://...`
- **SQLite**: `sqlite:///path/to/db`
- **SQL Server**: `mssql+pyodbc://...`
- **Oracle**: `oracle+cx_oracle://...`
- **DuckDB**: `duckdb:///path/to/db`

**Scanning Process**:
1. Connect via SQLAlchemy connection string
2. List tables (optionally filtered by schema)
3. For each table:
   - Query rows in batches (`batch_size`)
   - Convert to dict format (column name → value)
   - Process like file data

**Batch Processing**:
- Configurable batch size (default: 1000 rows)
- Progress reporting with `tqdm`
- Memory-efficient streaming

### MongoDB

MongoDB support via `pymongo`:

**Connection Methods**:
- Host/port: `scan_mongodb(host="localhost", port=27017)`
- Connection URI: `scan_mongodb(host="mongodb://...")`

**Scanning Process**:
1. Connect to MongoDB
2. List collections in database
3. For each collection:
   - Fetch documents in batches
   - Process documents (already dict-like)
   - Classify fields

**Features**:
- Authentication support (username/password)
- Replica set support
- Batch cursor configuration

## API Server Architecture

### Server Components

**MetacrafterApp** (`metacrafter/server/api.py`):
- Flask application factory
- Dependency injection for rules processor and date parser
- Lazy initialization of heavy components

**Server Manager** (`metacrafter/server/manager.py`):
- Server startup and configuration
- Secret key management (from environment)
- Debug logging configuration

### API Endpoints

**POST `/api/v1/scan_data`**:
- Accepts JSON array of items (list of dicts)
- Query parameters:
  - `format`: Output format (`short`/`full`)
  - `langs`: Comma-separated language filters
  - `contexts`: Comma-separated context filters
  - `limit`: Maximum records per field (default: 1000)
- Returns JSON with classification results

**Request Example**:
```json
[
  {"email": "user@example.com", "name": "John Doe"},
  {"email": "admin@example.com", "name": "Jane Smith"}
]
```

**Response Example**:
```json
{
  "results": [
    ["email", "str", "", "email 98.50", "https://registry.apicrafter.io/datatype/email"],
    ["name", "str", "", "name 100.00", "https://registry.apicrafter.io/datatype/name"]
  ],
  "data": [
    {
      "field": "email",
      "matches": [...],
      "ftype": "str",
      "tags": [],
      "datatype_url": "https://registry.apicrafter.io/datatype/email",
      "stats": {...}
    }
  ]
}
```

### Remote Scanning

CLI can use remote API server:

```bash
metacrafter scan file data.csv --remote http://localhost:10399
```

**Flow**:
1. CLI detects `--remote` flag
2. Data is serialized to JSON
3. POST request to `/api/v1/scan_data`
4. Response is parsed and formatted locally

**Benefits**:
- Centralized rule management
- Resource sharing
- API integration

## Configuration System

### Configuration Loading Order

1. **Command-line arguments** (highest priority)
   - `--rulepath ./custom_rules`
   - `--country-codes us,ca`

2. **Local `.metacrafter` file** (current directory)
   ```yaml
   rulepath:
     - ./rules
     - ./custom_rules
   ```

3. **Home directory `~/.metacrafter`** (user-wide config)

4. **Defaults** (lowest priority)
   - `rulepath: ["rules"]`

### Configuration Validation

Configuration is validated using **Pydantic**:
- Type checking
- Path existence validation
- Error messages for invalid configs

### Environment Variables

- `METACRAFTER_SECRET_KEY`: Secret key for API server (optional)

## Extension Points

### Adding Custom Rules

1. **Create YAML file** in rules directory:
```yaml
name: custom
description: Custom rules
context: common
lang: common
rules:
  my_rule:
    key: custom_type
    name: Custom data type
    type: data
    match: ppr
    rule: Word(alphas, min=3, max=10)
```

2. **Add to rulepath** (config file or CLI):
```bash
metacrafter scan file data.csv --rulepath ./rules,./custom_rules
```

### Adding Custom Validators

Validators are Python functions that validate matched values:

```python
# In your module
def validate_custom_type(value: str) -> bool:
    # Custom validation logic
    return len(value) > 5 and value.isalnum()

# In rule YAML
my_rule:
  validator: mymodule.validators.validate_custom_type
```

### Adding Custom Match Functions

```python
# In your module
def match_custom_pattern(value: str) -> bool:
    # Custom matching logic
    return value.startswith("CUSTOM_")

# In rule YAML
my_rule:
  match: func
  rule: mymodule.matchers.match_custom_pattern
```

### Adding File Format Support

File format support is handled by `iterabledata`. To add support:
1. Ensure `iterabledata` supports the format
2. Metacrafter will automatically detect and use it

## Performance Considerations

### Rule Compilation Caching

- PyParsing rules are cached with `@lru_cache(maxsize=256)`
- Reduces recompilation overhead for repeated rules

### Batch Processing

- Database queries use configurable batch sizes
- Default: 1000 rows/documents per batch
- Reduces memory usage for large datasets

### Sampling

- Data rules only sample up to `limit` values per field (default: 1000)
- Reduces processing time for large datasets
- Confidence scores are calculated from samples

### Progress Reporting

- Optional `tqdm` progress bars
- Can be disabled with `--quiet` flag
- Minimal overhead when disabled

### Memory Management

- Streaming file readers (via `iterabledata`)
- Batch-based database queries
- Results are aggregated incrementally

### Remote API Considerations

- Network latency for remote scans
- Configurable timeout and retry logic
- Batch size affects API payload size

## Key Design Decisions

### 1. Rule-Based Architecture

**Why**: Extensibility and maintainability
- Rules are declarative (YAML)
- Easy to add new rules without code changes
- Rules can be versioned and shared

### 2. Multiple Match Engines

**Why**: Flexibility for different use cases
- Text matching: Fast, exact matches
- PyParsing: Complex pattern matching
- Functions: Custom logic

### 3. Confidence Scoring

**Why**: Handle ambiguous matches
- Percentage-based confidence (0-100)
- Threshold filtering
- Multiple matches per field

### 4. Language & Context Filtering

**Why**: Performance and relevance
- Reduce rule set size
- Focus on relevant rules
- Support multi-language datasets

### 5. Unified Data Interface

**Why**: Code reuse across formats
- All sources convert to dict/list format
- Single processing pipeline
- Consistent output format

### 6. Security in Rule Compilation

**Why**: Prevent code injection
- Restricted namespace for PyParsing
- Function imports are controlled
- No arbitrary code execution

## Future Architecture Considerations

### Potential Enhancements

1. **Rule Versioning**: Track rule versions and changes
2. **Rule Testing Framework**: Unit tests for rules
3. **Distributed Processing**: Support for distributed scanning
4. **Rule Marketplace**: Share and discover rules
5. **Machine Learning Integration**: ML-based classification alongside rules
6. **Incremental Scanning**: Track changes in data sources
7. **Rule Performance Metrics**: Track rule execution time and accuracy

## Dependencies

### Core Dependencies

- **typer**: CLI framework
- **pyparsing**: Pattern matching engine
- **PyYAML**: Rule file parsing
- **pydantic**: Configuration validation
- **tabulate**: Table formatting
- **iterabledata**: File format support
- **qddate**: Date pattern detection
- **sqlalchemy**: SQL database support
- **pymongo**: MongoDB support

### Optional Dependencies

- **tqdm**: Progress bars
- **orjson**: Fast JSON parsing (used in stats module)

## Conclusion

Metacrafter's architecture is designed for:
- **Extensibility**: Easy to add new rules and formats
- **Performance**: Efficient processing of large datasets
- **Flexibility**: Multiple interfaces (CLI, API, Python library)
- **Security**: Safe rule compilation and execution
- **Maintainability**: Clear separation of concerns

The modular design allows components to evolve independently while maintaining a consistent interface for users and developers.

