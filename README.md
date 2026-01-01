# Metacrafter

Python command line tool and python engine to label table fields and fields in data files.
It could help to find meaningful data in your tables and data files or to find Personal identifable information (PII).


## Installation

To install Python library use `pip install metacrafter` via pip or `python setup.py install` 

## Features

Metacrafter is a rule based tool that helps to label fields of the tables in databases. It scans table and finds person names, surnames, midnames, PII data, basic identifiers like UUID/GUID. 
These rules written as .yaml files and could be easily extended.

File formats supported:

Metacrafter supports a wide range of data file formats through the `iterabledata` package. Format detection is automatic based on file extension.

**Text-based formats:**
* **CSV** (`.csv`) - Comma-separated values
* **TSV** (`.tsv`) - Tab-separated values
* **JSON** (`.json`) - JSON array of objects
* **JSONL/NDJSON** (`.jsonl`, `.ndjson`) - JSON Lines / Newline-delimited JSON
* **XML** (`.xml`) - Extensible Markup Language

**Binary formats:**
* **BSON** (`.bson`) - Binary JSON
* **Parquet** (`.parquet`) - Apache Parquet columnar storage
* **Avro** (`.avro`) - Apache Avro data serialization
* **ORC** (`.orc`) - Apache ORC columnar storage
* **Excel** (`.xls`, `.xlsx`) - Microsoft Excel spreadsheets
* **Pickle** (`.pickle`, `.pkl`) - Python pickle serialization

**Compression codecs:**

All supported formats can be compressed with the following codecs (automatically detected):
* **gzip** (`.gz`) - GNU zip compression
* **bzip2** (`.bz2`) - Bzip2 compression
* **xz** (`.xz`) - XZ compression (LZMA2)
* **lz4** (`.lz4`) - LZ4 fast compression
* **zstandard** (`.zst`) - Zstandard compression
* **Brotli** (`.br`) - Brotli compression
* **Snappy** - Snappy compression
* **ZIP** (`.zip`) - ZIP archive format

**Format detection:**

Metacrafter automatically detects file formats based on file extensions. For compressed files, both the compression codec and underlying format are detected automatically (e.g., `data.csv.gz` is detected as gzip-compressed CSV).

**Format-specific options:**

* **CSV/TSV**: Use `--delimiter` to specify custom delimiters (default: auto-detected)
* **XML**: Use `--tagname` to specify the XML tag containing data records
* **Encoding**: Use `--encoding` to specify character encoding (default: auto-detected)
* **Compression**: Use `--compression` to force compression handling (`auto`, `none`, or specific codec)

Databases support:
* Any SQL database supported by [SQLAlchemy](https://www.sqlalchemy.org/) 
* NoSQL databases: 
  * MongoDB

Metacrafter key features:
* 111 labeling rules
* all labels metadata collected into [Metacrafter registry](https://github.com/apicrafter/metacrafter-registry ) public repository
* 312 date detection rules/patterns, date detection using [qddate](https://github.com/ivbeg/qddate), "quick and dirty" date detection library
* extendable set of rules using PyParsing, exact text match and validation functions
* support any database supported by SQLAlchemy
* advanced context and language management. You could apply only rules relevant to certain data of choosen language
* built-in API server
* **LLM-based classification** using Retrieval-Augmented Generation (RAG) with support for multiple providers (OpenAI, OpenRouter, Ollama, LM Studio, Perplexity)
* **Hybrid classification** combining rule-based and LLM-based approaches
* DataHub integration for exporting scan results to metadata catalogs
* commercial support and additional rules available


## Command line examples

### File analysis examples (CLI)

#### Basic file scanning

Basic CSV scan with a human‑readable table:

```bash
metacrafter scan file somefile.csv --format short
```

JSON Lines scan with machine‑readable JSON output:

```bash
metacrafter scan file somefile.jsonl \
  --format full \
  --output-format json \
  --stdout \
  --pretty
```

#### CSV/TSV files

CSV scan with a custom delimiter and encoding:

```bash
metacrafter scan file somefile.csv \
  --format short \
  --encoding windows-1251 \
  --delimiter ';'
```

TSV (tab-separated) file scan:

```bash
metacrafter scan file data.tsv \
  --delimiter '\t' \
  --format full \
  -o results.json
```

#### JSON formats

JSON array file:

```bash
metacrafter scan file data.json \
  --format full \
  --output-format json \
  -o results.json
```

JSON Lines with PII detection:

```bash
metacrafter scan file users.jsonl \
  --contexts pii \
  --langs en \
  --confidence 20.0 \
  --format full
```

#### Binary formats

Parquet file scan:

```bash
metacrafter scan file data.parquet \
  --format full \
  --output-format json \
  -o parquet_results.json
```

Excel file scan (XLSX):

```bash
metacrafter scan file spreadsheet.xlsx \
  --format full \
  --limit 500 \
  -o excel_results.json
```

BSON file scan:

```bash
metacrafter scan file data.bson \
  --format full \
  --output-format json \
  -o bson_results.json
```

#### Compressed files

Gzip-compressed CSV (auto-detected):

```bash
metacrafter scan file data.csv.gz \
  --format full \
  -o results.json
```

Bzip2-compressed JSONL:

```bash
metacrafter scan file data.jsonl.bz2 \
  --compression bz2 \
  --format full \
  -o results.json
```

ZIP archive containing CSV:

```bash
metacrafter scan file archive.zip \
  --compression zip \
  --format full \
  -o results.json
```

#### XML files

XML file with custom tag name:

```bash
metacrafter scan file data.xml \
  --tagname "record" \
  --format full \
  -o xml_results.json
```

#### Statistics only

CSV scan with statistics only (no classification), written to file:

```bash
metacrafter scan file somefile.csv \
  --stats-only \
  --output-format json \
  -o somefile_stats.json
```

#### Advanced file scanning options

Scan with specific field filters and confidence threshold:

```bash
metacrafter scan file users.csv \
  --fields email,phone,name \
  --confidence 50.0 \
  --contexts pii \
  --format full \
  -o filtered_results.json
```

Scan with custom empty values:

```bash
metacrafter scan file data.csv \
  --empty-values "N/A,NA,NULL,empty" \
  --format full
```

#### Output format examples

**Table output (`--format full`):**

```
key               ftype    tags    matches                                                                datatype_url
----------------  -------  ------  ---------------------------------------------------------------------  ----------------------------------------------------------
Domain            str              fqdn 99.90                                                             https://registry.apicrafter.io/datatype/fqdn
Primary domain    str              fqdn 100.00                                                            https://registry.apicrafter.io/datatype/fqdn
Name              str              name 100.00                                                            https://registry.apicrafter.io/datatype/name
Domain type       str      dict
Organization      str
Status            str      dict
Region            str      dict    rusregion 22.95                                                        https://registry.apicrafter.io/datatype/rusregion
GovSystem         str      dict
HTTP Support      str      dict    boolean 100.00                                                         https://registry.apicrafter.io/datatype/boolean
HTTPS Support     str      dict    boolean 100.00                                                         https://registry.apicrafter.io/datatype/boolean
Statuscode        str      dict
Is archived       str      empty
Archives          str      empty
Archive priority  str      dict
Archive Strategy  str      dict
ASN               str              asn 93.77                                                              https://registry.apicrafter.io/datatype/asn
ASN Country code  str      dict    countrycode_alpha2 100.00,countrycode_alpha2 100.00,languagetag 99.56  https://registry.apicrafter.io/datatype/countrycode_alpha2
IPs               str              ipv4 96.28                                                             https://registry.apicrafter.io/datatype/ipv4
GovType           str      dict
```

**JSON output example:**

```json
{
  "results": [
    [
      "email",
      "str",
      "",
      "email 98.50",
      "https://registry.apicrafter.io/datatype/email"
    ],
    [
      "phone",
      "str",
      "",
      "phone 95.20",
      "https://registry.apicrafter.io/datatype/phone"
    ]
  ],
  "data": [
    {
      "field": "email",
      "matches": [
        {
          "ruleid": "email",
          "dataclass": "email",
          "confidence": 98.5,
          "ruletype": "data",
          "format": null,
          "classurl": "https://registry.apicrafter.io/datatype/email"
        }
      ],
      "tags": [],
      "ftype": "str",
      "datatype_url": "https://registry.apicrafter.io/datatype/email",
      "stats": {
        "key": "email",
        "ftype": "str",
        "is_dictkey": false,
        "is_uniq": true,
        "n_uniq": 100,
        "share_uniq": 100.0,
        "minlen": 10,
        "maxlen": 50,
        "avglen": 25.5,
        "tags": [],
        "has_digit": 0,
        "has_alphas": 1,
        "has_special": 1,
        "dictvalues": null
      }
    }
  ]
}
```

**CSV output example:**

```csv
key,ftype,tags,matches,datatype_url
email,str,,email 98.50,https://registry.apicrafter.io/datatype/email
phone,str,,phone 95.20,https://registry.apicrafter.io/datatype/phone
name,str,,name 100.00,https://registry.apicrafter.io/datatype/name
```

**Database scan JSON output (multiple tables):**

```json
[
  {
    "table": "users",
    "results": [
      ["email", "str", "", "email 98.50", "https://registry.apicrafter.io/datatype/email"],
      ["phone", "str", "", "phone 95.20", "https://registry.apicrafter.io/datatype/phone"]
    ],
    "fields": [
      {
        "field": "email",
        "matches": [...],
        "tags": [],
        "ftype": "str",
        "stats": {...}
      }
    ],
    "stats": {...}
  },
  {
    "table": "orders",
    "results": [...],
    "fields": [...],
    "stats": {...}
  }
]
```


### Database analysis examples (CLI)

#### SQL databases

**PostgreSQL** - Scan all schemas:

```bash
metacrafter scan sql "postgresql+psycopg2://username:password@127.0.0.1:15432/dbname" \
  --format short \
  --output-format json \
  --stdout
```

**PostgreSQL** - Scan a single schema (`public`) and write a CSV summary:

```bash
metacrafter scan sql "postgresql+psycopg2://username:password@127.0.0.1:15432/dbname" \
  --schema public \
  --format full \
  --output-format csv \
  -o db_results.csv
```

**SQLite** - Scan local database file:

```bash
metacrafter scan sql "sqlite:///path/to/database.db" \
  --format full \
  --output-format json \
  -o sqlite_results.json
```

**SQLite** - Scan with PII detection:

```bash
metacrafter scan sql "sqlite:///users.db" \
  --contexts pii \
  --langs en \
  --confidence 20.0 \
  --format full \
  -o pii_scan.json
```

**MySQL/MariaDB**:

```bash
metacrafter scan sql "mysql+pymysql://user:password@localhost:3306/dbname" \
  --format full \
  --output-format json \
  -o mysql_results.json
```

**DuckDB** (requires `duckdb-engine`):

```bash
metacrafter scan sql "duckdb:///path/to/database.duckdb" \
  --format full \
  --output-format json \
  -o duckdb_results.json
```

**SQL Server**:

```bash
metacrafter scan sql "mssql+pyodbc://user:password@server/dbname?driver=ODBC+Driver+17+for+SQL+Server" \
  --format full \
  --output-format json \
  -o sqlserver_results.json
```

**Oracle**:

```bash
metacrafter scan sql "oracle+cx_oracle://user:password@host:1521/service_name" \
  --format full \
  --output-format json \
  -o oracle_results.json
```

#### MongoDB

Scan MongoDB database:

```bash
metacrafter scan mongodb localhost \
  --port 27017 \
  --dbname mydatabase \
  --output-format json \
  -o mongodb_results.json
```

Scan MongoDB with authentication:

```bash
metacrafter scan mongodb localhost \
  --port 27017 \
  --dbname mydatabase \
  --username admin \
  --password secret \
  --format full \
  -o mongodb_results.json
```

Scan MongoDB using connection URI:

```bash
metacrafter scan mongodb "mongodb://user:pass@host1:27017,host2:27017/dbname?replicaSet=rs0" \
  --format full \
  -o mongodb_results.json
```

#### Advanced database scanning options

Scan with batch processing and progress bar:

```bash
metacrafter scan sql "postgresql://user:pass@localhost/db" \
  --batch-size 1000 \
  --progress \
  --format full \
  -o results.json
```

Scan specific fields only:

```bash
metacrafter scan sql "sqlite:///data.db" \
  --fields email,phone,name,address \
  --format full \
  -o filtered_results.json
```

Scan with statistics only:

```bash
metacrafter scan sql "postgresql://user:pass@localhost/db" \
  --stats-only \
  --output-format csv \
  -o stats_only.csv
```

Scan all supported files in a directory tree:

```bash
metacrafter scan bulk /path/to/data \
  --limit 200 \
  --output-format json \
  -o bulk_results.json
```

### Rules Inspection

Inspect and manage classification rules:

**List all rules:**
```bash
# List all rules in table format
metacrafter rules list

# List rules in JSON format
metacrafter rules list --output-format json -o rules.json

# List rules filtered by country codes
metacrafter rules list --country-codes us,ca --output-format csv -o us_ca_rules.csv

# List rules from custom rule path
metacrafter rules list --rulepath ./custom_rules --output-format yaml
```

**Show rule statistics:**
```bash
# Display aggregate statistics about loaded rules
metacrafter rules stats

# Statistics with custom rule path
metacrafter rules stats --rulepath ./custom_rules

# Statistics filtered by country codes
metacrafter rules stats --country-codes ru,de
```

The `rules list` command displays all field rules, data rules, and date/time patterns with their metadata including:
- Rule ID and name
- Type (field or data)
- Match method (text, ppr, func)
- Language and country codes
- Contexts (e.g., pii, finance)
- PII flag, priority, and length constraints

The `rules stats` command shows aggregate counts of:
- Field-based rules
- Data-based rules
- Rules by context
- Rules by language
- Rules by country code
- Date/time patterns

### Exporting to DataHub

Export scan results to DataHub metadata catalog:

```bash
# First, scan a file and save results
metacrafter scan file users.csv --format json -o results.json

# Then export to DataHub
metacrafter export datahub results.json \
  --dataset-urn "urn:li:dataset:(urn:li:dataPlatform:postgres,users,PROD)" \
  --datahub-url "http://localhost:8080" \
  --token "your-token" \
  --min-confidence 50.0
```

With configuration file (`.metacrafter`):

```bash
metacrafter export datahub results.json \
  --dataset-urn "urn:li:dataset:(urn:li:dataPlatform:postgres,users,PROD)"
```

### Server mode and remote scanning

Launch the local API server:

```bash
metacrafter server run --host 127.0.0.1 --port 10399
```

Use the server from the CLI to scan a CSV file remotely:

```bash
metacrafter scan file somefile.csv \
  --format full \
  --remote http://127.0.0.1:10399 \
  --output-format json \
  --stdout
```

### LLM-based classification examples

**LLM-only classification with OpenAI:**
```bash
metacrafter scan file data.csv \
  --classification-mode llm \
  --llm-provider openai \
  --llm-model gpt-4o-mini \
  --llm-api-key "sk-..." \
  --format full
```

**Hybrid classification (rules + LLM fallback):**
```bash
metacrafter scan file data.csv \
  --classification-mode hybrid \
  --llm-provider openai \
  --llm-api-key "sk-..." \
  --llm-min-confidence 60.0 \
  --format full
```

**Using Ollama (local LLM):**
```bash
metacrafter scan file data.csv \
  --llm-only \
  --llm-provider ollama \
  --llm-base-url "http://localhost:11434" \
  --llm-model "llama3" \
  --format full
```

**Using OpenRouter:**
```bash
metacrafter scan file data.csv \
  --classification-mode llm \
  --llm-provider openrouter \
  --llm-model "openai/gpt-4o-mini" \
  --llm-api-key "sk-or-..." \
  --format full
```

**Using LM Studio (local):**
```bash
metacrafter scan file data.csv \
  --llm-only \
  --llm-provider lmstudio \
  --llm-base-url "http://localhost:1234/v1" \
  --llm-model "local-model" \
  --format full
```

### Advanced CLI options (selection)

All `scan` commands share a rich set of options. Some commonly used ones:

- `--contexts` / `--langs`: filter rules by context and language (comma‑separated).
- `--confidence`, `-c`: minimum confidence threshold for a match.
- `--stop-on-match`: stop after the first matching rule per field.
- `--no-dates`: disable automatic date/time pattern detection.
- `--include-imprecise`: include imprecise rules that are ignored by default.
- `--include-empty`: include empty values in statistics and confidence.
- `--fields`: process only specific fields (comma‑separated).
- `--output-format`: `table`, `json`, `yaml`, or `csv`.
- `--stdout`, `--pretty`, `--indent`: control where and how JSON/YAML is written.
- `--rulepath`: override default rule paths with your own YAML rule directories.
- `--country-codes`: restrict rules to specific ISO country codes.
- `--classification-mode`: set classification mode (`rules`, `llm`, or `hybrid`).
- `--llm-provider`: LLM provider (`openai`, `openrouter`, `ollama`, `lmstudio`, `perplexity`).
- `--llm-model`: model name for the selected provider.
- `--llm-api-key`: API key for cloud providers.
- `--llm-base-url`: base URL for local providers (Ollama, LM Studio).

**Rules commands:**
- `metacrafter rules list`: List all loaded rules with metadata
- `metacrafter rules stats`: Display aggregate statistics about loaded rules

Run `metacrafter --help`, `metacrafter scan file --help`, `metacrafter rules list --help`, etc. for the full list.

## LLM-Based Classification

Metacrafter now supports LLM-based classification using Retrieval-Augmented Generation (RAG) to identify semantic data types. This feature uses vector embeddings and similarity search to provide context-aware classification.

### Features

- **Multiple LLM Providers**: Support for OpenAI, OpenRouter, Ollama, LM Studio, and Perplexity
- **RAG Architecture**: Uses vector embeddings (ChromaDB) and similarity search to retrieve relevant registry entries
- **Three Classification Modes**:
  - **Rules-only** (default): Traditional rule-based classification
  - **LLM-only**: Use only LLM classification, skipping rule-based matching
  - **Hybrid**: Rule-based first, with LLM as fallback for unmatched or low-confidence fields
- **Automatic Index Building**: Vector index is automatically built from registry on first use
- **Configurable Confidence Thresholds**: Set minimum confidence for LLM results

### Installation

LLM features require additional dependencies:

```bash
pip install openai chromadb requests
```

### Quick Start

**LLM-only classification:**
```bash
metacrafter scan file data.csv \
  --classification-mode llm \
  --llm-provider openai \
  --llm-api-key "sk-..." \
  --format full
```

**Hybrid classification (rules + LLM fallback):**
```bash
metacrafter scan file data.csv \
  --classification-mode hybrid \
  --llm-provider openai \
  --llm-api-key "sk-..." \
  --llm-min-confidence 50.0 \
  --format full
```

**Using Ollama (local LLM):**
```bash
metacrafter scan file data.csv \
  --classification-mode llm \
  --llm-provider ollama \
  --llm-base-url "http://localhost:11434" \
  --llm-model "llama3" \
  --format full
```

### Supported LLM Providers

| Provider | Model Examples | API Key Required | Base URL |
|----------|---------------|------------------|----------|
| **OpenAI** | gpt-4o-mini, gpt-4, gpt-3.5-turbo | Yes (OPENAI_API_KEY) | https://api.openai.com/v1 |
| **OpenRouter** | openai/gpt-4o-mini, anthropic/claude-3-haiku | Yes (OPENROUTER_API_KEY) | https://openrouter.ai/api/v1 |
| **Ollama** | llama3, mistral, codellama | No | http://localhost:11434 |
| **LM Studio** | Any local model | No | http://localhost:1234/v1 |
| **Perplexity** | llama-3.1-sonar-small-128k-online | Yes (PERPLEXITY_API_KEY) | https://api.perplexity.ai |

### Configuration

Add LLM settings to your `.metacrafter` config file:

```yaml
rulepath:
  - ./rules

# LLM Configuration
classification_mode: hybrid  # rules, llm, or hybrid
llm_provider: openai
llm_model: gpt-4o-mini
llm_registry_path: ../metacrafter-registry/data/datatypes_latest.jsonl
llm_index_path: ./llm_index
llm_api_key: sk-...  # Or use OPENAI_API_KEY env var
llm_min_confidence: 50.0
```

**Environment Variables:**
- `OPENAI_API_KEY`: OpenAI API key (for OpenAI provider and embeddings)
- `OPENROUTER_API_KEY`: OpenRouter API key
- `PERPLEXITY_API_KEY`: Perplexity API key

### CLI Options

**LLM-related options:**
- `--classification-mode`: Set classification mode (`rules`, `llm`, or `hybrid`)
- `--llm-only`: Use LLM-only mode (shorthand for `--classification-mode llm`)
- `--use-llm`: Enable LLM in hybrid mode (shorthand for `--classification-mode hybrid`)
- `--llm-provider`: LLM provider (`openai`, `openrouter`, `ollama`, `lmstudio`, `perplexity`)
- `--llm-model`: Model name (provider-specific)
- `--llm-api-key`: API key for the provider
- `--llm-base-url`: Base URL (for Ollama, LM Studio, or custom endpoints)
- `--llm-registry-path`: Path to registry JSONL file
- `--llm-index-path`: Path to vector index directory
- `--llm-min-confidence`: Minimum confidence threshold (0-100, default: 50.0)

### Python API

**LLM-only classification:**
```python
from metacrafter.core import CrafterCmd

cmd = CrafterCmd(
    llm_only=True,
    llm_provider="openai",
    llm_api_key="sk-...",
    llm_registry_path="../registry/data/datatypes_latest.jsonl"
)

report = cmd.scan_data(
    items=[{"email": "test@example.com", "unknown_field": "xyz123"}],
    classification_mode="llm"
)
```

**Hybrid classification:**
```python
from metacrafter.core import CrafterCmd

cmd = CrafterCmd(
    use_llm=True,
    llm_provider="openai",
    llm_api_key="sk-...",
    llm_min_confidence=60.0
)

report = cmd.scan_data(
    items=[{"email": "test@example.com", "unknown_field": "xyz123"}],
    classification_mode="hybrid"
)
```

**Using Ollama:**
```python
from metacrafter.core import CrafterCmd

cmd = CrafterCmd(
    llm_only=True,
    llm_provider="ollama",
    llm_base_url="http://localhost:11434",
    llm_model="llama3"
)

report = cmd.scan_data(
    items=[{"email": "test@example.com"}],
    classification_mode="llm"
)
```

### How It Works

1. **Index Building**: On first use, Metacrafter loads the registry and creates vector embeddings for all datatypes using OpenAI's embedding API
2. **Query Embedding**: For each field, the field name and sample values are embedded
3. **Vector Search**: Similar registry entries are retrieved using ChromaDB
4. **LLM Classification**: The LLM receives a prompt with the field context and retrieved registry entries
5. **Result Formatting**: LLM results are converted to Metacrafter-compatible format

### Performance Considerations

- **Index Building**: One-time cost (several minutes for large registries)
- **Classification**: Each field requires one LLM API call (~1-3 seconds depending on provider)
- **Cost**: Depends on LLM provider and model (OpenAI charges per token)
- **Caching**: Vector index is persisted to disk and reused across sessions

### Limitations

- Requires internet connection for cloud providers (OpenAI, OpenRouter, Perplexity)
- Local providers (Ollama, LM Studio) require the service to be running
- API costs apply for cloud providers
- Index must be rebuilt when registry updates

## Configuration

Metacrafter can be configured using a `.metacrafter` configuration file. The configuration file is a YAML file that can be placed in:
- The current working directory (`.metacrafter`)
- Your home directory (`~/.metacrafter`)

### Configuration file format

```yaml
rulepath:
  - ./rules
  - ./custom_rules
  - /path/to/additional/rules

# LLM Configuration (optional)
classification_mode: hybrid  # rules, llm, or hybrid
llm_provider: openai
llm_model: gpt-4o-mini
llm_registry_path: ../metacrafter-registry/data/datatypes_latest.jsonl
llm_index_path: ./llm_index
llm_api_key: sk-...  # Or use environment variable
llm_min_confidence: 50.0
```

The `rulepath` option specifies a list of directories where Metacrafter should look for rule YAML files. If not specified, it defaults to `["rules"]`.

You can also override the rule path using the `--rulepath` command-line option.

### DataHub Integration Configuration

Metacrafter can be configured to export scan results to DataHub. Add the following to your `.metacrafter` config file:

```yaml
rulepath:
  - ./rules

datahub:
  url: "http://localhost:8080"
  token: "your-authentication-token"
```

Alternatively, you can use environment variables:
- `DATAHUB_URL`: DataHub GMS server URL
- `DATAHUB_TOKEN`: DataHub authentication token

## DataHub Integration

Metacrafter can export scan results to [DataHub](https://datahubproject.io), a popular metadata catalog. This allows you to automatically tag dataset columns with PII labels, datatypes, and other classification metadata.

### Installation

To use the DataHub integration, install the DataHub Python SDK:

```bash
pip install 'acryl-datahub[datahub-rest]'
```

### Exporting Scan Results to DataHub

1. **Scan a file and save results:**
   ```bash
   metacrafter scan file users.csv --format json -o results.json
   ```

2. **Export to DataHub:**
   ```bash
   metacrafter export datahub results.json \
     --dataset-urn "urn:li:dataset:(urn:li:dataPlatform:postgres,users,PROD)" \
     --datahub-url "http://localhost:8080" \
     --token "your-token"
   ```

### Export Options

- `--dataset-urn`: DataHub dataset URN (required)
- `--datahub-url`: DataHub GMS server URL (or use `DATAHUB_URL` env var)
- `--token`: Authentication token (or use `DATAHUB_TOKEN` env var)
- `--add-pii-tags`: Add PII tags to fields (default: true)
- `--add-datatype-tags`: Add datatype tags (default: true)
- `--link-glossary-terms`: Link glossary terms (default: true)
- `--add-properties`: Add custom properties (default: true)
- `--min-confidence`: Minimum confidence threshold 0-100 (default: 0.0)
- `--replace`: Replace existing metadata instead of merging (default: false)

### What Gets Exported

Metacrafter exports the following metadata to DataHub:

- **Tags**: PII tags and datatype tags (e.g., "PII", "Email", "Phone")
- **Glossary Terms**: Links to glossary terms for detected datatypes
- **Custom Properties**:
  - `metacrafter_confidence`: Confidence score (0-100)
  - `metacrafter_datatype`: Detected datatype name
  - `metacrafter_datatype_url`: Link to registry entry
  - `metacrafter_rule_id`: Rule that matched
  - `metacrafter_field_type`: Field data type (str, int, etc.)

### Example Workflow

```bash
# 1. Scan your data
metacrafter scan file users.csv \
  --contexts pii \
  --format json \
  -o scan_results.json

# 2. Export to DataHub
metacrafter export datahub scan_results.json \
  --dataset-urn "urn:li:dataset:(urn:li:dataPlatform:postgres,users,PROD)" \
  --datahub-url "http://datahub.example.com:8080" \
  --min-confidence 50.0
```

For more details, see the [DataHub Integration Documentation](devdocs/ISSUE_24_IMPLEMENTATION.md).

### OpenMetadata Integration

Metacrafter can export scan results to OpenMetadata metadata catalog, adding tags, glossary terms, and custom properties to table columns.

**Installation:**
```bash
pip install openmetadata-ingestion
```

**Usage:**
```bash
# 1. Scan your data
metacrafter scan file users.csv \
  --contexts pii \
  --format json \
  -o scan_results.json

# 2. Export to OpenMetadata
metacrafter export openmetadata scan_results.json \
  --table-fqn "postgres.default.public.users" \
  --openmetadata-url "http://localhost:8585/api" \
  --min-confidence 50.0
```

**Configuration:**
Create `.metacrafter` file:
```yaml
openmetadata:
  url: "http://localhost:8585/api"
  token: "your-jwt-token"
```

**What Gets Exported:**
- **Tags**: PII tags and datatype tags (e.g., "PII", "Email", "Phone")
- **Glossary Terms**: Links to glossary terms for detected datatypes
- **Custom Properties**:
  - `metacrafter_confidence`: Confidence score (0-100)
  - `metacrafter_datatype`: Detected datatype name
  - `metacrafter_datatype_url`: Link to registry entry
  - `metacrafter_rule_id`: Rule that matched
  - `metacrafter_field_type`: Field data type (str, int, etc.)

For more details, see the [OpenMetadata Integration Documentation](devdocs/ISSUE_OPENMETADATA_IMPLEMENTATION.md).

## Python API examples

Metacrafter can also be used as a Python library.

### Scan in‑memory records (list of dicts)

```python
from metacrafter.core import CrafterCmd

# Example in‑memory data (e.g. loaded from your own sources)
items = [
    {"email": "alice@example.com", "full_name": "Alice Example"},
    {"email": "bob@example.com", "full_name": "Bob Example"},
]

cmd = CrafterCmd()

report = cmd.scan_data(
    items,
    limit=100,
    contexts="pii",        # optional: restrict to PII‑related rules
    langs="en",            # optional: restrict to English rules
    confidence=20.0,       # minimum confidence threshold
    stop_on_match=False,   # consider multiple matches per field
)

# High‑level table‑like summary
for row in report["results"]:
    field, ftype, tags, matches, datatype_url = row
    print(field, "=>", matches, "(", datatype_url, ")")

# Detailed per‑field metadata and matches
for field_info in report["data"]:
    print(field_info["field"], field_info["matches"])
```

### Scan a file programmatically

**Basic file scan:**

```python
from metacrafter.core import CrafterCmd

cmd = CrafterCmd()

cmd.scan_file(
    filename="somefile.csv",
    delimiter=",",
    encoding="utf8",
    limit=500,
    contexts="pii",
    langs="en",
    dformat="short",
    output="results.json",
    output_format="json",
)
```

**Scan Parquet file:**

```python
from metacrafter.core import CrafterCmd

cmd = CrafterCmd()

cmd.scan_file(
    filename="data.parquet",
    limit=1000,
    dformat="full",
    output="parquet_results.json",
    output_format="json",
)
```

**Scan compressed CSV:**

```python
from metacrafter.core import CrafterCmd

cmd = CrafterCmd()

cmd.scan_file(
    filename="data.csv.gz",
    compression="auto",  # or "gz" to force
    limit=500,
    output="compressed_results.json",
    output_format="json",
)
```

**Scan Excel file:**

```python
from metacrafter.core import CrafterCmd

cmd = CrafterCmd()

cmd.scan_file(
    filename="spreadsheet.xlsx",
    limit=1000,
    dformat="full",
    output="excel_results.json",
    output_format="json",
)
```

**Scan XML file:**

```python
from metacrafter.core import CrafterCmd

cmd = CrafterCmd()

cmd.scan_file(
    filename="data.xml",
    tagname="record",  # XML tag containing data records
    limit=500,
    output="xml_results.json",
    output_format="json",
)
```

**Get statistics only:**

```python
from metacrafter.core import CrafterCmd

cmd = CrafterCmd()

cmd.scan_file(
    filename="data.csv",
    stats_only=True,
    output="stats.json",
    output_format="json",
)
```

### Scan a database programmatically

**Scan SQLite database:**

```python
from metacrafter.core import CrafterCmd

cmd = CrafterCmd()

cmd.scan_db(
    connectstr="sqlite:///path/to/database.db",
    limit=1000,
    dformat="full",
    output="db_results.json",
    output_format="json",
)
```

**Scan PostgreSQL database:**

```python
from metacrafter.core import CrafterCmd

cmd = CrafterCmd()

cmd.scan_db(
    connectstr="postgresql+psycopg2://user:password@localhost:5432/dbname",
    schema="public",  # Optional: specific schema
    limit=1000,
    batch_size=500,  # Rows per batch
    dformat="full",
    output="postgres_results.json",
    output_format="json",
)
```

**Scan MongoDB database:**

```python
from metacrafter.core import CrafterCmd

cmd = CrafterCmd()

cmd.scan_mongodb(
    host="localhost",
    port=27017,
    dbname="mydatabase",
    username="admin",  # Optional
    password="secret",  # Optional
    limit=1000,
    dformat="full",
    output="mongodb_results.json",
    output_format="json",
)
```

**Scan database with filters:**

```python
from metacrafter.core import CrafterCmd

cmd = CrafterCmd()

cmd.scan_db(
    connectstr="postgresql://user:pass@localhost/db",
    schema="public",
    contexts=["pii"],  # Only PII-related rules
    langs=["en"],     # Only English rules
    confidence=20.0,  # Minimum confidence threshold
    fields=["email", "phone", "name"],  # Specific fields only
    dformat="full",
    output="filtered_results.json",
    output_format="json",
)
```

**Get database scan results as Python dict:**

```python
from metacrafter.core import CrafterCmd

cmd = CrafterCmd()

# When output=None, scan_db returns None (writes to stdout)
# To get results programmatically, use scan_data() with data from database
import sqlite3

conn = sqlite3.connect("database.db")
cursor = conn.cursor()
cursor.execute("SELECT * FROM users LIMIT 100")
rows = cursor.fetchall()
columns = [description[0] for description in cursor.description]
items = [dict(zip(columns, row)) for row in rows]

report = cmd.scan_data(
    items=items,
    limit=100,
    contexts="pii",
)

# Access results
for row in report["results"]:
    field, ftype, tags, matches, datatype_url = row
    print(f"{field}: {matches}")

for field_info in report["data"]:
    print(f"{field_info['field']}: {field_info['matches']}")
```

### Using custom rule paths or country filters

```python
from metacrafter.core import CrafterCmd

cmd = CrafterCmd(
    rulepath=["./rules", "./more_rules"],
    country_codes=["us", "ca"],   # restrict to North‑American rules
)

report = cmd.scan_data(
    items=[{"ssn": "123-45-6789"}],
    contexts="pii",
)
```

### Using LLM-based classification

**LLM-only classification:**
```python
from metacrafter.core import CrafterCmd

cmd = CrafterCmd(
    llm_only=True,
    llm_provider="openai",
    llm_api_key="sk-...",
    llm_registry_path="../metacrafter-registry/data/datatypes_latest.jsonl"
)

report = cmd.scan_data(
    items=[
        {"email": "test@example.com", "unknown_field": "xyz123"},
        {"phone": "555-1234", "mystery_field": "abc456"}
    ],
    classification_mode="llm"
)

# Access LLM classification results
for field_info in report["data"]:
    print(f"{field_info['field']}: {field_info['matches']}")
```

**Hybrid classification (rules + LLM fallback):**
```python
from metacrafter.core import CrafterCmd

cmd = CrafterCmd(
    use_llm=True,
    llm_provider="openai",
    llm_api_key="sk-...",
    llm_min_confidence=60.0
)

report = cmd.scan_data(
    items=[
        {"email": "test@example.com", "unknown_field": "xyz123"}
    ],
    classification_mode="hybrid"
)
```

**Using Ollama (local LLM):**
```python
from metacrafter.core import CrafterCmd

cmd = CrafterCmd(
    llm_only=True,
    llm_provider="ollama",
    llm_base_url="http://localhost:11434",
    llm_model="llama3"
)

report = cmd.scan_data(
    items=[{"email": "test@example.com"}],
    classification_mode="llm"
)
```

**Using OpenRouter:**
```python
from metacrafter.core import CrafterCmd

cmd = CrafterCmd(
    llm_only=True,
    llm_provider="openrouter",
    llm_model="openai/gpt-4o-mini",
    llm_api_key="sk-or-..."
)

report = cmd.scan_data(
    items=[{"email": "test@example.com"}],
    classification_mode="llm"
)
```

### Exporting to DataHub programmatically

```python
from metacrafter.core import CrafterCmd
from metacrafter.integrations.datahub import DataHubExporter

# Scan a file
cmd = CrafterCmd()
report = cmd.scan_file(
    filename="users.csv",
    contexts="pii",
    output_format="json"
)

# Export to DataHub
exporter = DataHubExporter(
    datahub_url="http://localhost:8080",
    token="your-token"
)

stats = exporter.export_scan_results(
    dataset_urn="urn:li:dataset:(urn:li:dataPlatform:postgres,users,PROD)",
    scan_report=report,
    min_confidence=50.0,
    add_pii_tags=True,
    add_datatype_tags=True,
    link_glossary_terms=True,
    add_properties=True,
)

print(f"Exported {stats['fields_processed']} fields")
print(f"Added {stats['tags_added']} tags")
print(f"Linked {stats['glossary_terms_linked']} glossary terms")
```

# Rules

All rules are described as YAML files. By default, rules are loaded from the `rules` directory or from a list of directories specified in the `.metacrafter` configuration file (see [Configuration](#configuration) section above).


All rules could be applied to **fields** or **data** .

Compare engines defined in **match** parameter in rule description:
* text - scan text for exact match to one of text values. Text values delimited by comma (',')
* ppr - scan text for PyParsing. PyParsing rule defined as Python code with PyParsing objects like Word(nums, exact=4)
* func - scan text using Python function provided. Function shoud accept one string parameter and shoud return True or False

## How to write rules

### Function (func)

Example Russian administrative legal act/law matched by custom function
```
  runpabyfunc:
    key: runpa
    name: Russian legal act / law
    maxlen: 500
    minlen: 3
    priority: 1
    match: func
    type: data
    rule: metacrafter.rules.ru.gov.is_ru_law
```

### Exact text match (text)

Example midname matching by exact field name
```
  midname:
    key: person_midname
    name: Person midname by known
    rule: midname,secondname,middlename,mid_name,middle_name
    type: field
    match: text
```
### PyParsing rule (ppr)

Example Russian cadastral number
```
  rukadastr:
    key: rukadastr
    name: Russian land territory cadastral identifier
    rule: Word(nums, min=1, max=2) + Literal(':').suppress() + Word(nums, min=1, max=2) + Literal(':').suppress() + Word(nums, min=6, max=7) + Literal(':').suppress() + Word(nums, min=1, max=6)
    maxlen: 20
    minlen: 12
    priority: 1
    match: ppr
    type: data
```


## Commercial support

Please write ibegtin@apicrafter.io or ivan@begtin.tech to request beta access to commercial API.
Commercial API support 195 fields and data rules and provided with dedicated support.
