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

Run `metacrafter --help`, `metacrafter scan file --help`, etc. for the full list.

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
```

The `rulepath` option specifies a list of directories where Metacrafter should look for rule YAML files. If not specified, it defaults to `["rules"]`.

You can also override the rule path using the `--rulepath` command-line option.

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
