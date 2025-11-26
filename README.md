# Metacrafter

Python command line tool and python engine to label table fields and fields in data files.
It could help to find meaningful data in your tables and data files or to find Personal identifable information (PII).


## Installation

To install Python library use `pip install metacrafter` via pip or `python setup.py install` 

## Features

Metacrafter is a rule based tool that helps to label fields of the tables in databases. It scans table and finds person names, surnames, midnames, PII data, basic identifiers like UUID/GUID. 
These rules written as .yaml files and could be easily extended.

File formats supported:
* CSV (comma-separated values)
* TSV (tab-separated values)
* JSON Lines (.jsonl, .ndjson)
* JSON (array of records)
* BSON (Binary JSON)
* Parquet
* Avro
* ORC
* XML
* Excel (.xls, .xlsx)

Compression codecs supported:
* gzip (.gz)
* bzip2 (.bz2)
* xz (.xz)
* lz4 (.lz4)
* zstandard (.zst)
* Brotli (.br)
* Snappy

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

Basic CSV scan with a human‑readable table:

```bash
metacrafter scan file somefile.csv --format short
```

CSV scan with a custom delimiter and encoding:

```bash
metacrafter scan file somefile.csv \
  --format short \
  --encoding windows-1251 \
  --delimiter ';'
```

JSON Lines scan with machine‑readable JSON output:

```bash
metacrafter scan file somefile.jsonl \
  --format full \
  --output-format json \
  --stdout \
  --pretty
```

CSV scan with statistics only (no classification), written to file:

```bash
metacrafter scan file somefile.csv \
  --stats-only \
  --output-format json \
  -o somefile_stats.json
```

Result example of `--format full` table output:
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


### Database analysis examples (CLI)

Scan a PostgreSQL database using a SQLAlchemy connection string (all schemas):

```bash
metacrafter scan sql "postgresql+psycopg2://username:password@127.0.0.1:15432/dbname" \
  --format short \
  --output-format json \
  --stdout
```

Scan a single schema (`public`) and write a CSV summary:

```bash
metacrafter scan sql "postgresql+psycopg2://username:password@127.0.0.1:15432/dbname" \
  --schema public \
  --format full \
  --output-format csv \
  -o db_results.csv
```

Scan a MongoDB database:

```bash
metacrafter scan mongodb localhost \
  --port 27017 \
  --dbname fns \
  --output-format json \
  -o mongodb_results.json
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
