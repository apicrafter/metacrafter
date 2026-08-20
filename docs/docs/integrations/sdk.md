---
title: "Python SDK"
description: "Use Metacrafter as a Python library"
---
# Python SDK

```python
from metacrafter import CrafterCmd
# or: from metacrafter.core import CrafterCmd
```

`CrafterCmd` is the same orchestrator the CLI uses.

## Scan in-memory records

```python
from metacrafter.core import CrafterCmd

items = [
    {"email": "alice@example.com", "full_name": "Alice Example"},
    {"email": "bob@example.com", "full_name": "Bob Example"},
]

cmd = CrafterCmd()

report = cmd.scan_data(
    items,
    limit=100,
    contexts="pii",
    langs="en",
    confidence=20.0,
    stop_on_match=False,
)

for row in report["results"]:
    field, ftype, tags, matches, datatype_url = row
    print(field, "=>", matches, "(", datatype_url, ")")

for field_info in report["data"]:
    print(field_info["field"], field_info["matches"])
```

## Scan a file

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

Parquet, gzip CSV, Excel, XML, and stats-only follow the same pattern (`compression`,
`tagname`, `stats_only`).

## Scan a database

```python
from metacrafter.core import CrafterCmd

cmd = CrafterCmd()
cmd.scan_db(
    connectstr="postgresql+psycopg2://user:password@localhost:5432/dbname",
    schema="public",
    limit=1000,
    batch_size=500,
    dformat="full",
    output="postgres_results.json",
    output_format="json",
)

cmd.scan_mongodb(
    host="localhost",
    port=27017,
    dbname="mydatabase",
    limit=1000,
    dformat="full",
    output="mongodb_results.json",
    output_format="json",
)
```

To keep results in Python, load rows yourself and call `scan_data()`.

## Custom rule paths

```python
cmd = CrafterCmd(
    rulepath=["./rules", "./more_rules"],
    country_codes=["us", "ca"],
)
report = cmd.scan_data(items=[{"ssn": "123-45-6789"}], contexts="pii")
```

## LLM classification

```python
cmd = CrafterCmd(
    use_llm=True,
    llm_provider="openai",
    llm_api_key="sk-...",
    llm_min_confidence=60.0,
)
report = cmd.scan_data(
    items=[{"email": "test@example.com", "unknown_field": "xyz123"}],
    classification_mode="hybrid",
)
```

See [LLM classification](/integrations/llm) for providers and [DataHub](/integrations/datahub)
for `DataHubExporter`.
