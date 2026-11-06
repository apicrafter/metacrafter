---
title: "Report format"
description: "Scan report version 3, output formats and crosswalk exports"
---
# Report format

Every scan returns a report with `report_version: 3`. The JSON Schema is published at
[`/schema/report-v3.schema.json`](pathname:///schema/report-v3.schema.json) and generated from the
models in `metacrafter.report.models`.

## Structure

A single source (file, table, in-memory data):

```json
{
  "report_version": 3,
  "tool": {"name": "metacrafter", "version": "0.1.0"},
  "source": "users.csv",
  "complete": true,
  "errors": [],
  "registry": {"source": "cache:…", "base_url": "https://registry.apicrafter.io", "version": "1.3.0.dev0"},
  "scoring_model": "v1",
  "registry_detectors": {"mode": "stable", "count": 99},
  "sampling": {"method": "head", "retained_count": 1000},
  "data": [
    {
      "field": "contact_email",
      "ftype": "str",
      "datatype_url": "https://registry.apicrafter.io/datatype/email",
      "pii": {"is_pii": true, "datatypes": ["email"]},
      "matches": [
        {
          "ruleid": "emailbyvalue",
          "dataclass": "email",
          "datatype": "email",
          "rule_key": "email",
          "confidence": 100.0,
          "ruletype": "data",
          "is_pii": true,
          "pii_source": "registry",
          "classurl": "https://registry.apicrafter.io/datatype/email",
          "evidence": {"scoring_model": "v1", "hit_rate": 1.0, "matched_count": 2, "considered_count": 2},
          "registry": {"name": "Email address", "status": "stable", "categories": ["common", "internet", "pii"],
                       "mappings": {"presidio": "EMAIL_ADDRESS", "google_dlp": "EMAIL_ADDRESS"}}
        }
      ],
      "stats": {"…": "…"}
    }
  ]
}
```

Several sources (SQL, MongoDB, bulk) are wrapped in
`{"report_version": 3, "sources": [...], "complete": ..., "errors": [...]}`.

SQL scans add the table's keys to each key column: `"database": {"primary_key": true,
"autoincrement": true}` or `"database": {"foreign_key": "main.parent.id"}`, the tags
`primary_key`, `autoincrement` and `foreign_key`, and for a database-generated key a match
`{"ruleid": "_db_autoincrement_key", "dataclass": "id", "ruletype": "schema", "confidence": 100}`.

Version 3 is a superset of version 2: `data` and `fields` hold the same list, and readers
of version 2 keep working. `metacrafter.reports.normalize_report` accepts versions 1, 2
and 3.

## Output formats

| `--output-format` | Output |
|---|---|
| `table` | Terminal table (default) |
| `json`, `yaml` | Report version 3 |
| `md` | Markdown: one table per source with registry links and a PII summary |
| `legacy` | Pre-1.0 JSON (`results`, `data`, `stats`, `stats_table`; matches without registry fields) |
| `csv` | Summary rows |

JSON, YAML and Markdown output omit raw values from the scanned data (dictionary values and
string minimum/maximum values) and set `samples_included: false`. Pass `--include-samples`
to keep them. The Python API (`CrafterCmd.scan_data`) always returns full statistics.

## Crosswalk exports

Registry records map datatypes to other classification systems. Export the labels of a
saved report:

```bash
metacrafter scan file users.csv --output-format json -o report.json
metacrafter export labels report.json --system presidio
metacrafter export labels report.json --system google_dlp --min-confidence 80
```

Systems: `presidio`, `google_dlp`, `dpv`, `purview`, `schema_org`, `wikidata`. Matches
without a mapping are listed under `unmapped`.

## Frictionless Table Schema

```bash
metacrafter export frictionless report.json -o datapackage.json
```

Writes a Data Package with one resource per source. Each field gets `rdfType` (from the
Schema.org or Wikidata mapping), `metacrafter:datatype`, `metacrafter:url`,
`metacrafter:confidence` and `metacrafter:pii`.

## Catalog exports

DataHub, OpenMetadata and Atlas exports read PII, datatype ids and names from the report.
They tag a field as PII when a match has `is_pii: true` (from the rule or the registry),
use the canonical registry id for datatype tags and glossary terms, and add the registry
name, status, categories and mappings as custom properties.
