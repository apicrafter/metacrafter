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
  "registry_detectors": {"mode": "checksum", "count": 11},
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

### PII flags of secondary matches

Matches are ordered by confidence, then field-name evidence. Values of one datatype can be
valid values of another: OKPD2 codes such as `21.20.10.226` are also IPv4 addresses. A
value match of a PII datatype without field-name evidence does not make the field PII when
another value match:

- has field-name evidence (`evidence.name_score` above 0),
- is not PII,
- is a different datatype that is not the PII datatype's registry parent or child,
- has the same or a higher confidence.

The match stays in `matches` with `is_pii: false`, `pii_source: null` and
`pii_outranked_by` set to the datatype that outranks it; `registry.is_pii` keeps the
datatype's own flag. `--pii-only` drops such matches. A field `okpd2` with OKPD2 codes
reports `okpd` first, then `ipv4` with `"pii_outranked_by": "okpd"`, and its PII summary is
false. A field-name-only match (`ruletype: field`) never outranks a value match: a field
`login` with email values stays PII.

### Field groups

`field_groups` lists sibling fields (same parent path) whose names share structure. Names
are split on case changes and separators:

- `prefix`: two or more fields start with the same word of three or more letters (not a
  flag word such as `is` or `has`), as `place`, `placeURL`, `placeType`; `parts` gives the
  rest of each name (`""`, `url`, `type`).
- `suffix`: two or more multi-word fields end with the same word, as `reviewScore`,
  `avgScore` (`score`) or `user_id`, `group_id` (`id`).
- `language`: a field and its language variants, as `title`, `title_en`, `title_ru`;
  `parts` gives the language codes. Prefix and suffix groups compare the base names.

```json
"field_groups": [
  {"kind": "prefix", "name": "place", "parent": "", "fields": ["place", "placeType", "placeURL"], "parts": ["", "type", "url"]},
  {"kind": "suffix", "name": "score", "parent": "", "fields": ["avgScore", "reviewScore"]}
]
```

Markdown output lists the groups under each source.

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
