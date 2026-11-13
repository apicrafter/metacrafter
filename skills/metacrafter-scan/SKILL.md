---
name: metacrafter-scan
description: Scan a dataset, file or database table with Metacrafter to label its columns with semantic datatypes from the Metacrafter Registry and find personal data (PII). Use when asked what columns of a table contain, which fields are PII, to document or classify a dataset, or to prepare catalog labels.
---

# Scan data with Metacrafter

Metacrafter labels each column with datatypes from the Metacrafter Registry
(https://registry.apicrafter.io). Each label has a canonical registry id, a URL,
a confidence (0-100), a PII flag and evidence.

## Choose an access method

1. **MCP tools** (best): if the `metacrafter` MCP server is connected
   (`metacrafter mcp`), use its tools. They run locally.
   - You already have sample values: `classify_columns(columns=[{"name": ..., "values": [...]}])`.
   - A file in an allowed directory: `scan_file(path, limit)`.
   - A configured database: `scan_sql(connection, tables, limit)`. Pass the
     connection **name**; URLs are refused.
2. **CLI**: `metacrafter scan file data.csv --output-format md` prints a Markdown table;
   `--output-format json` gives report version 3.
3. **HTTP**: `POST /v2/classify` with `{"columns": [...]}` on a running
   `metacrafter server run`.

Do not send real personal data to a remote service to classify it. Metacrafter runs
locally and its outputs omit scanned values unless `--include-samples` is set.

## Read the result

For each field:

- **`datatype`** is the canonical registry id. Cite it with its `url`.
  `rule_key` is the rule's own key; it may be an older alias such as `ssn` for `usssn`.
- **`confidence`** is heuristic, not a probability:
  - 90-100 with data evidence is strong;
  - field-name-only matches score 60 by the name alone (`ruletype: field`), up to 90 when
    the values pass a specific check of the datatype and down to 50 when they fail even a
    broad one (`evidence.value_fit`), so check the values before relying on them;
  - scans report matches from confidence 50 by default; pass `confidence: 5` (CLI
    `--confidence 5`) to see weaker candidates, which include chance checksum matches;
  - matches with `evidence.checksum_rate` 1.0 (IBAN, INN, card numbers) are near-certain.
- **`is_pii`** with `pii_source`: `rule` (the rule says so), `registry` (the
  datatype is PII in the registry) or `context` (the rule file is a PII pack).
- **`status: draft`** means the registry record has not been reviewed. Say "draft" and
  do not base compliance decisions on it alone.
- **`ruletype: registry`** means the match came from a registry pattern without a curated
  rule (`registry:<id>` rule ids).
- Several datatypes on one field: the first has the highest confidence. Formats can
  overlap (for example 64 hex digits can be several hash types); use the field name and
  context to decide, and say so.
- No match: report the column as unknown. Do not force a datatype.

Use `explain_match(datatype, values)` to see why a datatype was or was not detected.
For details of a datatype, crosswalks to other vocabularies or value identification without
rules, use the registry's own `registry-lookup` skill and MCP server
(https://github.com/apicrafter/metacrafter-registry/tree/main/skills/registry-lookup).

## Report

| Column | Datatype | PII | Confidence | Evidence |
|---|---|---|---|---|
| `contact_email` | [Email address](https://registry.apicrafter.io/datatype/email) (`email`) | yes | 100 | 100% of values match |

Add a short list of PII columns. For catalogs, `metacrafter export labels report.json
--system presidio` maps datatypes to Presidio, Google DLP, DPV or Purview labels.
