---
title: "scan file"
description: "Scan a data file and classify its fields"
---
# scan file

```bash
metacrafter scan file <filename> [options]
```

Detects format from the extension (including compression) and classifies each
column. See [Formats](/formats/) and [File scanning](/use-cases/file-scanning).

## File-specific options

| Flag | Meaning |
|------|---------|
| `--delimiter` | CSV/TSV delimiter (default: auto-detected) |
| `--tagname` | XML tag that wraps records |
| `--encoding` | Character encoding (default: auto-detected) |
| `--compression` | `auto`, `none`, or a codec (`gz`, `bz2`, `zip`, …) |

Plus the [shared scan options](/commands/shared-options). Default `--limit` is
100 records per field.

## Examples

```bash
metacrafter scan file somefile.csv --format short

metacrafter scan file data.parquet \
  --format full \
  --output-format json \
  -o parquet_results.json

metacrafter scan file users.csv \
  --fields email,phone,name \
  --confidence 50.0 \
  --contexts pii \
  --format full \
  -o filtered_results.json
```
