---
title: "File scanning"
description: "Classify columns in CSV, JSON, Parquet, Excel, and compressed files"
---
# File scanning

`metacrafter scan file` detects format from the file extension, including
compression suffixes such as `.csv.gz`. See [Formats](/formats/) for the full
list.

## CSV / TSV

```bash
metacrafter scan file somefile.csv --format short

metacrafter scan file somefile.csv \
  --format short \
  --encoding windows-1251 \
  --delimiter ';'

metacrafter scan file data.tsv \
  --delimiter '\t' \
  --format full \
  -o results.json
```

## JSON and JSONL

```bash
metacrafter scan file data.json \
  --format full \
  --output-format json \
  -o results.json

metacrafter scan file somefile.jsonl \
  --format full \
  --output-format json \
  --stdout \
  --pretty
```

## Binary formats

```bash
metacrafter scan file data.parquet --format full --output-format json -o parquet_results.json
metacrafter scan file spreadsheet.xlsx --format full --limit 500 -o excel_results.json
metacrafter scan file data.bson --format full --output-format json -o bson_results.json
```

## Compressed files

```bash
metacrafter scan file data.csv.gz --format full -o results.json

metacrafter scan file data.jsonl.bz2 \
  --compression bz2 \
  --format full \
  -o results.json

metacrafter scan file archive.zip \
  --compression zip \
  --format full \
  -o results.json
```

## XML

```bash
metacrafter scan file data.xml \
  --tagname "record" \
  --format full \
  -o xml_results.json
```

## Statistics only

```bash
metacrafter scan file somefile.csv \
  --stats-only \
  --output-format json \
  -o somefile_stats.json
```

## Related docs

- [scan file](/commands/scan-file)
- [scan bulk](/commands/scan-bulk)
- [Formats](/formats/)
