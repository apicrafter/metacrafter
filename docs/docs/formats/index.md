---
title: "Data File Formats"
description: "Formats and compression codecs supported via iterabledata"
slug: /formats
---

# Data File Formats

Metacrafter reads files through [iterabledata](https://github.com/datenoio/iterabledata).
Format detection is automatic from the file extension. For compressed files,
both the codec and the underlying format are detected (for example `data.csv.gz`
is gzip-compressed CSV).

## Text formats

| Format | Extensions | Notes |
|--------|------------|-------|
| CSV | `.csv` | `--delimiter` overrides auto-detection |
| TSV | `.tsv` | Use `--delimiter '\t'` if needed |
| JSON | `.json` | Array of objects |
| JSON Lines | `.jsonl`, `.ndjson` | Newline-delimited JSON |
| XML | `.xml` | `--tagname` selects the record element |

## Binary formats

| Format | Extensions |
|--------|------------|
| BSON | `.bson` |
| Parquet | `.parquet` |
| Avro | `.avro` |
| ORC | `.orc` |
| Excel | `.xls`, `.xlsx` |
| Pickle | `.pickle`, `.pkl` |

## Compression codecs

These suffixes (and `--compression`) are recognized: gzip (`.gz`), bzip2
(`.bz2`), xz (`.xz`), lz4 (`.lz4`), zstandard (`.zst`), Brotli (`.br`), Snappy,
ZIP (`.zip`).

`--encoding` selects character encoding for text formats (default:
auto-detected). `--compression auto` is the default; `none` disables codec
handling.

## Related docs

- [scan file](/commands/scan-file)
- [File scanning](/use-cases/file-scanning)
