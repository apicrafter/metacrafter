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
| CSV | `.csv` | Delimiter (comma, semicolon, tab or pipe) detected from the first records; `--delimiter` overrides it. An Excel `sep=;` first line and title or note rows above the header are skipped |
| TSV | `.tsv` | Tab-delimited |
| Delimited text | `.txt` | Read as CSV when the lines have a consistent delimiter (GTFS `stops.txt`), else as plain text |
| JSON | `.json` | Array of objects, or an object that holds them: the longest list of objects (OData `value`, JSON-LD `@graph`, `result.records`) gives the records, or column names next to rows as lists (`columns` + `data`); `--tagname` names the key |
| JSON Lines | `.jsonl`, `.ndjson` | Newline-delimited JSON |
| XML | `.xml` | `--tagname` selects the record element. Records that repeat one element (`<body><column>…</column><column>…</column></body>`) get the names listed by a sibling header element (`<header><column>選挙名</column>…</header>`) |
| PC-Axis | `.px` | Read in the encoding of `CODEPAGE` / `CHARSET`; the metadata keywords are not reported as fields |

## Geodata formats

| Format | Extensions | Notes |
|--------|------------|-------|
| GeoJSON, TopoJSON | `.geojson`, `.topojson` | Feature properties are scanned as `properties.<name>` |
| KML | `.kml`, `.kmz` | Placemarks; `Data`, `SchemaData/SimpleData` and the attribute table of an ArcGIS description become `properties.<name>` |
| GML | `.gml` | GML and WFS feature collections (`gml:featureMember`, `wfs:member`) |
| Shapefile | `.shp.zip` | Zipped Shapefile; needs `pyshp` (in the `formats` extra) |

## Binary formats

| Format | Extensions |
|--------|------------|
| BSON | `.bson` |
| Parquet | `.parquet` |
| Avro | `.avro` |
| ORC | `.orc` |
| Excel | `.xls`, `.xlsx` (the first sheet that holds a table; title rows and multi-row headers are handled) |
| OpenDocument spreadsheet | `.ods` (needs `odfpy`, in the `formats` extra) |
| Pickle | `.pickle`, `.pkl` |
| SPSS | `.sav`, `.zsav` (needs `pyreadstat` 1.3.1 or later, in the `formats` extra; no pandas). Variable labels and value labels are reported with each field (`label`, `value_labels`) |

## Compression codecs

These suffixes (and `--compression`) are recognized: gzip (`.gz`), bzip2
(`.bz2`), xz (`.xz`), lz4 (`.lz4`), zstandard (`.zst`), Brotli (`.br`), Snappy,
ZIP (`.zip`).

`--encoding` selects character encoding for text formats (default: detected from the
first megabyte of the decompressed file; single-byte Latin text gets cp1252, cp1250,
cp1257 or cp1254 by its letters). `--compression auto` is the default; `none` disables codec
handling.

## Related docs

- [scan file](/commands/scan-file)
- [File scanning](/use-cases/file-scanning)
