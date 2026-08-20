---
title: "scan bulk"
description: "Scan every supported file in a directory tree"
---
# scan bulk

```bash
metacrafter scan bulk <path> [options]
```

Walks a directory, opens each supported file, and writes a combined report.

## Examples

```bash
metacrafter scan bulk /path/to/data \
  --limit 200 \
  --output-format json \
  -o bulk_results.json

metacrafter scan bulk ./datasets \
  --contexts pii \
  --format full \
  -o pii_bulk.json
```

File-type options such as `--delimiter` and `--encoding` apply to text files
found in the tree. See [shared scan options](/commands/shared-options).
