---
title: "scan mongodb"
description: "Scan MongoDB collections"
---
# scan mongodb

```bash
metacrafter scan mongodb <host> [options]
```

`host` can be a hostname or a full `mongodb://` URI.

## MongoDB-specific options

| Flag | Meaning |
|------|---------|
| `--port` | Port (default 27017; ignored for URI hosts) |
| `--dbname` | Database name |
| `--username` / `--password` | Authentication |

Plus the [shared scan options](/commands/shared-options). Default `--limit` is
1000 documents per field.

## Examples

```bash
metacrafter scan mongodb localhost \
  --port 27017 \
  --dbname mydatabase \
  --output-format json \
  -o mongodb_results.json

metacrafter scan mongodb "mongodb://user:pass@host1:27017,host2:27017/dbname?replicaSet=rs0" \
  --format full \
  -o mongodb_results.json
```
