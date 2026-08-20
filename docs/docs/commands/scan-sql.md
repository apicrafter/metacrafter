---
title: "scan sql"
description: "Scan SQL database tables via SQLAlchemy"
---
# scan sql

```bash
metacrafter scan sql <connectstr> [options]
```

`connectstr` is a SQLAlchemy URL. Tables are listed (optionally filtered by
`--schema`), then rows are fetched in batches.

## SQL-specific options

| Flag | Meaning |
|------|---------|
| `--schema` | Limit to one schema |
| `--batch-size` | Rows fetched per batch (default 1000) |

Plus the [shared scan options](/commands/shared-options). Default `--limit` is
1000 records per field.

## Examples

```bash
metacrafter scan sql "sqlite:///path/to/database.db" \
  --format full \
  --output-format json \
  -o sqlite_results.json

metacrafter scan sql "postgresql+psycopg2://user:pass@localhost/dbname" \
  --schema public \
  --contexts pii \
  --progress \
  --format full \
  -o db_results.json
```

More dialects: [Database scanning](/use-cases/database-scanning).
