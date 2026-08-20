---
title: "export atlas"
description: "Export a scan report to Apache Atlas"
---
# export atlas

```bash
metacrafter export atlas <results.json> \
  --table-qn "postgres.public.users" \
  --atlas-url "http://localhost:21000" \
  --username admin \
  --password admin
```

## Options

| Flag | Meaning |
|------|---------|
| `--table-qn` | Atlas table qualified name (required) |
| `--atlas-url` | Server URL, or `ATLAS_URL` |
| `--username` / `--password` | Credentials, or `ATLAS_USERNAME` / `ATLAS_PASSWORD` |
| `--add-pii-classifications` | PII classifications (default on) |
| `--add-datatype-classifications` | Datatype classifications (default on) |
| `--add-attributes` | Custom attributes (default on) |
| `--min-confidence` | Drop weaker matches |
| `--entity-type` | Atlas entity type for columns (default `rdbms_column`) |
| `--replace` | Replace existing metadata instead of merging |

Full workflow: [Apache Atlas integration](/integrations/atlas).
