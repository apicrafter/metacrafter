---
title: "Apache Atlas"
description: "Export scan results as Apache Atlas classifications and attributes"
---
# Apache Atlas

```bash
metacrafter export atlas results.json \
  --table-qn "postgres.public.users" \
  --atlas-url "http://localhost:21000" \
  --username admin \
  --password admin
```

Credentials also come from `ATLAS_URL`, `ATLAS_USERNAME`, and `ATLAS_PASSWORD`.

Classifications and custom attributes are written onto column entities
(`rdbms_column` by default; override with `--entity-type`). Use
`--min-confidence` and `--replace` the same way as the other catalog exporters.

CLI flags: [export atlas](/commands/export-atlas). End-to-end flow:
[Catalog export](/use-cases/catalog-export).
