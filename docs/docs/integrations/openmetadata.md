---
title: "OpenMetadata"
description: "Export scan results to OpenMetadata tags and glossary terms"
---
# OpenMetadata

```bash
pip install 'metacrafter[openmetadata]'
```

## Configuration

```yaml
openmetadata:
  url: "http://localhost:8585/api"
  token: "your-jwt-token"
```

## Workflow

```bash
metacrafter scan file users.csv \
  --contexts pii \
  --format full \
  --output-format json \
  -o scan_results.json

metacrafter export openmetadata scan_results.json \
  --table-fqn "postgres.default.public.users" \
  --openmetadata-url "http://localhost:8585/api" \
  --min-confidence 50.0
```

Exported metadata matches the DataHub integration: PII/datatype tags, glossary
terms, and custom properties (`metacrafter_confidence`, `metacrafter_datatype`,
registry URL, rule id, field type).

CLI flags: [export openmetadata](/commands/export-openmetadata).
