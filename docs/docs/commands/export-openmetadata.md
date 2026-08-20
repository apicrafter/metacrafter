---
title: "export openmetadata"
description: "Export a scan report to OpenMetadata"
---
# export openmetadata

```bash
metacrafter export openmetadata <results.json> \
  --table-fqn "postgres.default.public.users" \
  --openmetadata-url "http://localhost:8585/api" \
  --min-confidence 50.0
```

Install the extra first: `pip install 'metacrafter[openmetadata]'`.

`--table-fqn` is the OpenMetadata table fully-qualified name. URL and token can
come from flags, environment variables, or the `openmetadata` section of
`.metacrafter`.

Full workflow: [OpenMetadata integration](/integrations/openmetadata).
