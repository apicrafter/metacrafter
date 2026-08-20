---
title: "export datahub"
description: "Export a scan report to DataHub"
---
# export datahub

```bash
metacrafter export datahub <results.json> \
  --dataset-urn "urn:li:dataset:(urn:li:dataPlatform:postgres,users,PROD)" \
  --datahub-url "http://localhost:8080" \
  --token "your-token"
```

Install the extra first: `pip install 'metacrafter[datahub]'`.

## Options

| Flag | Meaning |
|------|---------|
| `--dataset-urn` | DataHub dataset URN (required) |
| `--datahub-url` | GMS URL, or `DATAHUB_URL` / `.metacrafter` `datahub.url` |
| `--token` | Auth token, or `DATAHUB_TOKEN` |
| `--add-pii-tags` / `--no-pii-tags` | PII tags (default on) |
| `--add-datatype-tags` / `--no-datatype-tags` | Datatype tags (default on) |
| `--link-glossary-terms` / `--no-glossary-terms` | Glossary links (default on) |
| `--add-properties` / `--no-properties` | Custom properties (default on) |
| `--min-confidence` | Drop weaker matches (0–100) |
| `--replace` | Replace existing metadata instead of merging |

Full workflow: [DataHub integration](/integrations/datahub).
