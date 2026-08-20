---
title: "Catalog export"
description: "Push scan labels into DataHub, OpenMetadata, or Apache Atlas"
---
# Catalog export

Scan first, then export the JSON report. Do not point export commands at the
original CSV.

```bash
metacrafter scan file users.csv \
  --contexts pii \
  --format full \
  --output-format json \
  -o scan_results.json
```

## DataHub

Requires `pip install 'metacrafter[datahub]'`.

```bash
metacrafter export datahub scan_results.json \
  --dataset-urn "urn:li:dataset:(urn:li:dataPlatform:postgres,users,PROD)" \
  --datahub-url "http://localhost:8080" \
  --token "your-token" \
  --min-confidence 50.0
```

Details: [DataHub integration](/integrations/datahub).

## OpenMetadata

Requires `pip install 'metacrafter[openmetadata]'`.

```bash
metacrafter export openmetadata scan_results.json \
  --table-fqn "postgres.default.public.users" \
  --openmetadata-url "http://localhost:8585/api" \
  --min-confidence 50.0
```

Details: [OpenMetadata integration](/integrations/openmetadata).

## Apache Atlas

```bash
metacrafter export atlas scan_results.json \
  --table-qn "postgres.public.users" \
  --atlas-url "http://localhost:21000" \
  --username admin \
  --password admin
```

Details: [Apache Atlas integration](/integrations/atlas).

## What gets exported

Typically tags or classifications for PII and datatype, plus custom properties
such as confidence, rule id, and registry URL. Use `--min-confidence` so weak
matches stay out of the catalog.
