---
title: "DataHub"
description: "Export scan results to DataHub tags, glossary terms, and properties"
---
# DataHub

```bash
pip install 'metacrafter[datahub]'
```

## Configuration

`.metacrafter`:

```yaml
datahub:
  url: "http://localhost:8080"
  token: "your-authentication-token"
```

Or `DATAHUB_URL` and `DATAHUB_TOKEN`.

## Workflow

```bash
metacrafter scan file users.csv --contexts pii --format full --output-format json -o scan_results.json

metacrafter export datahub scan_results.json \
  --dataset-urn "urn:li:dataset:(urn:li:dataPlatform:postgres,users,PROD)" \
  --datahub-url "http://datahub.example.com:8080" \
  --min-confidence 50.0
```

## What gets exported

- **Tags**: PII and datatype tags (for example `PII`, `Email`, `Phone`)
- **Glossary terms**: links for detected datatypes
- **Custom properties**: `metacrafter_confidence`, `metacrafter_datatype`,
  `metacrafter_datatype_url`, `metacrafter_rule_id`, `metacrafter_field_type`

CLI flags: [export datahub](/commands/export-datahub).

## Python

```python
from metacrafter.core import CrafterCmd
from metacrafter.integrations.datahub import DataHubExporter

cmd = CrafterCmd()
report = cmd.scan_file(filename="users.csv", contexts="pii", output_format="json")

exporter = DataHubExporter(datahub_url="http://localhost:8080", token="your-token")
stats = exporter.export_scan_results(
    dataset_urn="urn:li:dataset:(urn:li:dataPlatform:postgres,users,PROD)",
    scan_report=report,
    min_confidence=50.0,
)
print(stats)
```
