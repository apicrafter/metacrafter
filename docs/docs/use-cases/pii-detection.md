---
title: "PII detection"
description: "Scan files and databases for personal identifiable information"
---
# PII detection

Metacrafter tags PII using rules in the `pii` context (and language-specific
packs such as `en`, `ru`, `fr`). Combine context filters with a confidence
threshold so the report stays useful.

## File

```bash
metacrafter scan file users.csv \
  --contexts pii \
  --langs en \
  --confidence 20.0 \
  --format full \
  -o pii_scan.json
```

JSON Lines:

```bash
metacrafter scan file users.jsonl \
  --contexts pii \
  --langs en \
  --confidence 20.0 \
  --format full
```

## Database

```bash
metacrafter scan sql "sqlite:///users.db" \
  --contexts pii \
  --langs en \
  --confidence 20.0 \
  --format full \
  -o pii_scan.json
```

Limit columns when you already know the likely PII fields:

```bash
metacrafter scan file users.csv \
  --fields email,phone,name,address \
  --contexts pii \
  --format full
```

## What you get

Each matching field includes:

- rule id and datatype key
- confidence (0–100)
- registry URL, for example `https://registry.apicrafter.io/datatype/email`

Rich table output highlights PII matches when writing to a terminal.

## Next steps

- Export the JSON report to [DataHub](/integrations/datahub) or [OpenMetadata](/integrations/openmetadata)
- Add country-specific PII with `pip install 'metacrafter[rules]'`
- Use [hybrid LLM mode](/integrations/llm) for columns rules miss
