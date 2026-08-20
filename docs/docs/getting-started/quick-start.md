---
title: "Quick Start"
description: "Task-oriented first success paths for Metacrafter"
---
# Quick Start

Short task-oriented paths to first success. Not sure where to start? Pick your
goal in the [cookbook](/getting-started/cookbook). Installation details:
[Installation](/getting-started/installation). CLI flag list: [CLI reference](/commands/).

## Scan a CSV in 30 seconds

```bash
pip install metacrafter
printf 'email,name\nalice@example.com,Alice Example\nbob@example.com,Bob Example\n' > people.csv
metacrafter scan file people.csv --format short
```

Write machine-readable JSON:

```bash
metacrafter scan file people.csv \
  --format full \
  --output-format json \
  --stdout \
  --pretty
```

## Detect PII in a file

```bash
metacrafter scan file users.csv \
  --contexts pii \
  --langs en \
  --confidence 20.0 \
  --format full \
  -o pii_scan.json
```

## Scan a SQLite database

```bash
metacrafter scan sql "sqlite:///users.db" \
  --contexts pii \
  --format full \
  --output-format json \
  -o sqlite_results.json
```

## Inspect loaded rules

```bash
metacrafter rules list
metacrafter rules stats
```

## Next steps

- [Usage scenarios by role](/getting-started/cookbook)
- [Format support](/formats/)
- [When to use Metacrafter](/getting-started/when-to-use)
- [LLM classification](/integrations/llm)
- [Python SDK](/integrations/sdk)
