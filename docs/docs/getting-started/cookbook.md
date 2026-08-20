---
title: "Cookbook"
description: "Task-oriented index by role and goal"
---
# Cookbook

Pick a role and a goal, then jump to a verified command path.

## Privacy / compliance

| Goal | Start here |
|------|------------|
| Find PII in a CSV | [PII detection](/use-cases/pii-detection) |
| Scan a database for PII | [Database scanning](/use-cases/database-scanning) |
| Restrict rules to English PII | `metacrafter scan file users.csv --contexts pii --langs en` |
| Push labels into DataHub | [Catalog export](/use-cases/catalog-export) |

## Data engineers

| Goal | Start here |
|------|------------|
| Classify columns in Parquet / Excel | [File scanning](/use-cases/file-scanning) |
| Scan every file in a directory | [scan bulk](/commands/scan-bulk) |
| Use Metacrafter from Python | [Python SDK](/integrations/sdk) |
| Run a shared classifier API | [API server](/integrations/api) |

## Analysts / stewards

| Goal | Start here |
|------|------------|
| Understand what a column *is* | [Semantic labeling](/use-cases/semantic-labeling) |
| Look up a datatype | [Registry](/integrations/registry) |
| See loaded rules | [rules](/commands/rules) |
| Statistics without classification | `metacrafter scan file data.csv --stats-only` |

## Rule authors

| Goal | Start here |
|------|------------|
| Add a YAML rule | [Custom rules](/integrations/rules) |
| Install the extended pack | `pip install 'metacrafter[rules]'` |
| Filter by country | `--country-codes us,ca` |
| Hybrid rules + LLM | [LLM classification](/integrations/llm) |

## First commands

```bash
# Human-readable table
metacrafter scan file data.csv --format short

# PII-focused JSON report
metacrafter scan file users.csv --contexts pii --format full --output-format json -o report.json

# Database
metacrafter scan sql "sqlite:///app.db" --format full -o db.json

# Inspect rules
metacrafter rules list --output-format json
```
