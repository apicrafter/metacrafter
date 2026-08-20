---
title: "Troubleshooting"
description: "Common errors, exit codes, and how to debug scans"
---
# Troubleshooting

## Command help

```bash
metacrafter --help
metacrafter scan file --help
metacrafter rules list --help
```

Add `--debug` or `--verbose` for more logging. `--quiet` reduces non-essential
output.

## Rules not matching

- Confirm the rule loaded: `metacrafter rules list | grep my_key`
- Check `--contexts`, `--langs`, and `--country-codes` are not filtering it out
- Lower `--confidence` (default is 5.0)
- Imprecise rules are skipped unless you pass `--include-imprecise`
- Duplicate rule `key`s are skipped with a warning; later copies do not load

## Extended rules missing

Install the extra and keep auto-discovery on:

```bash
pip install 'metacrafter[rules]'
```

Set `auto_rules: false` only when you want a custom-only `rulepath`.

## File format / encoding issues

- Encoding: `--encoding utf-8` or `--encoding windows-1251`
- CSV delimiter: `--delimiter ';'`
- XML records: `--tagname record`
- Compression: `--compression auto` (default), or `gz`, `bz2`, `zip`, `none`

See [Formats](/formats/) for the supported matrix.

## Database connection errors

Use a SQLAlchemy URL the driver understands:

```text
postgresql+psycopg2://user:pass@host:5432/dbname
mysql+pymysql://user:pass@host:3306/dbname
sqlite:///path/to/database.db
```

Install the corresponding DB-API driver in the same environment. MongoDB needs
`pymongo` (already a core dependency).

## LLM / RAG failures

- Cloud providers need an API key (`OPENAI_API_KEY`, `OPENROUTER_API_KEY`, …)
- Ollama and LM Studio must be running at `--llm-base-url`
- The first LLM run builds a ChromaDB index; that can take several minutes
- Embeddings currently use OpenAI even when the chat model is local

See [LLM classification](/integrations/llm).

## Remote server timeouts

```bash
metacrafter scan file data.csv \
  --remote http://127.0.0.1:10399 \
  --timeout 60 \
  --retries 2 \
  --retry-delay 1.0
```

Start the server with `metacrafter server run` first.

## Catalog export

- DataHub: `DATAHUB_URL` / `--datahub-url` and `DATAHUB_TOKEN`
- OpenMetadata: `--openmetadata-url` and a JWT token
- Atlas: `--atlas-url`, `--username`, `--password`

The export commands read a **JSON scan report**, not the original data file.

## Related docs

- [Best practices](/getting-started/best-practices)
- [Architecture](/development/architecture)
- [Contributing](/development/contributing)
