---
title: "Shared CLI options"
description: "Flags shared by scan commands"
---
# Shared CLI options

Most `metacrafter scan …` commands accept the same classification and output
flags.

## Classification

| Flag | Meaning |
|------|---------|
| `--contexts` | Comma-separated context filters (`pii`, `finance`, …) |
| `--langs` | Comma-separated language filters (`en`, `ru`, `fr`) |
| `--country-codes` | Restrict rules to ISO country codes |
| `--confidence`, `-c` | Minimum confidence (0–100, default 5.0) |
| `--stop-on-match` | Stop after the first matching rule per field |
| `--no-dates` | Disable qddate pattern detection |
| `--include-imprecise` | Include imprecise rules that are ignored by default |
| `--include-empty` | Include empty values in statistics and confidence |
| `--fields` | Process only specific fields (comma-separated) |
| `--rulepath` | Extra YAML rule directories (comma-separated) |
| `--limit` | Maximum records sampled per field |

## LLM

| Flag | Meaning |
|------|---------|
| `--classification-mode` | `rules` (default), `llm`, or `hybrid` |
| `--llm-only` | Shortcut for `--classification-mode llm` |
| `--use-llm` | Shortcut for `--classification-mode hybrid` |
| `--llm-provider` | `openai`, `openrouter`, `ollama`, `lmstudio`, `perplexity` |
| `--llm-model` | Model name for the selected provider |
| `--llm-api-key` | API key for cloud providers |
| `--llm-base-url` | Base URL for Ollama, LM Studio, or custom endpoints |
| `--llm-registry` | Path to registry JSONL |
| `--llm-index` | Path to vector index directory |
| `--llm-min-confidence` | Minimum LLM confidence (default 50.0) |

## Output

| Flag | Meaning |
|------|---------|
| `--format` | `short` or `full` result detail |
| `--output-format` | `table`, `json`, `yaml`, or `csv` |
| `--output`, `-o` | Output file path |
| `--stdout` | Write to stdout |
| `--pretty` / `--indent` | JSON formatting |
| `--table-format` | `auto`, `rich`, `plain`, or a tabulate format |
| `--stats-only` | Statistics without classification |

Set `METACRAFTER_PLAIN` to disable rich table rendering globally.

## Remote API

| Flag | Meaning |
|------|---------|
| `--remote` | Base URL of a running `metacrafter server` |
| `--timeout` | Request timeout in seconds (`<=0` disables) |
| `--retries` | Retry attempts |
| `--retry-delay` | Delay between retries |

## Logging

`--debug`, `--verbose`, `--quiet`, `--progress`.

See [LLM classification](/integrations/llm) and [API server](/integrations/api)
for provider and remote details.
