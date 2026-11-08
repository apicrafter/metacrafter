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
| `--langs` | Comma-separated language filters (`en`, `ru`, `fr`); language-independent rules (`lang: common`) always apply |
| `--country-codes` | ISO country codes; rules of other countries are not loaded, universal rules (`country_code: xx` or none) always are |
| `--confidence`, `-c` | Minimum confidence (0–100, default 50.0; `5` for a broad scan) |
| `--stop-on-match` | Stop after the first matching rule per field |
| `--no-dates` | Disable qddate pattern detection |
| `--include-imprecise` | Include imprecise rules that are ignored by default |
| `--include-empty` | Include empty values in statistics and confidence |
| `--strip` | Remove leading and trailing whitespace from values before analysis (off by default) |
| `--fields` | Process only specific fields (comma-separated). Nested fields are paths (`info.okoguCode`); reports write a dot inside a key or header as `\.` (`info\.okoguCode` for a CSV header `info.okoguCode`), and either form selects the field |
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
| `--llm-max-requests` | Most LLM requests per scan (default `llm_max_requests` in `.metacrafter`, else 100) |

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
