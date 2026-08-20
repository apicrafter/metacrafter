---
title: "LLM classification"
description: "RAG-based classification with OpenAI, Ollama, and other providers"
---
# LLM classification

Metacrafter can classify fields with Retrieval-Augmented Generation: embed the
field name and sample values, retrieve similar [registry](/integrations/registry)
entries, then ask an LLM. Three modes:

| Mode | Flag | Behavior |
|------|------|----------|
| Rules | `--classification-mode rules` (default) | YAML rules only |
| LLM | `--classification-mode llm` or `--llm-only` | Skip rules |
| Hybrid | `--classification-mode hybrid` or `--use-llm` | Rules first; LLM for unmatched / low-confidence fields |

## Installation

```bash
pip install 'metacrafter[llm]'
```

## CLI examples

```bash
metacrafter scan file data.csv \
  --classification-mode llm \
  --llm-provider openai \
  --llm-model gpt-4o-mini \
  --llm-api-key "sk-..." \
  --format full

metacrafter scan file data.csv \
  --classification-mode hybrid \
  --llm-provider openai \
  --llm-min-confidence 60.0 \
  --format full

metacrafter scan file data.csv \
  --llm-only \
  --llm-provider ollama \
  --llm-base-url "http://localhost:11434" \
  --llm-model llama3 \
  --format full
```

OpenRouter and LM Studio use the same flags (`openrouter` / `lmstudio` and a
`--llm-base-url` for LM Studio, default `http://localhost:1234/v1`).

## Providers

| Provider | Model examples | API key | Base URL |
|----------|----------------|---------|----------|
| OpenAI | gpt-4o-mini, gpt-4 | `OPENAI_API_KEY` | `https://api.openai.com/v1` |
| OpenRouter | openai/gpt-4o-mini | `OPENROUTER_API_KEY` | `https://openrouter.ai/api/v1` |
| Ollama | llama3, mistral | none | `http://localhost:11434` |
| LM Studio | any local model | none | `http://localhost:1234/v1` |
| Perplexity | llama-3.1-sonar-small-128k-online | `PERPLEXITY_API_KEY` | `https://api.perplexity.ai` |

Embeddings currently use OpenAI even when the chat model is local.

## Configuration

```yaml
classification_mode: hybrid
llm_provider: openai
llm_model: gpt-4o-mini
llm_registry_path: ../metacrafter-registry/data/datatypes_latest.jsonl
llm_index_path: ./llm_index
llm_min_confidence: 50.0
```

Do not commit API keys; use environment variables.

## How it works

1. On first use, registry datatypes are embedded into a ChromaDB index
2. Each field name + samples is embedded and similar types are retrieved
3. The LLM classifies with that context
4. Results are converted to the same match structure as rules

Index build is a one-time cost. Each field is typically one LLM call. Rebuild
the index when the registry changes.

## Python

```python
from metacrafter.core import CrafterCmd

cmd = CrafterCmd(
    llm_only=True,
    llm_provider="ollama",
    llm_base_url="http://localhost:11434",
    llm_model="llama3",
)
report = cmd.scan_data(
    items=[{"email": "test@example.com"}],
    classification_mode="llm",
)
```

CLI flags are listed under [Shared CLI options](/commands/shared-options).
