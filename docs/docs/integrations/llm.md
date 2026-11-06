---
title: "LLM classification"
description: "Registry-constrained LLM classification with OpenAI-compatible providers and Claude"
---
# LLM classification

Metacrafter can ask a language model to label fields that rules and registry patterns
leave unresolved. The model never invents datatypes: it chooses from a shortlist of
[registry](/integrations/registry) records, or answers "none".

| Mode | Flag | Behaviour |
|------|------|----------|
| Rules | `--classification-mode rules` (default) | Rules and registry detectors only |
| LLM | `--classification-mode llm` or `--llm-only` | LLM for every field |
| Hybrid | `--classification-mode hybrid` or `--use-llm` | Rules first; the LLM only for fields without a match at or above `--llm-min-confidence`. Rule matches are not reviewed, so hybrid adds labels but does not remove false ones |

## Verifier (`--llm-verify`)

Rules and registry detectors propose datatypes from values; some proposals fit the pattern
but not the context. `--llm-verify` (or `llm_verify: true` in `.metacrafter`) sends one
request per table after the rules: all column paths, and for each column with value
matches its type, distinct-value count, samples under `--sample-policy` and the candidate
datatypes with their registry descriptions. The answer may only keep candidate ids; value
matches it does not keep are removed (`--explain` lists them as "Rejected by the LLM
verifier"), kept ones get `evidence.llm_verified: true`. Field-name matches are never sent
or removed. The report gets an `llm_verifier` summary (provider, model, columns, removed,
disclosure). Answers are cached; the request counts against `llm_max_requests`; on failure
`--llm-fallback rules` keeps the rule matches.

```bash
metacrafter scan file data.csv --llm-verify --llm-provider openrouter \
  --llm-model openai/gpt-5-mini --sample-policy redacted
```

On `armsis` GeoNode metadata it removed the one remaining false match (`icaoairportcode` on
`links.name`) and kept all 44 correct ones; on the IATI catalogue it removed `uiccode`,
`oid` and `ennationality` false matches.

## How it works

1. **Shortlist.** For each field, the registry ranks candidates from the sample values
   (regexp hit rate, checksum, field-name similarity) and from the field name. This runs
   locally; raw values are used here only.
2. **Disclosure.** The prompt gets the field name and, depending on the policy, nothing
   else, masked value shapes (`Aaaa.Aaaa@aaaa.aaa`) or raw values (see below).
3. **Prompt.** Each candidate is described with its name, description, examples and,
   separately, its counter-examples (registry examples marked `valid: false`).
4. **Constrained answer.** The answer schema allows only shortlist ids or `null`. The
   answer is validated again; aliases resolve to canonical ids, and any other id is
   rejected and recorded in `evidence.rejected`.
5. **Score.** Model confidence maps to `100 × confidence × 0.8`, capped at 80, so strong
   value evidence from rules outranks an unverified model answer. Matches have
   `ruletype: llm` and `evidence` with the shortlist, disclosure mode, provider and model.
6. **Cache.** Answers are cached per field name, values and registry version.

## Installation

```bash
pip install 'metacrafter[llm]'        # adds the Anthropic SDK; OpenAI-compatible APIs need nothing extra
```

## Providers

All OpenAI-compatible servers go through one HTTP client; the presets only set defaults.
`--llm-base-url`, `--llm-model` and `--llm-api-key` override them.

| `--llm-provider` | Default model | API key | Default base URL | Local |
|---|---|---|---|---|
| `openai` | gpt-4o-mini | `OPENAI_API_KEY` | `https://api.openai.com/v1` | no |
| `openrouter` | openai/gpt-4o-mini | `OPENROUTER_API_KEY` | `https://openrouter.ai/api/v1` | no |
| `perplexity` | llama-3.1-sonar-small-128k-online | `PERPLEXITY_API_KEY` | `https://api.perplexity.ai` | no |
| `ollama` | llama3 | none | `http://localhost:11434/v1` | yes |
| `lmstudio` | local-model | none | `http://localhost:1234/v1` | yes |
| `openai-compatible` | gpt-4o-mini | `OPENAI_API_KEY` | set `--llm-base-url` | by host |
| `anthropic` | claude-opus-5-5 | `ANTHROPIC_API_KEY` (or `ant auth login`) | Anthropic API | no |

A provider counts as local when its base URL points to `localhost` or a loopback address.
OpenAI-compatible providers request JSON-schema output and fall back to JSON mode when a
server does not support it. The Anthropic provider uses structured outputs and enables
server-side refusal fallbacks; a refusal is reported as an LLM error, never as a label.

## Privacy

`sample_policy` decides what may leave the machine:

| `sample_policy` | Prompt contains |
|---|---|
| `names-only` (default) | Field name and registry candidates; no values |
| `redacted` | Masked value shapes |
| `raw` | Up to 10 raw values, **except** that values of fields whose shortlist contains a PII datatype are masked when the provider is not local |

Set `llm_send_pii_values: true` in `.metacrafter` to allow raw PII values to a remote
provider. Each match records the mode used in `evidence.disclosure`.

## Configuration

```yaml
# .metacrafter
classification_mode: hybrid
llm_provider: anthropic          # or openai, ollama, ...
llm_model: claude-opus-5-5
llm_min_confidence: 60
llm_shortlist_size: 15
llm_send_pii_values: false
llm_max_requests: 100            # most LLM requests per scan
```

A scan makes at most `llm_max_requests` LLM requests (default 100; `--llm-max-requests`
overrides it). Cached answers and fields without registry candidates do not count. When a
scan needs more, it stops with `LLM request budget of N requests exceeded`, unless
`--llm-fallback rules` is set: then the remaining fields use rules.

## CLI examples

```bash
metacrafter scan file data.csv --classification-mode hybrid --llm-provider anthropic
metacrafter scan file data.csv --llm-only --llm-provider ollama --llm-model llama3
```

## Deprecated: vector index

`llm_registry_path` / `--llm-registry` select the older pipeline that embeds a registry
JSONL file into ChromaDB (`pip install 'metacrafter[llm-chroma]'`). It still works but logs
a deprecation warning; the registry adapter replaces it.
